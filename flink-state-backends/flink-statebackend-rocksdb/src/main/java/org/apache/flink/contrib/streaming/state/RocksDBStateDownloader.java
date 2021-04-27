/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteBatchKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Help class for downloading RocksDB state files.
 */
public class RocksDBStateDownloader extends RocksDBStateDataTransfer {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateDownloader.class);

	public RocksDBStateDownloader(int restoringThreadNum) {
		super(restoringThreadNum);
	}

	/**
	 * Transfer all state data to the target directory using specified number of threads.
	 *
	 * @param restoreStateHandle Handles used to retrieve the state data.
	 * @param dest The target directory which the state data will be stored.
	 *
	 * @throws Exception Thrown if can not transfer all the state data.
	 */
	public void transferAllStateDataToDirectory(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path dest,
		CloseableRegistry closeableRegistry) throws Exception {

		final Map<StateHandleID, StreamStateHandle> sstFiles =
			restoreStateHandle.getSharedState();
		final Map<StateHandleID, StreamStateHandle> miscFiles =
			restoreStateHandle.getPrivateState();

		if (restoreStateHandle instanceof IncrementalRemoteBatchKeyedStateHandle) {
			final Map<StateHandleID, List<StateHandleID>> usedSstFiles =
				((IncrementalRemoteBatchKeyedStateHandle) restoreStateHandle).getUsedFiles();
			Preconditions.checkState(usedSstFiles.size() == sstFiles.size());
			downloadDataForAllStateHandles(sstFiles, dest, closeableRegistry, true, usedSstFiles);
			downloadDataForAllStateHandles(miscFiles, dest, closeableRegistry, true, null);
		} else {
			downloadDataForAllStateHandles(sstFiles, dest, closeableRegistry, false, null);
			downloadDataForAllStateHandles(miscFiles, dest, closeableRegistry, false, null);
		}
	}

	/**
	 * Copies all the files from the given stream state handles to the given path, renaming the files w.r.t. their
	 * {@link StateHandleID}.
	 *
	 * @param stateHandleMap Maps from state files to state file handles, the state file can be batch or single file.
	 * @param restoreInstancePath Location for downloaded state files.
	 * @param closeableRegistry File operation handle registration.
	 * @param useStateFiles Required files in batches. If null, all files is required.
	 * @throws Exception
	 */
	private void downloadDataForAllStateHandles(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		CloseableRegistry closeableRegistry,
		boolean isBatched,
		@Nullable Map<StateHandleID, List<StateHandleID>> useStateFiles) throws Exception {

		try {
			List<Runnable> runnables = createDownloadRunnables(stateHandleMap, restoreInstancePath, closeableRegistry, isBatched, useStateFiles);
			List<CompletableFuture<Void>> futures = new ArrayList<>(runnables.size());
			for (Runnable runnable : runnables) {
				futures.add(CompletableFuture.runAsync(runnable, executorService));
			}
			FutureUtils.waitForAll(futures).get();
		} catch (ExecutionException e) {
			Throwable throwable = ExceptionUtils.stripExecutionException(e);
			throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			} else {
				throw new FlinkRuntimeException("Failed to download data for state handles.", e);
			}
		}
	}

	private List<Runnable> createDownloadRunnables(
		Map<StateHandleID, StreamStateHandle> stateHandleMap,
		Path restoreInstancePath,
		CloseableRegistry closeableRegistry,
		boolean isBatched,
		@Nullable Map<StateHandleID, List<StateHandleID>> usedStateFiles) {
		List<Runnable> runnables = new ArrayList<>(stateHandleMap.size());
		for (Map.Entry<StateHandleID, StreamStateHandle> entry : stateHandleMap.entrySet()) {
			StateHandleID stateHandleID = entry.getKey();
			StreamStateHandle remoteFileHandle = entry.getValue();

			if (isBatched) {
				List<Path> paths = new ArrayList<>();
				List<Long> offsets = new ArrayList<>();
				List<Long> fileSizes = new ArrayList<>();
				extractUsedSstFiles(paths, offsets, fileSizes, remoteFileHandle, stateHandleID, usedStateFiles, restoreInstancePath);

				runnables.add(ThrowingRunnable.unchecked(
					() -> downloadDataForStateHandle(paths, offsets, fileSizes, remoteFileHandle, closeableRegistry)));
			} else {
				Path path = restoreInstancePath.resolve(stateHandleID.toString());
				runnables.add(ThrowingRunnable.unchecked(
					() -> downloadDataForStateHandle(path, remoteFileHandle, closeableRegistry)));
			}

		}
		return runnables;
	}

	/**
	 * Copies the file from a single state handle to the given path.
	 */
	private void downloadDataForStateHandle(
		Path restoreFilePath,
		StreamStateHandle remoteFileHandle,
		CloseableRegistry closeableRegistry) throws IOException {

		FSDataInputStream inputStream = null;

		try {
			inputStream = remoteFileHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			Files.createDirectories(restoreFilePath.getParent());
			downloadForSingleFile(restoreFilePath, inputStream, closeableRegistry, Long.MAX_VALUE);
		} finally {
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}


	/**
	 * Copies files from a single state handle to the given path.
	 */
	private void downloadDataForStateHandle(
		List<Path> restoreFilePaths,
		List<Long> offsets,
		List<Long> fileSizes,
		StreamStateHandle remoteFileHandle,
		CloseableRegistry closeableRegistry) throws IOException {

		FSDataInputStream inputStream = null;

		// create restore directory
		Files.createDirectories(restoreFilePaths.get(0).getParent());

		try {
			inputStream = remoteFileHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			for (int i = 0; i < restoreFilePaths.size(); i++) {
				inputStream.seek(offsets.get(i));
				downloadForSingleFile(restoreFilePaths.get(i), inputStream, closeableRegistry, fileSizes.get(i));
			}
		} finally {
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}

	// ----------------------------------------------------------------
	// Utilities
	// ----------------------------------------------------------------
	private static void extractUsedSstFiles(List<Path> paths, List<Long> offsets, List<Long> fileSizes, StreamStateHandle remoteFileHandle, StateHandleID batchFileId,
											@Nullable Map<StateHandleID, List<StateHandleID>> usedStateFiles, Path restoreInstancePath) {
		BatchStateHandle batchStateHandle = (BatchStateHandle) remoteFileHandle;

		StateHandleID[] stateHandleIds = batchStateHandle.getStateHandleIds();
		Tuple2<Long, Long>[] offsetsAndSizes = batchStateHandle.getOffsetsAndSizes();

		if (usedStateFiles == null) {
			// download all misc files
			for (int i = 0; i < stateHandleIds.length; i++) {
				paths.add(restoreInstancePath.resolve(stateHandleIds[i].getKeyString()));
				offsets.add(offsetsAndSizes[i].f0);
				fileSizes.add(offsetsAndSizes[i].f1);
			}
		} else {
			// download used sst files
			for (StateHandleID sstFileName : usedStateFiles.get(batchFileId)) {
				Tuple2<Long, Long> offsetAndSize = batchStateHandle.getOffsetAndSize(sstFileName);
				Preconditions.checkState(offsetAndSize.f0 != -1);

				paths.add(restoreInstancePath.resolve(sstFileName.getKeyString()));
				offsets.add(offsetAndSize.f0);
				fileSizes.add(offsetAndSize.f1);
			}
		}
	}

	/**
	 * Copies a single file from inputStream with limited length.
	 */
	private void downloadForSingleFile(
		Path filePath,
		FSDataInputStream inputStream,
		CloseableRegistry closeableRegistry,
		long toWriteLocalTotal) throws IOException {

		OutputStream outputStream = null;
		try {
			outputStream = Files.newOutputStream(filePath);
			closeableRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[8 * 1024];
			long hasWrite = 0;
			while (hasWrite <= toWriteLocalTotal) {
				int numBytes = inputStream.read(buffer);
				if (numBytes == -1) {
					break;
				}
				hasWrite += numBytes;
				outputStream.write(buffer, 0,
					hasWrite <= toWriteLocalTotal ? numBytes : (int) (toWriteLocalTotal - (hasWrite - numBytes)));
			}
		} finally {
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				outputStream.close();
			}
		}
	}
}
