/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.BatchStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Help class for uploading RocksDB state files.
 */
public class RocksDBStateUploader extends RocksDBStateDataTransfer {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateUploader.class);

	private static final int READ_BUFFER_SIZE = 16 * 1024;

	private int maxRetryTimes;

	@Nullable
	private final RocksDBSstBatchStrategy batchStrategy;

	public RocksDBStateUploader(int numberOfSnapshottingThreads, int maxRetryTimes) {
		this(numberOfSnapshottingThreads, maxRetryTimes, null);
	}

	public RocksDBStateUploader(int threadNum, int maxRetryTimes, @Nullable RocksDBSstBatchStrategy batchStrategy) {
		super(threadNum);
		this.maxRetryTimes = maxRetryTimes;
		this.batchStrategy = batchStrategy;
	}

	/**
	 * Upload all the files to checkpoint fileSystem using specified number of threads.
	 *
	 * @param files The files will be uploaded to checkpoint filesystem.
	 * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
	 *
	 * @throws Exception Thrown if can not upload all the files.
	 */
	public Map<StateHandleID, StreamStateHandle> uploadFilesToCheckpointFs(
		@Nonnull Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws Exception {

		Map<StateHandleID, StreamStateHandle> handles = new HashMap<>();

		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures =
			createUploadFutures(files, checkpointStreamFactory, closeableRegistry);

		try {
			FutureUtils.waitForAll(futures.values()).get();

			for (Map.Entry<StateHandleID, CompletableFuture<StreamStateHandle>> entry : futures.entrySet()) {
				handles.put(entry.getKey(), entry.getValue().get());
			}
		} catch (ExecutionException e) {
			Throwable throwable = ExceptionUtils.stripExecutionException(e);
			throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			} else {
				throw new FlinkRuntimeException("Failed to upload data for state handles.", e);
			}
		}

		return handles;
	}

	private Map<StateHandleID, CompletableFuture<StreamStateHandle>> createUploadFutures(
		Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws Exception {
		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures = new HashMap<>(files.size());

		if (batchStrategy == null) {
			for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
				final Supplier<StreamStateHandle> supplier =
					tryWithMultipleTimes(() -> uploadLocalFileToCheckpointFs(entry.getValue(), checkpointStreamFactory, closeableRegistry));
				futures.put(entry.getKey(), CompletableFuture.supplyAsync(supplier, executorService));
			}
		} else {
			// batchId -> collection of state files
			Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(files);
			for (Map.Entry<StateHandleID, List<RocksDBFileMeta>> entry : batches.entrySet()) {
				final Supplier<StreamStateHandle> supplier =
					tryWithMultipleTimes(() -> uploadLocalFileToCheckpointFs(entry.getKey(), entry.getValue(), checkpointStreamFactory, closeableRegistry));

				// state file ID -> stream state handle (shared by all state files in the batch)
				CompletableFuture<StreamStateHandle> sharedStateHandle = CompletableFuture.supplyAsync(supplier, executorService);
				for (RocksDBFileMeta stateFile : entry.getValue()) {
					futures.put(stateFile.getShId(), sharedStateHandle);
				}
			}
		}

		return futures;
	}

	/**
	 * Upload a single state file, state file batching disabled.
	 */
	private StreamStateHandle uploadLocalFileToCheckpointFs(
		Path filePath,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws IOException {

		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		try {
			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
			closeableRegistry.registerCloseable(outputStream);

			uploadSingleFile(filePath, closeableRegistry, outputStream);

			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}
			return result;

		} finally {

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}

	/**
	 * Upload a bunch of state files, state file batching enabled.
	 */
	private StreamStateHandle uploadLocalFileToCheckpointFs(
		StateHandleID batchFileID,
		List<RocksDBFileMeta> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws IOException {

		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		long currentOffset = 0;
		int numFiles = files.size();
		StateHandleID[] fileNames = new StateHandleID[numFiles];
		Tuple2<Long, Long>[] offsetsAndSizes = new Tuple2[numFiles];

		try {
			// (1) open checkpoint fs output stream
			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
			closeableRegistry.registerCloseable(outputStream);

			// (2) open local input stream of each file, and write all files to the same output stream
			for (int i = 0; i < numFiles; i++) {
				long fileSize = uploadSingleFile(files.get(i).getFilePath(), closeableRegistry, outputStream);
				fileNames[i] = files.get(i).getShId();
				offsetsAndSizes[i] = new Tuple2<>(currentOffset, fileSize);
				currentOffset += fileSize;
			}

			// (3) finish writing all files in the batch OR single file,
			// ready to close and get state handle.
			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}

			// (4) wrap the state handle to BatchStateHandle
			return new BatchStateHandle(result, fileNames, offsetsAndSizes, batchFileID);
		} finally {
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}

	// ---------------------------------------------------
	// Utilities
	// ---------------------------------------------------

	private Supplier<StreamStateHandle> tryWithMultipleTimes(SupplierWithException<StreamStateHandle, IOException> internalCallable) {
		return CheckedSupplier.unchecked(() -> {
			int tryCount = 0;
			while (true) {
				try {
					return internalCallable.get();
				} catch (Throwable t) {
					if (++tryCount >= maxRetryTimes) {
						throw t;
					}
					LOG.warn("Fail to upload file to HDFS, retrying...", t);
				}
			}
		});
	}

	/**
	 * Write one file to outputStream.
	 */
	private long uploadSingleFile(
		Path filePath,
		CloseableRegistry closeableRegistry,
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream) throws IOException {

		InputStream inputStream = null;
		long fileSize = 0L;

		try {
			final byte[] buffer = new byte[READ_BUFFER_SIZE];

			inputStream = Files.newInputStream(filePath);
			closeableRegistry.registerCloseable(inputStream);

			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}
				fileSize += numBytes;

				outputStream.write(buffer, 0, numBytes);
			}
			return fileSize;
		} finally {
			if (closeableRegistry.unregisterCloseable(inputStream)) {
				IOUtils.closeQuietly(inputStream);
			}
		}
	}
}

