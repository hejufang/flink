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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Help class for uploading RocksDB state files.
 */
public class RocksDBStateUploader extends RocksDBStateDataTransfer {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateUploader.class);

	private static final int READ_BUFFER_SIZE = 16 * 1024;

	private int maxRetryTimes;

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

		// single file: state file name -> (state file name, localPath)
		// batch: batch file name -> [(state file name, localPath),...]
		Map<StateHandleID, List<Map.Entry<StateHandleID, Path>>> filesOrBatches = new HashMap<>(files.size());

		if (batchStrategy == null) {
			for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
				filesOrBatches.put(entry.getKey(), Collections.singletonList(entry));
			}
		} else {
			// batchId -> collection of state files
			Map<StateHandleID, List<RocksDBFileMeta>> batches = batchStrategy.batch(files);
			for (Map.Entry<StateHandleID, List<RocksDBFileMeta>> entry : batches.entrySet()) {
				List<Map.Entry<StateHandleID, Path>> filePaths =
					entry.getValue().stream().map(RocksDBFileMeta::toEntry).collect(Collectors.toList());
				filesOrBatches.put(entry.getKey(), filePaths);
			}
		}

		for (Map.Entry<StateHandleID, List<Map.Entry<StateHandleID, Path>>> entry : filesOrBatches.entrySet()) {
			final Supplier<StreamStateHandle> supplier =
				CheckedSupplier.unchecked(() -> {
					int tryCount = 0;
					while (true) {
						try {
							return uploadLocalFileToCheckpointFs(entry.getValue(), checkpointStreamFactory, closeableRegistry, entry.getKey());
						} catch (Throwable t) {
							if (++tryCount >= maxRetryTimes) {
								throw t;
							}
							LOG.warn("Fail to upload file to HDFS, retrying...", t);
						}
					}
				});

			// map state file -> stream state handle
			CompletableFuture<StreamStateHandle> stateHandleForFileOrBatch = CompletableFuture.supplyAsync(supplier, executorService);
			for (Map.Entry<StateHandleID, Path> stateFile : entry.getValue()) {
				futures.put(stateFile.getKey(), stateHandleForFileOrBatch);
			}
		}

		return futures;
	}

	/**
	 * Upload a single file or a bunch of files to checkpoint fs. A bunch of files will return
	 * {@link BatchStateHandle}, which represents a batch file.
	 *
	 * <p>Note: filePaths must be a list, for we upload files in a batch in the order returned
	 * by batchStrategy.
	 *
	 * @param filePaths Maps from state file name to local path.
	 * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
	 * @param closeableRegistry Close callback for input/output stream.
	 * @param batchFileID Batch file name, ignored if uploading a single file.
	 * @return {@link StreamStateHandle} represents a single file or a batch file.
	 * @throws IOException Thrown if can not upload all the files.
	 */
	private StreamStateHandle uploadLocalFileToCheckpointFs(
		List<Map.Entry<StateHandleID, Path>> filePaths,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry,
		StateHandleID batchFileID) throws IOException {

		InputStream inputStream = null;
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		Map<StateHandleID, Long> stateFileNameToOffset = new HashMap<>();
		final byte[] buffer = new byte[READ_BUFFER_SIZE];
		long currentOffset = 0;

		try {
			// (1) open checkpoint fs output stream
			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
			closeableRegistry.registerCloseable(outputStream);

			// (2) open local input stream of each file, and write all files to the same output stream
			for (Map.Entry<StateHandleID, Path> entry : filePaths) {
				stateFileNameToOffset.put(entry.getKey(), currentOffset);

				Path filePath = entry.getValue();
				try {
					inputStream = Files.newInputStream(filePath);
					closeableRegistry.registerCloseable(inputStream);

					while (true) {
						int numBytes = inputStream.read(buffer);

						if (numBytes == -1) {
							break;
						}

						outputStream.write(buffer, 0, numBytes);
						// write one buffer, forward current offset
						currentOffset += numBytes;
					}
				} finally {
					if (closeableRegistry.unregisterCloseable(inputStream)) {
						IOUtils.closeQuietly(inputStream);
					}
				}
			}

			// (3) finish writing all files in the batch OR single file,
			// ready to close and get state handle.
			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}

			// (4) wrap the state handle to BatchStateHandle if batch strategy is set
			return batchStrategy == null ?
				result : new BatchStateHandle(result, stateFileNameToOffset, batchFileID);
		} finally {
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}
}

