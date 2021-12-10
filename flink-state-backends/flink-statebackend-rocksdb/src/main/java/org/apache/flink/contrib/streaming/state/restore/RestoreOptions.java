/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.rocksdb.CompressionType;
import org.rocksdb.Env;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;

import java.io.Serializable;

/**
 * Restore options when using sst file writer.
 */
public class RestoreOptions implements Serializable {
	public static final long MIN_STATE_SIZE_OF_KEY_GROUP_RANGE = 256 * 1024 * 1024; // 256MB

	/** Whether to use Sst file writer to restore. */
	private final boolean useSstFileWriter;

	/** The number of asynchronous threads used during recovery. */
	private final int numberOfAsyncExecutor;

	/** Maximum disk usage during recovery at the same time. */
	private final long maxDiskSizeInProgress;

	/** The maximum size of a single sst file. */
	private final long maxSstFileSize;

	/** Compression type for sst file. */
	private final CompressionType compressionType;

	private RestoreOptions(boolean useSstFileWriter, int numberOfAsyncExecutor, long maxDiskSizeInProgress, long maxSstFileSize, CompressionType compressionType) {
		this.useSstFileWriter = useSstFileWriter;
		this.numberOfAsyncExecutor = numberOfAsyncExecutor;
		this.maxDiskSizeInProgress = maxDiskSizeInProgress;
		this.maxSstFileSize = maxSstFileSize;
		this.compressionType = compressionType;
	}

	public boolean isUseSstFileWriter() {
		return useSstFileWriter;
	}

	public int getNumberOfAsyncExecutor() {
		return numberOfAsyncExecutor;
	}

	public long getMaxDiskSizeInProgress() {
		return maxDiskSizeInProgress;
	}

	public long getMaxSstFileSize() {
		return maxSstFileSize;
	}

	public EnvOptions getEnvOptions() {
		return new EnvOptions();
	}

	public Options getOptions() {
		return new Options()
			.setCreateIfMissing(true)
			.setEnv(Env.getDefault())
			.setCompressionType(compressionType);
	}

	public IngestExternalFileOptions getIngestExternalFileOptions() {
		return new IngestExternalFileOptions(
			/* moveFiles */ true,
			/* snapshotConsistency */ true,
			/* allowGlobalSeqNo */ true,
			/* allowBlockingFlush */ false);
	}

	/**
	 * Builder of {@link RestoreOptions}.
	 */
	public static class Builder {
		private static final int MIN_ASYNC_EXECUTOR = 4;

		private boolean useSstFileWriter;
		private int numberOfAsyncExecutor;
		private long maxDiskSizeInProgress;
		private long maxSstFileSize;
		private CompressionType compressionType;

		public Builder() {
			this.useSstFileWriter = false;
			this.numberOfAsyncExecutor = MIN_ASYNC_EXECUTOR;
			this.maxDiskSizeInProgress = 20 * 1024 * 1024 * 1024L; // 20GB
			this.maxSstFileSize = 64 * 1024 * 1024L; // 64MB
			this.compressionType = CompressionType.SNAPPY_COMPRESSION;
		}

		public Builder setUseSstFileWriter(boolean useSstFileWriter) {
			this.useSstFileWriter = useSstFileWriter;
			return this;
		}

		public Builder setNumberOfAsyncExecutor(int numberOfAsyncExecutor) {
			this.numberOfAsyncExecutor = Math.max(numberOfAsyncExecutor, MIN_ASYNC_EXECUTOR);
			return this;
		}

		public Builder setMaxDiskSizeInProgress(long maxDiskSizeInProgress) {
			this.maxDiskSizeInProgress = maxDiskSizeInProgress;
			return this;
		}

		public Builder setMaxSstFileSize(long maxSstFileSize) {
			this.maxSstFileSize = maxSstFileSize;
			return this;
		}

		public Builder setCompressionType(CompressionType compressionType) {
			this.compressionType = compressionType;
			return this;
		}

		public RestoreOptions build() {
			if (compressionType != CompressionType.NO_COMPRESSION) {
				maxSstFileSize *= 2;
			}

			return new RestoreOptions(
				useSstFileWriter,
				numberOfAsyncExecutor,
				maxDiskSizeInProgress,
				maxSstFileSize,
				compressionType);
		}
	}
}
