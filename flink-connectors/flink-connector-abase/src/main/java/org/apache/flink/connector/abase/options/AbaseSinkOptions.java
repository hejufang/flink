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

package org.apache.flink.connector.abase.options;

import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * sink options for abase: sink.
 */
public class AbaseSinkOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	private final int flushMaxRetries;
	private final AbaseSinkMode mode;
	private final int bufferMaxRows;
	private final long bufferFlushInterval;
	private final int ttlSeconds;
	private final int parallelism;
	/**
	 * Flag indicating whether to ignore failures (and log them), or to fail on failures.
	 */
	private final boolean logFailuresOnly;

	// sorted indices of value columns except "event-ts" and "tag" columns
	private final int[] valueColIndices;

	// sorted indices of columns that may need to be serialized
	private final int[] serColIndices;

	private final boolean ignoreDelete;

	private final boolean ignoreNull;

	public int[] getValueColIndices() {
		return valueColIndices;
	}

	public int[] getSerColIndices() {
		return serColIndices;
	}

	public int getFlushMaxRetries() {
		return flushMaxRetries;
	}

	public AbaseSinkMode getMode() {
		return mode;
	}

	public int getBufferMaxRows() {
		return bufferMaxRows;
	}

	public int getTtlSeconds() {
		return ttlSeconds;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	public int getParallelism() {
		return parallelism;
	}

	public boolean isIgnoreDelete() {
		return ignoreDelete;
	}

	public boolean isIgnoreNull() {
		return ignoreNull;
	}

	public long getBufferFlushInterval() {
		return bufferFlushInterval;
	}

	private AbaseSinkOptions(
			int[] valueColIndices,
			int[] serColIndices,
			int flushMaxRetries,
			AbaseSinkMode mode,
			int bufferMaxRows,
			long bufferFlushInterval,
			int ttlSeconds,
			boolean logFailuresOnly,
			boolean ignoreDelete,
			boolean ignoreNull,
			int parallelism) {
		this.valueColIndices = valueColIndices;
		this.serColIndices = serColIndices;
		this.flushMaxRetries = flushMaxRetries;
		this.mode = mode;
		this.bufferMaxRows = bufferMaxRows;
		this.bufferFlushInterval = bufferFlushInterval;
		this.ttlSeconds = ttlSeconds;
		this.logFailuresOnly = logFailuresOnly;
		this.ignoreDelete = ignoreDelete;
		this.ignoreNull = ignoreNull;
		this.parallelism = parallelism;
	}

	public static AbaseInsertOptionsBuilder builder() {
		return new AbaseInsertOptionsBuilder();
	}

	/**
	 * Abase insert options builder.
	 */
	public static class AbaseInsertOptionsBuilder {
		private int[] valueColIndices = null;
		private int[] serColIndices = null;
		private int flushMaxRetries = 5;
		private AbaseSinkMode mode = AbaseSinkMode.INSERT;
		private int bufferMaxRows = 50;
		private long bufferFlushInterval = 2000;
		private int ttlSeconds = -1;
		private boolean logFailuresOnly;
		private boolean ignoreDelete = true;
		private boolean ignoreNull = false;
		private int parallelism;

		private AbaseInsertOptionsBuilder() {
		}

		public AbaseInsertOptionsBuilder setValueColIndices(int[] valueColIndices) {
			this.valueColIndices = valueColIndices;
			return this;
		}

		public AbaseInsertOptionsBuilder setSerColIndices(int[] serColIndices) {
			this.serColIndices = serColIndices;
			return this;
		}

		public AbaseInsertOptionsBuilder setFlushMaxRetries(int flushMaxRetries) {
			this.flushMaxRetries = flushMaxRetries;
			return this;
		}

		public AbaseInsertOptionsBuilder setMode(AbaseSinkMode mode) {
			this.mode = mode;
			return this;
		}

		public AbaseInsertOptionsBuilder setBufferMaxRows(int bufferMaxRows) {
			this.bufferMaxRows = bufferMaxRows;
			return this;
		}

		public AbaseInsertOptionsBuilder setBufferFlushInterval(long bufferFlushInterval) {
			this.bufferFlushInterval = bufferFlushInterval;
			return this;
		}

		public AbaseInsertOptionsBuilder setTtlSeconds(int ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
			return this;
		}

		public AbaseInsertOptionsBuilder setIgnoreDelete(boolean ignoreDelete) {
			this.ignoreDelete = ignoreDelete;
			return this;
		}

		public AbaseInsertOptionsBuilder setIgnoreNull(boolean ignoreNull) {
			this.ignoreNull = ignoreNull;
			return this;
		}

		public AbaseInsertOptionsBuilder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public AbaseInsertOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public AbaseSinkOptions build() {
			Preconditions.checkNotNull(valueColIndices, "valueIndices should be not null");
			Preconditions.checkArgument(flushMaxRetries > 0,
				"flushMaxRetries must be greater than 0");
			Preconditions.checkArgument(bufferMaxRows > 0,
				"batchSize must be greater than 0");
			Arrays.sort(valueColIndices);
			Arrays.sort(serColIndices);
			return new AbaseSinkOptions(
				valueColIndices,
				serColIndices,
				flushMaxRetries,
				mode,
				bufferMaxRows,
				bufferFlushInterval,
				ttlSeconds,
				logFailuresOnly,
				ignoreDelete,
				ignoreNull,
				parallelism);
		}

		@Override
		public String toString() {
			return "AbaseInsertOptionsBuilder{" +
				", valueIndices=" + Arrays.toString(valueColIndices) +
				", serColIndices=" + Arrays.toString(serColIndices) +
				", flushMaxRetries=" + flushMaxRetries +
				", mode=" + mode +
				", bufferMaxRows=" + bufferMaxRows +
				", bufferFlushInterval=" + bufferFlushInterval +
				", ttlSeconds=" + ttlSeconds +
				", logFailuresOnly=" + logFailuresOnly +
				", ignoreDelete=" + ignoreDelete +
				", ignoreNull=" + ignoreNull +
				", parallelism=" + parallelism +
				'}';
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AbaseSinkOptions)) {
			return false;
		}
		AbaseSinkOptions that = (AbaseSinkOptions) o;
		return Arrays.equals(valueColIndices, that.valueColIndices) &&
			Arrays.equals(serColIndices, that.serColIndices) &&
			flushMaxRetries == that.flushMaxRetries &&
			bufferMaxRows == that.bufferMaxRows &&
			bufferFlushInterval == that.bufferFlushInterval &&
			ttlSeconds == that.ttlSeconds &&
			parallelism == that.parallelism &&
			logFailuresOnly == that.logFailuresOnly &&
			ignoreDelete == that.ignoreDelete &&
			ignoreNull == that.ignoreNull &&
			mode == that.mode;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			Arrays.hashCode(valueColIndices),
			Arrays.hashCode(serColIndices),
			flushMaxRetries,
			mode,
			bufferMaxRows,
			bufferFlushInterval,
			ttlSeconds,
			parallelism,
			logFailuresOnly,
			ignoreDelete,
			ignoreNull);
	}
}
