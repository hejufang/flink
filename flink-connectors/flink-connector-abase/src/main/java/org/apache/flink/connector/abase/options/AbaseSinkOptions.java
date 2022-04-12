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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

	/**
	 * Indices of columns that are not written to abase.
	 */
	private final List<Integer> skipIdx;

	private final boolean ignoreDelete;

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

	public List<Integer> getSkipIdx() {
		return skipIdx;
	}

	public boolean isIgnoreDelete() {
		return ignoreDelete;
	}

	public long getBufferFlushInterval() {
		return bufferFlushInterval;
	}

	private AbaseSinkOptions(
			int flushMaxRetries,
			AbaseSinkMode mode,
			int bufferMaxRows,
			long bufferFlushInterval,
			int ttlSeconds,
			boolean logFailuresOnly,
			List<Integer> skipIdx,
			boolean ignoreDelete,
			int parallelism) {
		this.flushMaxRetries = flushMaxRetries;
		this.mode = mode;
		this.bufferMaxRows = bufferMaxRows;
		this.bufferFlushInterval = bufferFlushInterval;
		this.ttlSeconds = ttlSeconds;
		this.logFailuresOnly = logFailuresOnly;
		this.skipIdx = skipIdx;
		this.ignoreDelete = ignoreDelete;
		this.parallelism = parallelism;
	}

	public static AbaseInsertOptionsBuilder builder() {
		return new AbaseInsertOptionsBuilder();
	}

	/**
	 * Abase insert options builder.
	 */
	public static class AbaseInsertOptionsBuilder {
		private int flushMaxRetries = 5;
		private AbaseSinkMode mode = AbaseSinkMode.INSERT;
		private int bufferMaxRows = 50;
		private long bufferFlushInterval = 2000;
		private int ttlSeconds = -1;
		private boolean logFailuresOnly;
		private List<Integer> skipIdx = new ArrayList<>();
		private boolean ignoreDelete = true;
		private int parallelism;

		private AbaseInsertOptionsBuilder() {
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

		public AbaseInsertOptionsBuilder setSkipIdx(List<Integer> skipIdx) {
			this.skipIdx = skipIdx;
			return this;
		}

		public AbaseInsertOptionsBuilder setIgnoreDelete(boolean ignoreDelete) {
			this.ignoreDelete = ignoreDelete;
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
			Preconditions.checkArgument(flushMaxRetries > 0,
				"flushMaxRetries must be greater than 0");
			Preconditions.checkArgument(bufferMaxRows > 0,
				"batchSize must be greater than 0");
			Collections.sort(skipIdx);
			return new AbaseSinkOptions(
				flushMaxRetries,
				mode,
				bufferMaxRows,
				bufferFlushInterval,
				ttlSeconds,
				logFailuresOnly,
				skipIdx,
				ignoreDelete,
				parallelism);
		}

		@Override
		public String toString() {
			return "AbaseInsertOptionsBuilder{" +
				"flushMaxRetries=" + flushMaxRetries +
				", mode=" + mode +
				", bufferMaxRows=" + bufferMaxRows +
				", bufferFlushInterval=" + bufferFlushInterval +
				", ttlSeconds=" + ttlSeconds +
				", logFailuresOnly=" + logFailuresOnly +
				", skipIdx=" + skipIdx +
				", ignoreDelete=" + ignoreDelete +
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
		return flushMaxRetries == that.flushMaxRetries &&
			bufferMaxRows == that.bufferMaxRows &&
			bufferFlushInterval == that.bufferFlushInterval &&
			ttlSeconds == that.ttlSeconds &&
			parallelism == that.parallelism &&
			logFailuresOnly == that.logFailuresOnly &&
			Objects.equals(skipIdx, that.skipIdx) &&
			ignoreDelete == that.ignoreDelete &&
			mode == that.mode;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			flushMaxRetries,
			mode,
			bufferMaxRows,
			bufferFlushInterval,
			ttlSeconds,
			parallelism,
			logFailuresOnly,
			skipIdx,
			ignoreDelete);
	}
}
