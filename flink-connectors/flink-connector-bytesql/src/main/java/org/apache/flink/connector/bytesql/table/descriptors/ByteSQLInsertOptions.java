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

package org.apache.flink.connector.bytesql.table.descriptors;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Options for writing ByteSQL.
 */
public class ByteSQLInsertOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_MAX_RETRY_TIMES = 5;
	private final int bufferFlushMaxRows;
	private final long bufferFlushIntervalMills;
	private final int maxRetryTimes;
	private final int parallelism;
	private final String[] keyFields;
	private final boolean ignoreNull;
	private final boolean logFailuresOnly;
	private final boolean ignoreDelete;
	private final int ttlSeconds;

	private ByteSQLInsertOptions(
			int bufferFlushMaxRows,
			long bufferFlushIntervalMills,
			int maxRetryTimes,
			int parallelism,
			String[] keyFields,
			boolean ignoreNull,
			boolean logFailuresOnly,
			boolean ignoreDelete,
			int ttlSeconds) {
		this.bufferFlushMaxRows = bufferFlushMaxRows;
		this.bufferFlushIntervalMills = bufferFlushIntervalMills;
		this.maxRetryTimes = maxRetryTimes;
		this.parallelism = parallelism;
		this.keyFields = keyFields;
		this.ignoreNull = ignoreNull;
		this.logFailuresOnly = logFailuresOnly;
		this.ignoreDelete = ignoreDelete;
		this.ttlSeconds = ttlSeconds;
	}

	public int getBufferFlushMaxRows() {
		return bufferFlushMaxRows;
	}

	public long getBufferFlushIntervalMills() {
		return bufferFlushIntervalMills;
	}

	public int getMaxRetryTimes() {
		return maxRetryTimes;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	public boolean isIgnoreNull() {
		return ignoreNull;
	}

	public boolean isLogFailuresOnly() {
		return logFailuresOnly;
	}

	public boolean isIgnoreDelete() {
		return ignoreDelete;
	}

	public int getTtlSeconds() {
		return ttlSeconds;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ByteSQLInsertOptions) {
			ByteSQLInsertOptions options = (ByteSQLInsertOptions) o;
			return Objects.equals(bufferFlushMaxRows, options.bufferFlushMaxRows) &&
				Objects.equals(bufferFlushIntervalMills, options.bufferFlushIntervalMills) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes) &&
				Objects.equals(parallelism, options.parallelism) &&
				Arrays.equals(keyFields, options.keyFields) &&
				(ignoreNull == options.ignoreNull) &&
				(logFailuresOnly == options.logFailuresOnly) &&
				(ignoreDelete == options.ignoreDelete) &&
				(ttlSeconds == options.getTtlSeconds());
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link ByteSQLInsertOptions}.
	 */
	public static class Builder {
		private int bufferFlushMaxRows = 10;
		private long bufferFlushIntervalMills = 1000;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
		private int parallelism;
		private String[] keyFields;
		private boolean ignoreNull;
		private boolean logFailuresOnly;
		private boolean ignoreDelete = true;
		private int ttlSeconds = 0;

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setBufferFlushMaxRows(int bufferFlushMaxRows) {
			this.bufferFlushMaxRows = bufferFlushMaxRows;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setBufferFlushIntervalMills(long bufferFlushIntervalMills) {
			this.bufferFlushIntervalMills = bufferFlushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public Builder setKeyFields(String[] keyFields) {
			this.keyFields = keyFields;
			return this;
		}

		public Builder setIgnoreNull(boolean ignoreNull) {
			this.ignoreNull = ignoreNull;
			return this;
		}

		public Builder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public Builder setIgnoreDelete(boolean ignoreDelete) {
			this.ignoreDelete = ignoreDelete;
			return this;
		}

		public Builder setTtlSeconds(int ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
			return this;
		}

		public ByteSQLInsertOptions build() {
			return new ByteSQLInsertOptions(
				bufferFlushMaxRows,
				bufferFlushIntervalMills,
				maxRetryTimes,
				parallelism,
				keyFields,
				ignoreNull,
				logFailuresOnly,
				ignoreDelete,
				ttlSeconds);
		}
	}
}
