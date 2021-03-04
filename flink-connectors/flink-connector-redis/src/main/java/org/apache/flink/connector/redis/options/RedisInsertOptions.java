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

package org.apache.flink.connector.redis.options;

import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Options for writing into redis.
 */
public class RedisInsertOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final int flushMaxRetries;
	private final RedisSinkMode mode;
	private final int bufferMaxRows;
	private final long bufferFlushInterval;
	private final int ttlSeconds;
	private final int parallelism;
	/**
	 * Flag indicating whether to ignore failures (and log them), or to fail on failures.
	 */
	private final boolean logFailuresOnly;
	/**
	 * Flag indicating whether to only serialization without key.
	 */
	private final boolean skipFormatKey;

	private final boolean ignoreDelete;

	public int getFlushMaxRetries() {
		return flushMaxRetries;
	}

	public RedisSinkMode getMode() {
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

	public boolean isSkipFormatKey() {
		return skipFormatKey;
	}

	public boolean isIgnoreDelete() {
		return ignoreDelete;
	}

	public long getBufferFlushInterval() {
		return bufferFlushInterval;
	}

	private RedisInsertOptions(
			int flushMaxRetries,
			RedisSinkMode mode,
			int bufferMaxRows,
			long bufferFlushInterval,
			int ttlSeconds,
			boolean logFailuresOnly,
			boolean skipFormatKey,
			boolean ignoreDelete,
			int parallelism) {
		this.flushMaxRetries = flushMaxRetries;
		this.mode = mode;
		this.bufferMaxRows = bufferMaxRows;
		this.bufferFlushInterval = bufferFlushInterval;
		this.ttlSeconds = ttlSeconds;
		this.logFailuresOnly = logFailuresOnly;
		this.skipFormatKey = skipFormatKey;
		this.ignoreDelete = ignoreDelete;
		this.parallelism = parallelism;
	}

	public static RedisInsertOptionsBuilder builder() {
		return new RedisInsertOptionsBuilder();
	}

	/**
	 * Redis insert options builder.
	 */
	public static class RedisInsertOptionsBuilder {
		private int flushMaxRetries = 5;
		private RedisSinkMode mode = RedisSinkMode.INSERT;
		private int bufferMaxRows = 50;
		private long bufferFlushInterval = 2000;
		private int ttlSeconds = -1;
		private boolean logFailuresOnly;
		private boolean skipFormatKey;
		private boolean ignoreDelete = true;
		private int parallelism;

		private RedisInsertOptionsBuilder() {
		}

		public RedisInsertOptionsBuilder setFlushMaxRetries(int flushMaxRetries) {
			this.flushMaxRetries = flushMaxRetries;
			return this;
		}

		public RedisInsertOptionsBuilder setMode(RedisSinkMode mode) {
			this.mode = mode;
			return this;
		}

		public RedisInsertOptionsBuilder setBufferMaxRows(int bufferMaxRows) {
			this.bufferMaxRows = bufferMaxRows;
			return this;
		}

		public RedisInsertOptionsBuilder setBufferFlushInterval(long bufferFlushInterval) {
			this.bufferFlushInterval = bufferFlushInterval;
			return this;
		}

		public RedisInsertOptionsBuilder setTtlSeconds(int ttlSeconds) {
			this.ttlSeconds = ttlSeconds;
			return this;
		}

		public RedisInsertOptionsBuilder setSkipFormatKey(boolean skipFormatKey) {
			this.skipFormatKey = skipFormatKey;
			return this;
		}

		public RedisInsertOptionsBuilder setIgnoreDelete(boolean ignoreDelete) {
			this.ignoreDelete = ignoreDelete;
			return this;
		}

		public RedisInsertOptionsBuilder setLogFailuresOnly(boolean logFailuresOnly) {
			this.logFailuresOnly = logFailuresOnly;
			return this;
		}

		public RedisInsertOptionsBuilder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public RedisInsertOptions build() {
			Preconditions.checkArgument(flushMaxRetries > 0,
				"flushMaxRetries must be greater than 0");
			Preconditions.checkArgument(bufferMaxRows > 0,
				"batchSize must be greater than 0");
			return new RedisInsertOptions(
				flushMaxRetries,
				mode,
				bufferMaxRows,
				bufferFlushInterval,
				ttlSeconds,
				logFailuresOnly,
				skipFormatKey,
				ignoreDelete,
				parallelism);
		}

		@Override
		public String toString() {
			return "RedisInsertOptionsBuilder{" +
				"flushMaxRetries=" + flushMaxRetries +
				", mode='" + mode + '\'' +
				", bufferMaxRows=" + bufferMaxRows +
				", bufferFlushInterval=" + bufferFlushInterval +
				", ttlSeconds=" + ttlSeconds +
				", logFailuresOnly=" + logFailuresOnly +
				", skipFormatKey=" + skipFormatKey +
				", ignoreDelete=" + ignoreDelete +
				", parallelism=" + parallelism +
				'}';
		}
	}
}
