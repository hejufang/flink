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

import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.LOOKUP_ASYNC_CONCURRENCY;

/**
 * Options for the ByteSQL lookup.
 */
public class ByteSQLLookupOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final int laterJoinRetryTimes;
	private final long laterJoinLatency;
	private final boolean isAsync;
	private final int asyncConcurrency;
	@Nullable
	private final Boolean isInputKeyByEnabled;

	public ByteSQLLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			int laterJoinRetryTimes,
			long laterJoinLatency,
			boolean isAsync,
			int asyncConcurrency,
			@Nullable Boolean isInputKeyByEnabled) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.laterJoinRetryTimes = laterJoinRetryTimes;
		this.laterJoinLatency = laterJoinLatency;
		this.isAsync = isAsync;
		this.asyncConcurrency = asyncConcurrency;
		this.isInputKeyByEnabled = isInputKeyByEnabled;
	}

	public long getCacheMaxSize() {
		return cacheMaxSize;
	}

	public long getCacheExpireMs() {
		return cacheExpireMs;
	}

	public int getMaxRetryTimes() {
		return maxRetryTimes;
	}

	public int getLaterJoinRetryTimes() {
		return laterJoinRetryTimes;
	}

	public long getLaterJoinLatency() {
		return laterJoinLatency;
	}

	public boolean isAsync() {
		return isAsync;
	}

	public int getAsyncConcurrency() {
		return asyncConcurrency;
	}

	@Nullable
	public Boolean isInputKeyByEnabled() {
		return isInputKeyByEnabled;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ByteSQLLookupOptions) {
			ByteSQLLookupOptions options = (ByteSQLLookupOptions) o;
			return cacheMaxSize == options.cacheMaxSize &&
				cacheExpireMs == options.cacheExpireMs &&
				maxRetryTimes == options.maxRetryTimes &&
				laterJoinLatency == options.laterJoinLatency &&
				laterJoinRetryTimes == options.laterJoinRetryTimes &&
				isAsync == options.isAsync &&
				asyncConcurrency == options.asyncConcurrency &&
				isInputKeyByEnabled == options.isInputKeyByEnabled;
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link ByteSQLLookupOptions}.
	 */
	public static class Builder {
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = FactoryUtil.LOOKUP_MAX_RETRIES.defaultValue();
		private int laterJoinRetryTimes = FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES.defaultValue();
		private long laterJoinLatency = FactoryUtil.LOOKUP_LATER_JOIN_LATENCY.defaultValue().toMillis();
		private boolean isAsync;
		private int asyncConcurrency = LOOKUP_ASYNC_CONCURRENCY.defaultValue();
		// The null default value means this flag is not set by user.
		private Boolean isInputKeyByEnabled = null;

		/**
		 * optional, lookup cache max size, over this value, the old data will be eliminated.
		 */
		public Builder setCacheMaxSize(long cacheMaxSize) {
			this.cacheMaxSize = cacheMaxSize;
			return this;
		}

		/**
		 * optional, lookup cache expire mills, over this time, the old data will expire.
		 */
		public Builder setCacheExpireMs(long cacheExpireMs) {
			this.cacheExpireMs = cacheExpireMs;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setLaterJoinRetryTimes(int laterJoinRetryTimes) {
			this.laterJoinRetryTimes = laterJoinRetryTimes;
			return this;
		}

		public Builder setLaterJoinLatency(long laterJoinLatency) {
			this.laterJoinLatency = laterJoinLatency;
			return this;
		}

		public Builder setAsync(boolean async) {
			isAsync = async;
			return this;
		}

		public Builder setAsyncConcurrency(int asyncConcurrency) {
			this.asyncConcurrency = asyncConcurrency;
			return this;
		}

		public Builder setIsInputKeyByEnabled(Boolean isInputKeyByEnabled) {
			this.isInputKeyByEnabled = isInputKeyByEnabled;
			return this;
		}

		public ByteSQLLookupOptions build() {
			return new ByteSQLLookupOptions(
				cacheMaxSize,
				cacheExpireMs,
				maxRetryTimes,
				laterJoinRetryTimes,
				laterJoinLatency,
				isAsync,
				asyncConcurrency,
				isInputKeyByEnabled);
		}
	}
}
