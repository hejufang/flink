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

package org.apache.flink.connector.rpc.table.descriptors;

import org.apache.flink.connector.rpc.FailureHandleStrategy;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_ASYNC_CONCURRENCY;
import static org.apache.flink.connector.rpc.table.descriptors.RPCConfigs.LOOKUP_FAILURE_HANDLE_STRATEGY;

/**
 * Options for the RPC lookup.
 */
public class RPCLookupOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_MAX_RETRY_TIMES = 3;

	/**
	 * Specifies the maximum number of entries the cache may contain.
	 */
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final FailureHandleStrategy failureHandleStrategy;
	private final boolean isAsync;
	private final int asyncConcurrency;
	@Nullable
	private final Boolean isInputKeyByEnabled;

	private RPCLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			FailureHandleStrategy failureHandleStrategy,
			boolean isAsync,
			int asyncConcurrency,
			@Nullable Boolean isInputKeyByEnabled) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.failureHandleStrategy = failureHandleStrategy;
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

	public FailureHandleStrategy getFailureHandleStrategy() {
		return failureHandleStrategy;
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
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RPCLookupOptions that = (RPCLookupOptions) o;
		return cacheMaxSize == that.cacheMaxSize &&
			cacheExpireMs == that.cacheExpireMs &&
			maxRetryTimes == that.maxRetryTimes &&
			failureHandleStrategy == that.failureHandleStrategy &&
			isAsync == that.isAsync &&
			asyncConcurrency == that.asyncConcurrency &&
			isInputKeyByEnabled == that.isInputKeyByEnabled;
	}

	@Override
	public int hashCode() {
		return Objects.hash(cacheMaxSize, cacheExpireMs, maxRetryTimes, failureHandleStrategy,
			isAsync, asyncConcurrency, isInputKeyByEnabled);
	}

	/**
	 * Builder of {@link RPCLookupOptions}.
	 */
	public static class Builder {
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
		private FailureHandleStrategy failureHandleStrategy = LOOKUP_FAILURE_HANDLE_STRATEGY.defaultValue();
		private boolean isAsync;
		private int asyncConcurrency = LOOKUP_ASYNC_CONCURRENCY.defaultValue();
		// The null default value means this flag is not set by user.
		private Boolean isInputKeyByEnabled = null;

		private Builder() {
		}

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
		 * optional, max retry times for RPC connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setFailureHandleStrategy(FailureHandleStrategy strategy){
			this.failureHandleStrategy = strategy;
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

		public RPCLookupOptions build() {
			return new RPCLookupOptions(
				cacheMaxSize,
				cacheExpireMs,
				maxRetryTimes,
				failureHandleStrategy,
				isAsync,
				asyncConcurrency,
				isInputKeyByEnabled);
		}
	}

}
