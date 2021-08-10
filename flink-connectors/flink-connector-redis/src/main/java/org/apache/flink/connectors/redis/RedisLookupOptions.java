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

package org.apache.flink.connectors.redis;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Options for the Redis lookup.
 */
public class RedisLookupOptions implements Serializable {
	private static final int DEFAULT_MAX_RETRY_TIMES = 4;

	/**
	 * Specifies the maximum number of entries the cache may contain.
	 */
	private final long cacheMaxSize;

	private final long cacheExpireMs;

	private final int maxRetryTimes;

	private final boolean cacheNullValue;

	private final String keyField;

	private final long rateLimit;

	@Nullable
	private final Boolean isInputKeyByEnabled;

	private final boolean specifyHashKeys;

	private RedisLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			boolean cacheNullValue,
			String keyField,
			long rateLimit,
			@Nullable Boolean isInputKeyByEnabled,
			boolean specifyHashKeys) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.cacheNullValue = cacheNullValue;
		this.keyField = keyField;
		this.rateLimit = rateLimit;
		this.isInputKeyByEnabled = isInputKeyByEnabled;
		this.specifyHashKeys = specifyHashKeys;
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

	public boolean isCacheNullValue() {
		return cacheNullValue;
	}

	public String getKeyField() {
		return keyField;
	}

	public long getRateLimit() {
		return rateLimit;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Nullable
	public Boolean isInputKeyByEnabled() {
		return isInputKeyByEnabled;
	}

	public boolean isSpecifyHashKeys() {
		return specifyHashKeys;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof RedisLookupOptions) {
			RedisLookupOptions options = (RedisLookupOptions) o;
			return Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
					Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
					Objects.equals(maxRetryTimes, options.maxRetryTimes) &&
					Objects.equals(cacheNullValue, options.cacheNullValue) &&
					Objects.equals(keyField, options.keyField) &&
					Objects.equals(rateLimit, options.rateLimit) &&
					Objects.equals(isInputKeyByEnabled, options.isInputKeyByEnabled) &&
					Objects.equals(specifyHashKeys, options.specifyHashKeys);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link RedisLookupOptions}.
	 */
	public static class Builder {
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
		private boolean cacheNullValue = true;
		private String keyField;
		// -1 means to disable rate limit
		private long rateLimit = -1L;
		// The null default value means this flag is not set by user.
		private Boolean isInputKeyByEnabled = null;
		private boolean specifyHashKeys = false;

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
		 * optional, max retry times for redis connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public Builder setCacheNullValue(boolean cacheNullValue) {
			this.cacheNullValue = cacheNullValue;
			return this;
		}

		public Builder setKeyField(String keyField) {
			this.keyField = keyField;
			return this;
		}

		public Builder setRateLimit(long rateLimit) {
			this.rateLimit = rateLimit;
			return this;
		}

		public Builder setIsInputKeyByEnabled(Boolean isInputKeyByEnabled) {
			this.isInputKeyByEnabled = isInputKeyByEnabled;
			return this;
		}

		public Builder setSpecifyHashKeys(boolean specifyHashKeys) {
			this.specifyHashKeys = specifyHashKeys;
			return this;
		}

		public RedisLookupOptions build() {
			return new RedisLookupOptions(
				cacheMaxSize,
				cacheExpireMs,
				maxRetryTimes,
				cacheNullValue,
				keyField,
				rateLimit,
				isInputKeyByEnabled,
				specifyHashKeys);
		}
	}

}
