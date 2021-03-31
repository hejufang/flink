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

package org.apache.flink.api.java.io.jdbc;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.api.java.io.jdbc.JDBCUpsertOutputFormat.DEFAULT_MAX_RETRY_TIMES;

/**
 * Options for the JDBC lookup.
 */
public class JDBCLookupOptions implements Serializable {

	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final boolean cacheNullValue;
	@Nullable
	private final Boolean isInputKeyByEnabled;

	private JDBCLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			boolean cacheNullValue,
			@Nullable Boolean isInputKeyByEnabled) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.cacheNullValue = cacheNullValue;
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

	public boolean isCacheNullValue() {
		return cacheNullValue;
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
		if (o instanceof JDBCLookupOptions) {
			JDBCLookupOptions options = (JDBCLookupOptions) o;
			return Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
				Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes) &&
				Objects.equals(cacheNullValue, options.cacheNullValue) &&
				isInputKeyByEnabled == options.isInputKeyByEnabled;
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JDBCLookupOptions}.
	 */
	public static class Builder {
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = DEFAULT_MAX_RETRY_TIMES;
		private boolean cacheNullValue = true;
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

		/**
		 * optional, flag to indicate whether to hash the input stream by join key.
		 */
		public Builder setIsInputKeyByEnabled(boolean inputKeyByEnabled) {
			this.isInputKeyByEnabled = inputKeyByEnabled;
			return this;
		}

		public void setCacheNullValue(boolean cacheNullValue) {
			this.cacheNullValue = cacheNullValue;
		}

		public JDBCLookupOptions build() {
			return new JDBCLookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes, cacheNullValue, isInputKeyByEnabled);
		}
	}
}
