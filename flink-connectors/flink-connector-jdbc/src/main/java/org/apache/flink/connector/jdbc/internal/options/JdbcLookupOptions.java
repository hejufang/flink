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

package org.apache.flink.connector.jdbc.internal.options;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.Serializable;
import java.util.Objects;

/**
 * Options for the JDBC lookup.
 */
public class JdbcLookupOptions implements Serializable {

	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final long laterRetryMs;
	private final int laterRetryTimes;
	private final boolean cacheNull;

	public JdbcLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			long laterRetryMs,
			int laterRetryTimes,
			boolean cacheNull) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.laterRetryMs = laterRetryMs;
		this.laterRetryTimes = laterRetryTimes;
		this.cacheNull = cacheNull;
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

	public long getLaterRetryMs() {
		return laterRetryMs;
	}

	public int getLaterRetryTimes() {
		return laterRetryTimes;
	}

	public boolean isCacheNull() {
		return cacheNull;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcLookupOptions) {
			JdbcLookupOptions options = (JdbcLookupOptions) o;
			return Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
				Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes) &&
				Objects.equals(laterRetryMs, options.laterRetryMs) &&
				Objects.equals(laterRetryTimes, options.laterRetryTimes) &&
				Objects.equals(cacheNull, options.cacheNull);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JdbcLookupOptions}.
	 */
	public static class Builder {
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = JdbcExecutionOptions.DEFAULT_MAX_RETRY_TIMES;
		private long laterRetryMs = -1;
		private int laterRetryTimes = FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES.defaultValue();
		private boolean cacheNull = FactoryUtil.LOOKUP_CACHE_NULL_VALUE.defaultValue();

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
		 * optional, set later retry times for jdbc connector.
		 * Only old type of connector use this Builder, so this method is unused
		 */
		public Builder setLaterRetryMs(long laterRetryMs) {
			this.laterRetryMs = laterRetryMs;
			return this;
		}

		public Builder setCacheNull(boolean cacheNull) {
			this.cacheNull = cacheNull;
			return this;
		}

		public void setLaterRetryTimes(int laterRetryTimes) {
			this.laterRetryTimes = laterRetryTimes;
		}

		public JdbcLookupOptions build() {
			return new JdbcLookupOptions(cacheMaxSize, cacheExpireMs, maxRetryTimes, laterRetryMs, laterRetryTimes, cacheNull);
		}
	}
}
