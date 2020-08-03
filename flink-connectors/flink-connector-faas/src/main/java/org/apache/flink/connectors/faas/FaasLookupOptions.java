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

package org.apache.flink.connectors.faas;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Faas Lookup Options.
 */
public class FaasLookupOptions implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String url;
	private final int connectionTimeoutMs;
	private final int maxConnections;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;

	private FaasLookupOptions(
			String url,
			int connectionTimeoutMs,
			int maxConnections,
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes) {
		this.url = url;
		this.connectionTimeoutMs = connectionTimeoutMs;
		this.maxConnections = maxConnections;
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
	}

	public String getUrl() {
		return url;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public int getMaxConnections() {
		return maxConnections;
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

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder of {@link FaasLookupOptions}.
	 */
	public static class Builder {
		private String url;
		private int connectionTimeoutMs = 1000;
		private int maxConnections = 100;
		private long cacheMaxSize = -1L;
		private long cacheExpireMs = -1L;
		private int maxRetryTimes = 3;

		private Builder() {

		}

		/**
		 * required, url of faas function.
		 */
		public Builder setUrl(String url) {
			this.url = url;
			return this;
		}

		/**
		 * optional, faas connection timeout.
		 */
		public Builder setConnectionTimeoutMs(int connectionTimeoutMs) {
			this.connectionTimeoutMs = connectionTimeoutMs;
			return this;
		}

		/**
		 * optional, max number of connections that we can hold at a time.
		 */
		public Builder setMaxConnections(int maxConnections) {
			this.maxConnections = maxConnections;
			return this;
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
		 * optional, max retry times for faas request.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public FaasLookupOptions build() {
			Preconditions.checkNotNull(url, "url can not be null");
			return new FaasLookupOptions(
				url,
				connectionTimeoutMs,
				maxConnections,
				cacheMaxSize,
				cacheExpireMs,
				maxRetryTimes);
		}
	}
}
