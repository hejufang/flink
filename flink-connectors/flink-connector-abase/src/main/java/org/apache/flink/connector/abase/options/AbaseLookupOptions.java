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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * lookup options for abase: source.
 */
public class AbaseLookupOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * Specifies the maximum number of entries the cache may contain.
	 */
	private final long cacheMaxSize;

	private final long cacheExpireMs;

	private final int maxRetryTimes;

	private final long laterRetryMs;

	private final int laterRetryTimes;

	private final boolean cacheNull;

	@Nullable
	private final Boolean isInputKeyByEnabled;

	private final List<String> requestedHashKeys;

	public AbaseLookupOptions(
			long cacheMaxSize,
			long cacheExpireMs,
			int maxRetryTimes,
			long laterRetryMs,
			int laterRetryTimes,
			boolean cacheNull,
			@Nullable Boolean isInputKeyByEnabled,
			List<String> requestedHashKeys) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
		this.laterRetryMs = laterRetryMs;
		this.laterRetryTimes = laterRetryTimes;
		this.cacheNull = cacheNull;
		this.isInputKeyByEnabled = isInputKeyByEnabled;
		this.requestedHashKeys = requestedHashKeys;
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

	@Nullable
	public Boolean isInputKeyByEnabled() {
		return isInputKeyByEnabled;
	}

	public List<String> getRequestedHashKeys() {
		return requestedHashKeys;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof AbaseLookupOptions) {
			AbaseLookupOptions options = (AbaseLookupOptions) o;
			return Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
				Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes) &&
				Objects.equals(laterRetryMs, options.laterRetryMs) &&
				Objects.equals(laterRetryTimes, options.laterRetryTimes) &&
				Objects.equals(cacheNull, options.cacheNull) &&
				Objects.equals(isInputKeyByEnabled, options.isInputKeyByEnabled) &&
				Objects.equals(requestedHashKeys, options.requestedHashKeys);
		} else {
			return false;
		}
	}
}
