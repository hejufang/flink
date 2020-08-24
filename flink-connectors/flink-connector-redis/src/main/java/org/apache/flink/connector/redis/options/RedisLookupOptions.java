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

import java.io.Serializable;
import java.util.Objects;

/**
 * Options for Redis lookup.
 */
public class RedisLookupOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	/**
	 * Specifies the maximum number of entries the cache may contain.
	 */
	private final long cacheMaxSize;

	private final long cacheExpireMs;

	private final int maxRetryTimes;

	public RedisLookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
		this.cacheMaxSize = cacheMaxSize;
		this.cacheExpireMs = cacheExpireMs;
		this.maxRetryTimes = maxRetryTimes;
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

	@Override
	public boolean equals(Object o) {
		if (o instanceof RedisLookupOptions) {
			RedisLookupOptions options = (RedisLookupOptions) o;
			return Objects.equals(cacheMaxSize, options.cacheMaxSize) &&
				Objects.equals(cacheExpireMs, options.cacheExpireMs) &&
				Objects.equals(maxRetryTimes, options.maxRetryTimes);
		} else {
			return false;
		}
	}
}
