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

package org.apache.flink.runtime.state.cache.monitor;

import org.apache.flink.configuration.MemorySize;

/**
 * Cache statistics collected during the running of the task, such as request count, hit count, etc.
 */
public class CacheStatistic {
	private MemorySize memorySize;
	private long requestCount;
	private long hitCount;
	private long missCount;
	private long loadSuccessCount;

	public CacheStatistic(
			MemorySize memorySize,
			long requestCount,
			long hitCount,
			long missCount,
			long loadSuccessCount) {
		this.memorySize = memorySize;
		this.requestCount = requestCount;
		this.hitCount = hitCount;
		this.missCount = missCount;
		this.loadSuccessCount = loadSuccessCount;
	}

	public MemorySize getMemorySize() {
		return memorySize;
	}

	public long getRequestCount() {
		return requestCount;
	}

	public long getHitCount() {
		return hitCount;
	}

	public long getMissCount() {
		return missCount;
	}

	public long getLoadSuccessCount() {
		return loadSuccessCount;
	}
}
