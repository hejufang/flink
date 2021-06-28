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

package org.apache.flink.runtime.state.cache;

/**
 * Record cache statistics.
 */
public class PolicyStats {
	private long requestCount;
	private long hitCount;
	private long missCount;
	private long evictionCount;
	private long loadSuccessCount;
	private long estimatedKVSize;
	private long cacheSize;

	public PolicyStats() {
		this.requestCount = 0L;
		this.hitCount = 0L;
		this.missCount = 0L;
		this.evictionCount = 0L;
		this.estimatedKVSize = 0L;
		this.cacheSize = 0L;
	}

	public void recordOperation() {
		this.requestCount++;
	}

	public void recordHit() {
		this.hitCount++;
	}

	public void recordMiss() {
		this.missCount++;
	}

	public void recordEviction() {
		this.evictionCount++;
	}

	public void recordLoadSuccess() {
		this.loadSuccessCount++;
	}

	public void recordEstimatedKVSize(long estimatedKVSize) {
		this.estimatedKVSize = estimatedKVSize;
	}

	public void recordCacheSize(long cacheSize) {
		this.cacheSize = cacheSize;
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

	public long getEvictionCount() {
		return evictionCount;
	}

	public long getLoadSuccessCount() {
		return loadSuccessCount;
	}

	public long getEstimatedKVSize() {
		return estimatedKVSize;
	}

	public long getCacheSize() {
		return cacheSize;
	}

	public long getEstimatedSize() {
		return estimatedKVSize * cacheSize;
	}
}
