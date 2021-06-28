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

import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.PolicyStats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for monitoring the running status of all caches and providing weight
 * calculation data for {@link org.apache.flink.runtime.state.cache.scale.ScalingManager}.
 */
public class CacheStatusMonitor {
	private final Map<Cache, PolicyStats> cacheStatus;

	public CacheStatusMonitor() {
		this.cacheStatus = new HashMap<>();
	}

	public PolicyStats registerCache(Cache cache) {
		//TODO register cache and create policyStats object for cache
		return new PolicyStats();
	}

	/**
	 * Get runtime statistics of all caches.
	 */
	public Map<Cache, CacheStatistic> getCacheStatusStatistics() {
		//TODO try to get all cache statistic
		return Collections.emptyMap();
	}
}
