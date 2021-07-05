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
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Responsible for monitoring the running status of all caches and providing weight
 * calculation data for {@link org.apache.flink.runtime.state.cache.scale.ScalingManager}.
 */
public class CacheStatusMonitor {
	private static final Logger LOG = LoggerFactory.getLogger(CacheStatusMonitor.class);

	private final Map<Cache, PolicyStats> cacheStatus;

	private volatile boolean running;

	public CacheStatusMonitor() {
		this.cacheStatus = new ConcurrentHashMap<>();
		this.running = true;
	}

	public PolicyStats registerCache(Cache cache) {
		Preconditions.checkState(running, "Cache status monitor not running");
		return cacheStatus.computeIfAbsent(cache, reference -> new PolicyStats(reference));
	}

	public void unRegisterCache(Cache cache) {
		Preconditions.checkState(running, "Cache status monitor not running");
		cacheStatus.remove(cache);
	}

	/**
	 * Get runtime statistics of all caches.
	 */
	public Map<Cache, CacheStatistic> getCacheStatusStatistics() throws IOException {
		Preconditions.checkState(running, "Cache status monitor not running");
		Map<Cache, CacheStatistic> cacheStatistics = new HashMap<>(cacheStatus.size());
		for (Map.Entry<Cache, PolicyStats> entry : cacheStatus.entrySet()) {
			cacheStatistics.put(entry.getKey(), entry.getValue().snapshot());
		}
		return cacheStatistics;
	}

	public CacheStatistic getCacheStatusStatistic(Cache cache) {
		Preconditions.checkState(running, "Cache status monitor not running");
		PolicyStats policyStats = cacheStatus.get(cache);
		return policyStats != null ? policyStats.snapshot() : CacheStatistic.EMPTY_STATISTIC;
	}

	public void shutdown() {
		this.running = false;
	}
}
