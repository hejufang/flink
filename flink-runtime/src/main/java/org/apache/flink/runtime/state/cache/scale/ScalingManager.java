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

package org.apache.flink.runtime.state.cache.scale;

import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.memory.MemoryManager;
import org.apache.flink.runtime.state.cache.monitor.CacheStatusMonitor;
import org.apache.flink.runtime.state.cache.monitor.HeapMonitorResult;
import org.apache.flink.runtime.state.cache.monitor.HeapStatusListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used for dynamic scale management of {@link Cache}.
 */
public class ScalingManager implements HeapStatusListener {
	private  static final Logger LOG = LoggerFactory.getLogger(ScalingManager.class);

	/** A manager used to manage cache memory. */
	private final MemoryManager memoryManager;
	/** A monitor used to monitor the status of the Cache. */
	private final CacheStatusMonitor cacheStatusMonitor;
	/** Weight calculator for scale up. */
	private final WeightCalculator scaleUpCalculator;
	/** Weight calculator for scale down. */
	private final WeightCalculator scaleDownCalculator;
	/** The number of caches that need to be selected for each scale. */
	private final int numberOfScaleCache;
	/** Callback after the scale is completed. */
	private final ScaleCallback scaleCallback;

	public ScalingManager(
			MemoryManager memoryManager,
			CacheStatusMonitor cacheStatusMonitor,
			WeightCalculator scaleUpCalculator,
			WeightCalculator scaleDownCalculator,
		int numberOfScaleCache) {
		this.numberOfScaleCache = numberOfScaleCache;
		this.memoryManager = memoryManager;
		this.cacheStatusMonitor = cacheStatusMonitor;
		this.scaleUpCalculator = scaleUpCalculator;
		this.scaleDownCalculator = scaleDownCalculator;
		this.scaleCallback = scaleResult -> {
			//TODO update memoryManager
		};
	}

	@Override
	public void notifyHeapStatus(HeapMonitorResult result) {
		// TODO scale the cache according to the monitoring results of the heap state.
	}
}
