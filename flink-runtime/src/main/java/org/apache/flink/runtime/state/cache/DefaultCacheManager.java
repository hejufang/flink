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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.memory.CacheMemoryManager;
import org.apache.flink.runtime.state.cache.monitor.CacheStatistic;
import org.apache.flink.runtime.state.cache.monitor.CacheStatusMonitor;
import org.apache.flink.runtime.state.cache.monitor.HeapStatusListener;
import org.apache.flink.runtime.state.cache.monitor.HeapStatusMonitor;
import org.apache.flink.runtime.state.cache.scale.CacheWeightMeta;
import org.apache.flink.runtime.state.cache.scale.DefaultCacheWeightCalculator;
import org.apache.flink.runtime.state.cache.scale.ScalingManager;
import org.apache.flink.runtime.state.cache.scale.WeightCalculator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for managing all registered {@link Cache} in {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
public class DefaultCacheManager implements CacheManager {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultCacheManager.class);

	/** A manager that manages the memory used by the cache. */
	private final CacheMemoryManager cacheMemoryManager;
	/** A monitor that monitors the status of the heap. */
	private final HeapStatusMonitor heapStatusMonitor;
	/** A monitor that monitors the running status of the cache. */
	private final CacheStatusMonitor cacheStatusMonitor;
	/** A manager that manages the cache to scale. */
	private final ScalingManager scalingManager;
	private volatile boolean running;

	public DefaultCacheManager(CacheConfiguration configuration) {
		this.cacheMemoryManager = initializeMemoryManager(configuration);
		this.cacheStatusMonitor = initializeCacheStatusMonitor();
		this.scalingManager = initializeScalingManager(cacheMemoryManager, cacheStatusMonitor, configuration);
		this.heapStatusMonitor = initializeHeapStatusMonitor(scalingManager, configuration);
		this.heapStatusMonitor.startMonitor();
		this.running = true;
	}

	@Override
	public PolicyStats registerCache(TaskInfo taskInfo, String name, Cache cache, MemorySize initialSize) {
		Preconditions.checkState(running, "cache manager not running");
		LOG.info("Task[{}] register cache[{}] with {}", taskInfo.getTaskNameWithSubtasks(), name, cache);
		PolicyStats policyStats = cacheStatusMonitor.registerCache(cache);
		MemorySize allocateMemory = cacheMemoryManager.allocateMemory(initialSize);
		policyStats.recordMaxCacheMemorySize(allocateMemory);
		return policyStats;
	}

	@Override
	public void unregisterCache(TaskInfo taskInfo, String name, Cache cache) {
		Preconditions.checkState(running, "cache manager not running");
		LOG.info("Task[{}] unRegister cache[{}] with {}", taskInfo.getTaskNameWithSubtasks(), name, cache);
		CacheStatistic cacheStatistic = cacheStatusMonitor.getCacheStatusStatistic(cache);
		cacheStatusMonitor.unRegisterCache(cache);
		cacheMemoryManager.releaseMemory(cacheStatistic.getMaxMemorySize());
	}

	@Override
	public void shutdown() {
		running = false;
		heapStatusMonitor.shutDown();
		scalingManager.shutdown();
		cacheStatusMonitor.shutdown();
		cacheMemoryManager.shutdown();
	}

	//--------------------------------------------------------------------
	//		Initialize the services of internal components
	//--------------------------------------------------------------------

	public CacheMemoryManager initializeMemoryManager(CacheConfiguration configuration) {
		MemorySize totalSize = configuration.getMaxHeapSize();
		MemorySize blockSize = configuration.getBlockSize();
		double scaleUpRatio = configuration.getScaleUpRatio();
		double scaleDownRatio = configuration.getScaleDownRatio();

		return new CacheMemoryManager(totalSize, blockSize, scaleUpRatio, scaleDownRatio);
	}

	public CacheStatusMonitor initializeCacheStatusMonitor() {
		return new CacheStatusMonitor();
	}

	public ScalingManager initializeScalingManager(
			CacheMemoryManager cacheMemoryManager,
			CacheStatusMonitor cacheStatusMonitor,
			CacheConfiguration configuration) {

		WeightCalculator<CacheWeightMeta> scaleUpCalculator = new DefaultCacheWeightCalculator(
				configuration.getScaleUpRetainedSizeWeight(),
				configuration.getScaleUpLoadSuccessCountWeight());

		WeightCalculator<CacheWeightMeta> scaleDownCalculator = new DefaultCacheWeightCalculator(
				configuration.getScaleDownRetainedSizeWeight(),
				configuration.getScaleDownLoadSuccessCountWeight());

		return new ScalingManager(
			cacheMemoryManager,
			cacheStatusMonitor,
			scaleUpCalculator,
			scaleDownCalculator,
			configuration.getScaleCondition(),
			configuration.isEnableScale(),
			configuration.getScaleNum(),
			configuration.getCacheMinSize(),
			configuration.getCacheMaxSize());
	}

	public HeapStatusMonitor initializeHeapStatusMonitor(HeapStatusListener listener, CacheConfiguration configuration) {
		long interval = configuration.getHeapMonitorInterval();
		return new HeapStatusMonitor(interval, listener);
	}
}
