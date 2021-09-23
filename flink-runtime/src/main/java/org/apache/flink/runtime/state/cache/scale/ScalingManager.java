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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.memory.CacheMemoryManager;
import org.apache.flink.runtime.state.cache.monitor.CacheStatistic;
import org.apache.flink.runtime.state.cache.monitor.CacheStatusMonitor;
import org.apache.flink.runtime.state.cache.monitor.HeapMonitorResult;
import org.apache.flink.runtime.state.cache.monitor.HeapStatusListener;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Used for dynamic scale management of {@link Cache}.
 */
@SuppressWarnings("rawtypes")
public class ScalingManager implements HeapStatusListener {
	private  static final Logger LOG = LoggerFactory.getLogger(ScalingManager.class);

	/** A manager used to manage cache memory. */
	private final CacheMemoryManager cacheMemoryManager;

	/** A monitor used to monitor the status of the Cache. */
	private final CacheStatusMonitor cacheStatusMonitor;

	/** Weight calculator for scale up. */
	private final WeightCalculator<CacheWeightMeta> scaleUpCalculator;

	/** Weight calculator for scale down. */
	private final WeightCalculator<CacheWeightMeta> scaleDownCalculator;

	/** The condition that triggers the scale. */
	private final ScaleCondition scaleCondition;

	/** The number of caches that need to be selected for each scale. */
	private final int numberOfScaleCache;

	/** Callback after the scale is completed. */
	private final ScaleCallback scaleCallback;

	/** Indicates whether the service is still running. */
	private volatile boolean running;

	public ScalingManager(
			CacheMemoryManager cacheMemoryManager,
			CacheStatusMonitor cacheStatusMonitor,
			WeightCalculator<CacheWeightMeta> scaleUpCalculator,
			WeightCalculator<CacheWeightMeta> scaleDownCalculator,
			ScaleCondition scaleCondition,
			int numberOfScaleCache) {
		this.cacheMemoryManager = cacheMemoryManager;
		this.cacheStatusMonitor = cacheStatusMonitor;
		this.scaleUpCalculator = scaleUpCalculator;
		this.scaleDownCalculator = scaleDownCalculator;
		this.numberOfScaleCache = numberOfScaleCache;
		this.scaleCondition = scaleCondition;
		this.running = true;
		this.scaleCallback = scaleResult -> {
			//TODO update memoryManager
		};
	}

	@Override
	public void notifyHeapStatus(HeapMonitorResult result) {
		Preconditions.checkState(running, "Scaling manager not running");

		Action action = decideAction(result);
		if (LOG.isDebugEnabled()) {
			LOG.debug("The triggering result of this monitoring is: {}", action);
		}

		if (action == Action.SCALE_DOWN) {
			doScaleDown();
		} else if (action == Action.SCALE_UP) {
			doScaleUp();
		}
	}

	public void shutdown() {
		this.running = false;
	}

	private Action decideAction(HeapMonitorResult monitorResult) {
		if (scaleCondition.shouldScaleDown(monitorResult)) {
			return Action.SCALE_DOWN;
		} else if (scaleCondition.shouldScaleUp(monitorResult)) {
			return Action.SCALE_UP;
		} else {
			return Action.NONE;
		}
	}

	/**
	 * Calculate the size of the scale up, and select Cache to trigger the scale.
	 */
	private void doScaleUp() {
		// 1.filter out caches whose current usage is less than 0.5 because there is no need to scale up.
		Map<Cache, CacheStatistic> cacheStatistics = cacheStatusMonitor.getCacheStatusStatistics().entrySet().stream()
			.filter(entry -> {
				CacheStatistic cacheStatistic = entry.getValue();
				return ((double) cacheStatistic.getUsedMemorySize().getBytes()) / cacheStatistic.getMaxMemorySize().getBytes() > 0.5;
			}).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
		// 2.normalize and calculate the weights
		List<Tuple2<Cache, Double>> computedWeights = normalizeAndComputeWeight(cacheStatistics, scaleUpCalculator);
		// 3.sort the weights
		sortCacheWeights(computedWeights);
		// 4.CacheMemoryManager calculates the number of blocks used for scale up.
		Tuple2<Integer, MemorySize> scaleUpSize = cacheMemoryManager.computeScaleUpSize();
		// 5.calculate the cache that needs to be scaled up and the corresponding size.
		Map<Cache, MemorySize> scaleCacheSize = computeScaleCacheSize(computedWeights, scaleUpSize.f0, scaleUpSize.f1, numberOfScaleCache);
		// TODO Call scaleUp after the cache implements the Scalable interface.
	}

	/**
	 * Calculate the size of the scale down, and select Cache to trigger the scale.
	 */
	private void doScaleDown() {
		// 1.filter out caches whose size is already 0, because they can no longer be scaled down.
		Map<Cache, CacheStatistic> cacheStatistics = cacheStatusMonitor.getCacheStatusStatistics().entrySet().stream()
			.filter(entry -> entry.getValue().getMaxMemorySize().getBytes() > 0)
			.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()));
		// 2.normalize and calculate the weights
		List<Tuple2<Cache, Double>> computedWeights = normalizeAndComputeWeight(cacheStatistics, scaleDownCalculator);
		// 3.sort the weights
		sortCacheWeights(computedWeights);
		// 4.CacheMemoryManager calculates the number of blocks used for scale down.
		Tuple2<Integer, MemorySize> scaleDownSize = cacheMemoryManager.computeScaleDownSize();
		// 5.calculate the cache that needs to be scaled down and the corresponding size.
		Map<Cache, MemorySize> scaleCacheSize = computeScaleCacheSize(computedWeights, scaleDownSize.f0, scaleDownSize.f1, numberOfScaleCache);

		// TODO Call scaleDown after the cache implements the Scalable interface.
	}

	/**
	 * Normalize the data and calculate the weights.
	 */
	public static List<Tuple2<Cache, Double>> normalizeAndComputeWeight(Map<Cache, CacheStatistic> cacheStatistics, WeightCalculator<CacheWeightMeta> weightCalculator) {
		long maxSize = 0L, minSize = Long.MAX_VALUE, maxLoadSuccessCount = 0L, minLoadSuccessCount = Long.MAX_VALUE;
		for (CacheStatistic statistic : cacheStatistics.values()) {
			long memorySize = statistic.getMaxMemorySize().getBytes();
			maxSize = Math.max(maxSize, memorySize);
			minSize = Math.min(minSize, memorySize);
			long numLoadSuccessCount = statistic.getLoadSuccessCount();
			maxLoadSuccessCount = Math.max(maxLoadSuccessCount, numLoadSuccessCount);
			minLoadSuccessCount = Math.min(minLoadSuccessCount, numLoadSuccessCount);
		}

		List<Tuple2<Cache, Double>> computedWeights = new ArrayList<>(cacheStatistics.size());
		for (Map.Entry<Cache, CacheStatistic> entry : cacheStatistics.entrySet()) {
			CacheStatistic cacheStatistic = entry.getValue();
			double normalizedSize = ((double) (cacheStatistic.getMaxMemorySize().getBytes() - minSize)) / maxSize;
			double normalizedLoadSuccessCount = ((double) (cacheStatistic.getLoadSuccessCount() - minLoadSuccessCount)) / maxLoadSuccessCount;
			computedWeights.add(Tuple2.of(entry.getKey(), weightCalculator.weight(new CacheWeightMeta(normalizedSize, normalizedLoadSuccessCount))));
		}
		return computedWeights;
	}

	/**
	 * Sort by weight.
	 */
	public static void sortCacheWeights(List<Tuple2<Cache, Double>> computedWeights) {
		Comparator<Tuple2<Cache, Double>> comparator = (o1, o2) -> {
			if (o1 == o2) {
				return 0;
			} else if (o1 == null) {
				return -1;
			} else if (o2 == null) {
				return 1;
			} else {
				return (o1.f1 > o2.f1) ? -1 : 1;
			}
		};
		computedWeights.sort(comparator);
	}

	/**
	 * Select the cache that needs to be scaled, and calculate the size of the scale.
	 */
	public static Map<Cache, MemorySize> computeScaleCacheSize(List<Tuple2<Cache, Double>> sortedWeights, int totalBlockCount, MemorySize blockSize, int numberOfScaleCache) {
		int scaleCacheCount = Math.min(totalBlockCount, Math.min(numberOfScaleCache, sortedWeights.size()));
		Map<Cache, MemorySize> scaleCacheSize = new HashMap<>(scaleCacheCount);
		if (scaleCacheCount > 0) {
			int avgBlockCount = totalBlockCount / scaleCacheCount;
			int remainBlockCount = totalBlockCount % scaleCacheCount;
			for (int i = 0; i < scaleCacheCount; i++) {
				int scaleBlockCount = remainBlockCount-- > 0 ? avgBlockCount + 1 : avgBlockCount;
				scaleCacheSize.put(sortedWeights.get(i).f0, blockSize.multiply(scaleBlockCount));
			}
		}
		return scaleCacheSize;
	}

	enum Action {
		NONE, SCALE_DOWN, SCALE_UP
	}
}
