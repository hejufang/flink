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
import org.apache.flink.runtime.state.cache.LRUStrategy;
import org.apache.flink.runtime.state.cache.StateStore;
import org.apache.flink.runtime.state.cache.monitor.CacheStatistic;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test that {@link ScalingManager} calculates and selects the cache weight correctly.
 */
@SuppressWarnings("rawtypes")
public class ScalingManagerTest {

	private static final double WEIGHT_RETAINED_SIZE = 0.7;
	private static final double WEIGHT_LOAD_SUCCESS_COUNT = -0.3;
	private static final MemorySize BLOCK_SIZE = MemorySize.parse("16m");

	@Test
	public void testWeightCompute() {
		WeightCalculator<CacheWeightMeta> weightCalculator = new DefaultCacheWeightCalculator(WEIGHT_RETAINED_SIZE, WEIGHT_LOAD_SUCCESS_COUNT);
		Map<Cache, CacheStatistic> cacheStatistics = new LinkedHashMap<>(3);

		Cache firstCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		CacheStatistic firstCacheStatistic = createCacheStatistic(MemorySize.parse("64m"), 1024);
		cacheStatistics.put(firstCache, firstCacheStatistic);

		Cache secondCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		CacheStatistic secondStatistic = createCacheStatistic(MemorySize.parse("32m"), 2048);
		cacheStatistics.put(secondCache, secondStatistic);

		Cache thirdCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		CacheStatistic thirdStatistic = createCacheStatistic(MemorySize.parse("16m"), 256);
		cacheStatistics.put(thirdCache, thirdStatistic);

		// calculations of the double type will lose precision, so arithmetic expressions are used here.
		Tuple2[] expectedWeights = new Tuple2[] {
			Tuple2.of(firstCache, WEIGHT_RETAINED_SIZE * 0.75 + WEIGHT_LOAD_SUCCESS_COUNT * 0.375),
			Tuple2.of(secondCache, WEIGHT_RETAINED_SIZE * 0.25 + WEIGHT_LOAD_SUCCESS_COUNT * 0.875),
			Tuple2.of(thirdCache, 0.0)};
		List<Tuple2<Cache, Double>> computedWeights = ScalingManager.normalizeAndComputeWeight(cacheStatistics, weightCalculator);
		Assert.assertArrayEquals(expectedWeights, computedWeights.toArray());
	}

	@Test
	public void testSortCache() {
		List<Tuple2<Cache, Double>> computedWeights = new ArrayList<>(3);
		List<Tuple2<Cache, Double>> expectedWeights = new ArrayList<>(3);

		for (int i = 0; i < 3; i++) {
			Cache cache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
			computedWeights.add(Tuple2.of(cache, (double) i / 3));
			expectedWeights.add(Tuple2.of(cache, (double) i / 3));
		}
		Collections.reverse(expectedWeights);
		ScalingManager.sortCacheWeights(computedWeights);
		Assert.assertArrayEquals(expectedWeights.toArray(), computedWeights.toArray());
	}

	@Test
	public void testComputeScaleCacheSize() {
		List<Tuple2<Cache, Double>> sortedWeights = new ArrayList<>(3);

		Cache firstCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		sortedWeights.add(Tuple2.of(firstCache, 0.8));

		Cache secondCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		sortedWeights.add(Tuple2.of(secondCache, 0.6));

		Cache thirdCache = new Cache(new LRUStrategy<>(3L), new StateStore.SimpleStateStore());
		sortedWeights.add(Tuple2.of(thirdCache, 0.4));

		// when the number of blocks is greater than the number of caches that need to be scaled
		int totalBlockCount = 4, numberOfScaleCache = 2;
		Map<Cache, MemorySize> scaleCacheSize = ScalingManager.computeScaleCacheSize(sortedWeights, totalBlockCount, BLOCK_SIZE, numberOfScaleCache);
		Assert.assertEquals(2, scaleCacheSize.size());
		Assert.assertTrue(scaleCacheSize.containsKey(firstCache));
		Assert.assertTrue(scaleCacheSize.containsKey(secondCache));
		Assert.assertEquals(scaleCacheSize.get(firstCache), BLOCK_SIZE.multiply(2));
		Assert.assertEquals(scaleCacheSize.get(secondCache), BLOCK_SIZE.multiply(2));

		// When the number of blocks is greater than the number of caches that need to be scaled, but not evenly divisible
		totalBlockCount = 3;
		scaleCacheSize = ScalingManager.computeScaleCacheSize(sortedWeights, totalBlockCount, BLOCK_SIZE, numberOfScaleCache);
		Assert.assertEquals(2, scaleCacheSize.size());
		Assert.assertTrue(scaleCacheSize.containsKey(firstCache));
		Assert.assertTrue(scaleCacheSize.containsKey(secondCache));
		Assert.assertEquals(scaleCacheSize.get(firstCache), BLOCK_SIZE.multiply(2));
		Assert.assertEquals(scaleCacheSize.get(secondCache), BLOCK_SIZE);

		// when the number of blocks is less than the number of caches that need to be scaled
		totalBlockCount = 1;
		scaleCacheSize = ScalingManager.computeScaleCacheSize(sortedWeights, totalBlockCount, BLOCK_SIZE, numberOfScaleCache);
		Assert.assertEquals(1, scaleCacheSize.size());
		Assert.assertTrue(scaleCacheSize.containsKey(firstCache));
		Assert.assertEquals(scaleCacheSize.get(firstCache), BLOCK_SIZE);

		// when the number of caches is less than the number of scales required
		totalBlockCount = 4;
		numberOfScaleCache = 4;
		scaleCacheSize = ScalingManager.computeScaleCacheSize(sortedWeights, totalBlockCount, BLOCK_SIZE, numberOfScaleCache);
		Assert.assertEquals(3, scaleCacheSize.size());
		Assert.assertTrue(scaleCacheSize.containsKey(firstCache));
		Assert.assertTrue(scaleCacheSize.containsKey(secondCache));
		Assert.assertTrue(scaleCacheSize.containsKey(thirdCache));
		Assert.assertEquals(scaleCacheSize.get(firstCache), BLOCK_SIZE.multiply(2));
		Assert.assertEquals(scaleCacheSize.get(secondCache), BLOCK_SIZE);
		Assert.assertEquals(scaleCacheSize.get(thirdCache), BLOCK_SIZE);
	}

	private CacheStatistic createCacheStatistic(MemorySize maxMemorySize, long loadSuccessCount) {
		return new CacheStatistic(
			maxMemorySize,
			maxMemorySize,
			1024,
			loadSuccessCount * 10,
			loadSuccessCount * 8,
			loadSuccessCount * 2,
			loadSuccessCount,
			loadSuccessCount,
			loadSuccessCount,
			0,
			0,
			0,
			MemorySize.ZERO,
			MemorySize.ZERO);
	}

}
