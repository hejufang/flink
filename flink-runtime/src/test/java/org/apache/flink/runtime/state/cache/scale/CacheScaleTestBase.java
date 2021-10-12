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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.cache.Cache;
import org.apache.flink.runtime.state.cache.CacheStrategy;
import org.apache.flink.runtime.state.cache.DefaultEventListener;
import org.apache.flink.runtime.state.cache.PolicyStats;
import org.apache.flink.runtime.state.cache.StateStore;
import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test the scale of the cache.
 */
public abstract class CacheScaleTestBase {
	private AtomicReference<ScaleResult> result = new AtomicReference<>();
	ScaleCallback<MemorySize> callback = result::set;

	@Test
	public void testCacheScaleUp() throws Exception {
		Tuple2<Cache<String, VoidNamespace, String, Void, String>, PolicyStats> cacheWithPolicyStats = createAndConfigureCache();
		Cache<String, VoidNamespace, String, Void, String> cache = cacheWithPolicyStats.f0;
		PolicyStats policyStats = cacheWithPolicyStats.f1;

		// test the cache size is sufficient for scale up
		MemorySize scaleUpSize = new MemorySize(10);
		cache.scaleUp(scaleUpSize, new MemorySize(25L), callback);
		Assert.assertTrue(result.get().isSuccess());
		Assert.assertEquals(ScalingManager.Action.SCALE_UP, result.get().getAction());
		Assert.assertEquals(scaleUpSize, result.get().getRecommendedSize());
		Assert.assertEquals(scaleUpSize, result.get().getActualScaleSize());
		Assert.assertEquals(new MemorySize(20L), policyStats.getMaxMemorySize());

		visit(cache);
		Assert.assertEquals(20L, policyStats.getEstimatedSize());

		// test the cache size is not sufficient for scale up
		cache.scaleUp(scaleUpSize, new MemorySize(25L), callback);
		Assert.assertTrue(result.get().isSuccess());
		Assert.assertEquals(ScalingManager.Action.SCALE_UP, result.get().getAction());
		Assert.assertEquals(scaleUpSize, result.get().getRecommendedSize());
		Assert.assertEquals(new MemorySize(5L), result.get().getActualScaleSize());
		Assert.assertEquals(new MemorySize(25L), policyStats.getMaxMemorySize());

		visit(cache);
		Assert.assertEquals(25L, policyStats.getEstimatedSize());
	}

	@Test
	public void testCacheScaleDown() throws Exception {
		Tuple2<Cache<String, VoidNamespace, String, Void, String>, PolicyStats> cacheWithPolicyStats = createAndConfigureCache();
		Cache<String, VoidNamespace, String, Void, String> cache = cacheWithPolicyStats.f0;
		PolicyStats policyStats = cacheWithPolicyStats.f1;

		// test the cache size is sufficient for scale down
		MemorySize scaleDownSize = new MemorySize(1L);
		cache.scaleDown(scaleDownSize, MemorySize.ZERO, callback);
		Assert.assertTrue(result.get().isSuccess());
		Assert.assertEquals(ScalingManager.Action.SCALE_DOWN, result.get().getAction());
		Assert.assertEquals(scaleDownSize, result.get().getRecommendedSize());
		Assert.assertEquals(scaleDownSize, result.get().getActualScaleSize());
		Assert.assertEquals(new MemorySize(9L), policyStats.getMaxMemorySize());

		visit(cache);
		Assert.assertEquals(9L, policyStats.getEstimatedSize());

		// test the cache is not large enough for scale down
		scaleDownSize = new MemorySize(10L);
		cache.scaleDown(scaleDownSize, MemorySize.ZERO, callback);
		Assert.assertTrue(result.get().isSuccess());
		Assert.assertEquals(ScalingManager.Action.SCALE_DOWN, result.get().getAction());
		Assert.assertEquals(scaleDownSize, result.get().getRecommendedSize());
		Assert.assertEquals(new MemorySize(9L), result.get().getActualScaleSize());
		Assert.assertEquals(new MemorySize(0L), policyStats.getMaxMemorySize());

		visit(cache);
		Assert.assertEquals(0L, policyStats.getEstimatedSize());
	}

	private Tuple2<Cache<String, VoidNamespace, String, Void, String>, PolicyStats> createAndConfigureCache() {
		CacheStrategy<Tuple3<String, VoidNamespace, Void>, Cache.DirtyReference> strategy = createCacheStrategy();
		StateStore<String, VoidNamespace, String, Void, String> stateStore = new StateStore.SimpleStateStore<>();
		Cache<String, VoidNamespace, String, Void, String> cache = new Cache<>(strategy, stateStore);
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(10L));
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>());
		return Tuple2.of(cache, policyStats);
	}

	protected abstract <K, V> CacheStrategy<K, V> createCacheStrategy();

	protected void visit(Cache<String, VoidNamespace, String, Void, String> cache) throws Exception {
		for (int i = 0; i < 100; i++) {
			cache.put("test-key-" + i, VoidNamespace.INSTANCE, "test-value-" + i);
		}
	}

	private static class MockDataSynchronizer<K, V> implements DataSynchronizer<K, V> {
		private final Map<K, V> map;

		public MockDataSynchronizer() {
			this.map = new HashMap<>();
		}

		@Override
		public void saveState(K key, V value) throws Exception {
			map.put(key, value);
		}

		@Nullable
		@Override
		public V loadState(K key) throws Exception {
			return map.get(key);
		}

		@Override
		public void removeState(K key) throws Exception {
			map.remove(key);
		}

		@Override
		public void flush(boolean force) throws Exception {
			//do nothing
		}
	}

	private static class MockMemoryEstimator<K, V> implements MemoryEstimator<K, V> {
		@Override
		public void updateEstimatedSize(K key, V value) throws IOException {
			//do nothing
		}

		@Override
		public long getEstimatedSize() {
			return 1;
		}
	}
}
