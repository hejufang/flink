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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;
import org.apache.flink.runtime.state.cache.scale.ScaleResult;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Basic test class for cache strategy.
 */
public abstract class CacheStrategyTestBase {
	protected static final String TEST_KEY = "test_key";
	protected static final String REPLACE_KEY = "replace_key";
	protected static final String TEST_VALUE = "test_value";
	protected static final String REPLACE_VALUE = "replace_value";

	protected final VoidNamespace namespace = VoidNamespace.INSTANCE;

	@Test
	public void testCachePut() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		// try to get a key that does not exist
		Assert.assertNull(cache.get(TEST_KEY, namespace));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(1, policyStats.getRequestCount());

		// set a key
		cache.put(TEST_KEY, namespace, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(3, policyStats.getRequestCount());

		// replace a key
		cache.put(TEST_KEY, namespace, REPLACE_VALUE);
		Assert.assertEquals(REPLACE_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(5, policyStats.getRequestCount());
	}

	@Test
	public void testCacheEvict() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		// set a key
		cache.put(TEST_KEY, namespace, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getRequestCount());

		// set a replace key and value
		cache.put(REPLACE_KEY, namespace, REPLACE_VALUE);
		Assert.assertEquals(REPLACE_VALUE, cache.get(REPLACE_KEY, namespace));
		Assert.assertEquals(1, policyStats.getEvictCount());
		Assert.assertEquals(1, policyStats.getSaveCount());
		Assert.assertEquals(4, policyStats.getRequestCount());

		// try to get a key that exist
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(2, policyStats.getEvictCount());
		Assert.assertEquals(2, policyStats.getSaveCount());
		Assert.assertEquals(1, policyStats.getLoadSuccessCount());
	}

	@Test
	public void testCacheDelete() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		cache.put(TEST_KEY, namespace, TEST_VALUE);
		cache.put(REPLACE_KEY, namespace, REPLACE_VALUE);
		Assert.assertEquals(0, policyStats.getDeleteCount());
		Assert.assertEquals(1, policyStats.getEvictCount());
		Assert.assertEquals(1, policyStats.getSaveCount());

		cache.delete(TEST_KEY, namespace);
		Assert.assertEquals(1, policyStats.getCacheSize());
		Assert.assertEquals(1, policyStats.getDeleteCount());
		Assert.assertEquals(REPLACE_VALUE, cache.get(REPLACE_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());

		cache.delete(REPLACE_KEY, namespace);
		Assert.assertEquals(0, policyStats.getCacheSize());
		Assert.assertEquals(2, policyStats.getDeleteCount());
		Assert.assertNull(cache.get(REPLACE_KEY, namespace));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
	}

	@Test
	public void testCacheGetWithScaleDown() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		// put a key
		cache.put(TEST_KEY, namespace, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getRequestCount());
		Assert.assertEquals(1, policyStats.getHitCount());
		Assert.assertEquals(1, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// scale down to 0
		AtomicReference<ScaleResult<MemorySize>> scaleResult = new AtomicReference<>();
		cache.scaleDown(new MemorySize(1L), MemorySize.ZERO, result -> scaleResult.set(result));
		Assert.assertEquals(0, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// request
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getEstimatedSize());
		Assert.assertEquals(3, policyStats.getRequestCount());
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getHitCount());

		// request again
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(4, policyStats.getRequestCount());
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(1, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getHitCount());
	}

	@Test
	public void testCachePutWithScaleDown() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		// put a key
		cache.put(TEST_KEY, namespace, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getRequestCount());
		Assert.assertEquals(1, policyStats.getHitCount());
		Assert.assertEquals(1, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// scale down to 0
		AtomicReference<ScaleResult<MemorySize>> scaleResult = new AtomicReference<>();
		cache.scaleDown(new MemorySize(1L), MemorySize.ZERO, result -> scaleResult.set(result));
		Assert.assertEquals(0, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// put again
		cache.put(TEST_KEY, namespace, REPLACE_VALUE);
		Assert.assertEquals(0, policyStats.getEstimatedSize());
		Assert.assertEquals(3, policyStats.getRequestCount());
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(1, policyStats.getHitCount());
		Assert.assertEquals(REPLACE_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(1, policyStats.getLoadSuccessCount());
		Assert.assertEquals(1, policyStats.getHitCount());
	}

	@Test
	public void testCacheDeleteWithScaleDown() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = configureCache(cache);

		// put a key
		cache.put(TEST_KEY, namespace, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getRequestCount());
		Assert.assertEquals(1, policyStats.getHitCount());
		Assert.assertEquals(1, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// scale down to 0
		AtomicReference<ScaleResult<MemorySize>> scaleResult = new AtomicReference<>();
		cache.scaleDown(new MemorySize(1L), MemorySize.ZERO, result -> scaleResult.set(result));
		Assert.assertEquals(0, policyStats.getMaxMemorySize().getBytes());
		Assert.assertEquals(1, policyStats.getEstimatedSize());

		// delete
		cache.delete(TEST_KEY, namespace);
		Assert.assertEquals(0, policyStats.getEstimatedSize());
		Assert.assertEquals(3, policyStats.getRequestCount());
		Assert.assertNull(cache.get(TEST_KEY, namespace));
	}

	@Test
	public abstract void testStrategy() throws Exception;

	protected PolicyStats configureCache(Cache<String, VoidNamespace, String, Void, String> cache) throws Exception {
		return configureCache(cache, null);
	}

	protected PolicyStats configureCache(Cache<String, VoidNamespace, String, Void, String> cache, PolicyStats policyStats) throws Exception {
		if (policyStats == null) {
			policyStats = new PolicyStats(cache);
			policyStats.recordMaxCacheMemorySize(new MemorySize(1));
		}
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>());
		return policyStats;
	}

	protected abstract Cache<String, VoidNamespace, String, Void, String> createCache() throws Exception;

	private static class MockDataSynchronizer<K, V> implements DataSynchronizer<K, V> {
		private final Map<K, V> map;

		public MockDataSynchronizer() {
			this.map = new HashMap<>();
		}

		@Override
		public void saveState(K key, V value) throws Exception {
			Preconditions.checkNotNull(value, "state can not be null");
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
