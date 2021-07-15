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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.cache.memory.MemoryEstimator;
import org.apache.flink.runtime.state.cache.sync.DataSynchronizer;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Test that the LRU cache is operating correctly.
 */
public class LRUCacheTest {
	private static final String TEST_KEY = "test_key";
	private static final String REPLACE_KEY = "replace_key";
	private static final String TEST_VALUE = "test_value";
	private static final String REPLACE_VALUE = "replace_value";

	@Test
	public void testCachePut() throws Exception {
		LRUCache<String, String> cache = createLRUCache();
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(1));
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>(), Objects::nonNull);

		// try to get a key that does not exist
		Assert.assertNull(cache.get(TEST_KEY));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(1, policyStats.getRequestCount());

		// set a key
		cache.put(TEST_KEY, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(3, policyStats.getRequestCount());

		// replace a key
		cache.put(TEST_KEY, REPLACE_VALUE);
		Assert.assertEquals(REPLACE_VALUE, cache.get(TEST_KEY));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(5, policyStats.getRequestCount());
	}

	@Test
	public void testCacheEvict() throws Exception {
		LRUCache<String, String> cache = createLRUCache();
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(1));
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>(), Objects::nonNull);

		// set a key
		cache.put(TEST_KEY, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
		Assert.assertEquals(2, policyStats.getRequestCount());

		// set a replace key and value
		cache.put(REPLACE_KEY, REPLACE_VALUE);
		Assert.assertEquals(REPLACE_VALUE, cache.get(REPLACE_KEY));
		Assert.assertEquals(1, policyStats.getEvictionCount());
		Assert.assertEquals(1, policyStats.getSaveCount());
		Assert.assertEquals(4, policyStats.getRequestCount());

		// try to get a key that exist
		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY));
		Assert.assertEquals(2, policyStats.getEvictionCount());
		Assert.assertEquals(2, policyStats.getSaveCount());
		Assert.assertEquals(1, policyStats.getLoadSuccessCount());
	}

	@Test
	public void testCacheDelete() throws Exception {
		LRUCache<String, String> cache = createLRUCache();
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(1));
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>(), Objects::nonNull);

		cache.put(TEST_KEY, TEST_VALUE);
		cache.put(REPLACE_KEY, REPLACE_VALUE);
		Assert.assertEquals(0, policyStats.getDeleteCount());
		Assert.assertEquals(1, policyStats.getEvictionCount());
		Assert.assertEquals(1, policyStats.getSaveCount());

		cache.delete(TEST_KEY);
		Assert.assertEquals(1, policyStats.getCacheSize());
		Assert.assertEquals(1, policyStats.getDeleteCount());
		Assert.assertEquals(REPLACE_VALUE, cache.get(REPLACE_KEY));
		Assert.assertEquals(0, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());

		cache.delete(REPLACE_KEY);
		Assert.assertEquals(0, policyStats.getCacheSize());
		Assert.assertEquals(2, policyStats.getDeleteCount());
		Assert.assertNull(cache.get(REPLACE_KEY));
		Assert.assertEquals(1, policyStats.getMissCount());
		Assert.assertEquals(0, policyStats.getLoadSuccessCount());
	}

	@Test
	public void testLRUStrategy() throws Exception {
		LRUCache<String, String> cache = createLRUCache();
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(2));
		cache.configure(new DefaultEventListener<>(policyStats, new MockMemoryEstimator<>()), new MockDataSynchronizer<>(), Objects::nonNull);

		cache.put(TEST_KEY, TEST_VALUE);
		cache.put(REPLACE_KEY, REPLACE_VALUE);
		Assert.assertEquals(cache.get(TEST_KEY), TEST_VALUE);
		Assert.assertEquals(2, policyStats.getCacheSize());

		cache.put("123", "321");
		Assert.assertEquals(2, policyStats.getCacheSize());
		Assert.assertEquals(1, policyStats.getEvictionCount());
		Assert.assertEquals(1, policyStats.getSaveCount());

		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY));
		Assert.assertEquals(0, policyStats.getMissCount());
	}

	private <K, V> LRUCache<K, V> createLRUCache() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(CacheConfigurableOptions.CACHE_STRATEGY, "LRU");
		CacheFactory cacheFactory = CacheFactory.createCacheFactory(CacheConfiguration.fromConfiguration(configuration));
		Cache<K, V> cache = cacheFactory.createCache();
		return (LRUCache<K, V>) cache;
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
