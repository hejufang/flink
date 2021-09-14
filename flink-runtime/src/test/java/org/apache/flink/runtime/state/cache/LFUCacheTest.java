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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Assert;

/**
 * Test that the LRU cache is operating correctly.
 */
public class LFUCacheTest extends CacheStrategyTestBase {
	@Override
	public void testStrategy() throws Exception {
		Cache<String, VoidNamespace, String, Void, String> cache = createCache();
		PolicyStats policyStats = new PolicyStats(cache);
		policyStats.recordMaxCacheMemorySize(new MemorySize(2L));
		configureCache(cache, policyStats);

		cache.put(TEST_KEY, namespace, TEST_VALUE);
		cache.put(REPLACE_KEY, namespace, REPLACE_VALUE);
		Assert.assertEquals(cache.get(TEST_KEY, namespace), TEST_VALUE);
		Assert.assertEquals(2, policyStats.getCacheSize());

		for (int i = 0; i < 10; i++) {
			cache.get(TEST_KEY, namespace);
		}
		cache.get(REPLACE_KEY, namespace);

		cache.put("123", namespace, "321");
		Assert.assertEquals(2, policyStats.getCacheSize());
		Assert.assertEquals(1, policyStats.getEvictCount());
		Assert.assertEquals(1, policyStats.getSaveCount());

		Assert.assertEquals(TEST_VALUE, cache.get(TEST_KEY, namespace));
		Assert.assertEquals(0, policyStats.getMissCount());
	}

	@Override
	protected Cache<String, VoidNamespace, String, Void, String> createCache() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(CacheConfigurableOptions.CACHE_STRATEGY, "LFU");
		CacheStrategyFactory cacheStrategyFactory = CacheStrategyFactory.createCacheStrategyFactory(CacheConfiguration.fromConfiguration(configuration));
		CacheStrategy<Tuple3<String, VoidNamespace, Void>, Cache.DirtyReference> cacheStrategy = cacheStrategyFactory.createCacheStrategy();
		return new Cache<>(cacheStrategy, new StateStore.SimpleStateStore<>());
	}
}
