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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

/**
 * Tests for the {@link org.apache.flink.runtime.state.cache.CachedStateBackend}.
 */
public class CachedStateBackendTest extends StateBackendTestBase<CachedStateBackend> {

	@Override
	protected CachedStateBackend getStateBackend() throws Exception {
		CacheManager cacheManager = new MockCacheManager(getCacheConfiguration());
		StateBackend delegateStateBackend = new MemoryStateBackend(true);
		return new CachedStateBackend(cacheManager, delegateStateBackend, getCacheConfiguration());
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return false;
	}

	private CacheConfiguration getCacheConfiguration() {
		return CacheConfiguration.fromConfiguration(new Configuration());
	}

	private static class MockCacheManager implements CacheManager {
		private final CacheConfiguration configuration;

		public MockCacheManager(CacheConfiguration configuration) {
			this.configuration = configuration;
		}

		@Override
		public PolicyStats registerCache(TaskInfo taskInfo, String name, Cache cache, MemorySize initialSize) throws Exception {
			PolicyStats policyStats = new PolicyStats(cache);
			policyStats.recordMaxCacheMemorySize(configuration.getCacheInitialSize());
			return policyStats;
		}

		@Override
		public void unregisterCache(TaskInfo taskInfo, String name, Cache cache) {
			// do nothing
		}

		@Override
		public void shutdown() throws Exception {
			// do nothing
		}
	}
}
