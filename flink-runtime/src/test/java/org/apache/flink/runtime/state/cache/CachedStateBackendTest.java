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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.cache.internal.AbstractCachedKeyedState;
import org.apache.flink.runtime.state.cache.internal.CachedValueState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlValue;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * Tests for the {@link org.apache.flink.runtime.state.cache.CachedStateBackend}.
 */
@Ignore
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

	@Test
	@SuppressWarnings("unchecked")
	public void testTtlStateNotCreatedTwice() throws Exception {
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);
		try {
			ValueStateDescriptor<Long> kvId = new ValueStateDescriptor<>("id", Long.class);
			kvId.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(1)).build());

			ValueState<Long> state = backend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, kvId);
			try {
				Class<?> ttlValueState = Class.forName("org.apache.flink.runtime.state.ttl.TtlValueState");
				Assert.assertTrue(ttlValueState.isInstance(state));

				Class<?> abstractTtlDecorator = Class.forName("org.apache.flink.runtime.state.ttl.AbstractTtlDecorator");
				Field originalState = abstractTtlDecorator.getDeclaredField("original");
				originalState.setAccessible(true);
				ValueState<TtlValue<Long>> originValueState = (ValueState<TtlValue<Long>>) originalState.get(state);
				Assert.assertTrue(originValueState instanceof CachedValueState);

				Field delegateState = AbstractCachedKeyedState.class.getDeclaredField("delegateState");
				delegateState.setAccessible(true);
				ValueState<TtlValue<Long>> delegateValueState = (ValueState<TtlValue<Long>>) delegateState.get(originValueState);
				Class<?> heapValueState = Class.forName("org.apache.flink.runtime.state.heap.HeapValueState");
				Assert.assertTrue(heapValueState.isInstance(delegateValueState));
			} catch (ClassNotFoundException e){
				Assert.fail("Unexpected exception: " + e.getMessage());
			}
		} finally {
			backend.close();
			backend.dispose();
		}
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
