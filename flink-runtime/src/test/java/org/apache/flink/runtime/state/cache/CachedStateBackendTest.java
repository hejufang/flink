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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.cache.internal.AbstractCachedKeyedState;
import org.apache.flink.runtime.state.cache.internal.CachedValueState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlValue;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
			} catch (ClassNotFoundException e) {
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

	@Test
	@SuppressWarnings("unchecked")
	@Override
	public void testValueStateRace() throws Exception {
		final AbstractKeyedStateBackend<Integer> backend =
			createKeyedBackend(IntSerializer.INSTANCE);
		final Integer namespace = 1;

		final ValueStateDescriptor<String> kvId =
			new ValueStateDescriptor<>("id", String.class);

		final TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		final TypeSerializer<Integer> namespaceSerializer =
			IntSerializer.INSTANCE;

		final ValueState<String> state = backend
			.getPartitionedState(namespace, IntSerializer.INSTANCE, kvId);

		// this is only available after the backend initialized the serializer
		final TypeSerializer<String> valueSerializer = kvId.getSerializer();

		@SuppressWarnings("unchecked")
		final InternalKvState<Integer, Integer, String> kvState = (InternalKvState<Integer, Integer, String>) state;

		/**
		 * 1) Test that ValueState#value() return the same value.
		 */

		// set some key and namespace
		final int key1 = 1;
		backend.setCurrentKey(key1);
		kvState.setCurrentNamespace(2);
		state.update("2");
		assertEquals("2", state.value());

		// the state should not have changed!
		assertEquals("2", state.value());

		// re-set values
		kvState.setCurrentNamespace(namespace);

		/**
		 * 2) Test two threads concurrently using ValueState#value() and
		 * KvState#getSerializedValue(byte[]).
		 */

		// some modifications to the state
		final int key2 = 10;
		backend.setCurrentKey(key2);
		assertNull(state.value());
		state.update("1");

		final CheckedThread getter = new CheckedThread("State getter") {
			@Override
			public void go() throws Exception {
				while (!isInterrupted()) {
					assertEquals("1", state.value());
				}
			}
		};

		final CheckedThread serializedGetter = new CheckedThread("Serialized state getter") {
			@Override
			public void go() throws Exception {
				while (!isInterrupted() && getter.isAlive()) {
					try {
						final String serializedValue =
							getSerializedValue(kvState, key2, keySerializer,
								namespace, namespaceSerializer,
								valueSerializer);
						Assert.fail("unsupported operation");
					} catch (UnsupportedOperationException ignore) {
						//ignore
					}
				}
			}
		};

		getter.start();
		serializedGetter.start();

		// run both threads for max 100ms
		Timer t = new Timer("stopper");
		t.schedule(new TimerTask() {
			@Override
			public void run() {
				getter.interrupt();
				serializedGetter.interrupt();
				this.cancel();
			}
		}, 100);

		// wait for both threads to finish
		try {
			// serializedGetter will finish if its assertion fails or if
			// getter is not alive any more
			serializedGetter.sync();
			// if serializedGetter crashed, getter will not know -> interrupt just in case
			getter.interrupt();
			getter.sync();
			t.cancel(); // if not executed yet
		} finally {
			// clean up
			backend.dispose();
		}
	}

	@Test
	@SuppressWarnings("unchecked,rawtypes")
	@Override
	public void testMapState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		AbstractKeyedStateBackend<String> backend = createKeyedBackend(StringSerializer.INSTANCE);

		MapStateDescriptor<Integer, String> kvId = new MapStateDescriptor<>("id", Integer.class, String.class);

		TypeSerializer<String> keySerializer = StringSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		MapState<Integer, String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<String, VoidNamespace, Map<Integer, String>> kvState = (InternalKvState<String, VoidNamespace, Map<Integer, String>>) state;

		// these are only available after the backend initialized the serializer
		TypeSerializer<Integer> userKeySerializer = kvId.getKeySerializer();
		TypeSerializer<String> userValueSerializer = kvId.getValueSerializer();

		// some modifications to the state
		backend.setCurrentKey("1");
		assertNull(state.get(1));
		state.put(1, "1");
		backend.setCurrentKey("2");
		assertNull(state.get(2));
		state.put(2, "2");

		// put entry with different userKeyOffset
		backend.setCurrentKey("11");
		state.put(11, "11");

		backend.setCurrentKey("1");
		assertTrue(state.contains(1));
		assertEquals("1", state.get(1));

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(
			backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);

		// make some more modifications
		backend.setCurrentKey("1");
		state.put(1, "101");
		backend.setCurrentKey("2");
		state.put(102, "102");
		backend.setCurrentKey("3");
		state.put(103, "103");
		state.putAll(new HashMap<Integer, String>() {{ put(1031, "1031"); put(1032, "1032"); }});

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(
			backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);

		// validate the original state
		backend.setCurrentKey("1");
		assertEquals("101", state.get(1));
		backend.setCurrentKey("2");
		assertEquals("102", state.get(102));
		backend.setCurrentKey("3");
		assertTrue(state.contains(103));
		assertEquals("103", state.get(103));

		List<Integer> keys = new ArrayList<>();
		for (Integer key : state.keys()) {
			keys.add(key);
		}
		List<Integer> expectedKeys = Arrays.asList(103, 1031, 1032);
		assertEquals(keys.size(), expectedKeys.size());
		keys.removeAll(expectedKeys);

		List<String> values = new ArrayList<>();
		for (String value : state.values()) {
			values.add(value);
		}
		List<String> expectedValues = Arrays.asList("103", "1031", "1032");
		assertEquals(values.size(), expectedValues.size());
		values.removeAll(expectedValues);

		// make some more modifications
		backend.setCurrentKey("1");
		state.clear();
		backend.setCurrentKey("2");
		state.remove(102);
		backend.setCurrentKey("3");
		final String updateSuffix = "_updated";
		Iterator<Map.Entry<Integer, String>> iterator = state.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, String> entry = iterator.next();
			if (entry.getValue().length() != 4) {
				iterator.remove();
			} else {
				entry.setValue(entry.getValue() + updateSuffix);
			}
		}

		// validate the state
		backend.setCurrentKey("1");
		backend.setCurrentKey("2");
		assertFalse(state.contains(102));
		backend.setCurrentKey("3");
		for (Map.Entry<Integer, String> entry : state.entries()) {
			assertEquals(4 + updateSuffix.length(), entry.getValue().length());
			assertTrue(entry.getValue().endsWith(updateSuffix));
		}

		backend.dispose();
		// restore the first snapshot and validate it
		backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot1);
		snapshot1.discardState();

		MapState<Integer, String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<String, VoidNamespace, Map<Integer, String>> restoredKvState1 = (InternalKvState<String, VoidNamespace, Map<Integer, String>>) restored1;

		backend.setCurrentKey("1");
		assertEquals("1", restored1.get(1));
		backend.setCurrentKey("2");
		assertEquals("2", restored1.get(2));

		backend.dispose();
		// restore the second snapshot and validate it
		backend = restoreKeyedBackend(StringSerializer.INSTANCE, snapshot2);
		snapshot2.discardState();

		@SuppressWarnings("unchecked")
		MapState<Integer, String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<String, VoidNamespace, Map<Integer, String>> restoredKvState2 = (InternalKvState<String, VoidNamespace, Map<Integer, String>>) restored2;

		backend.setCurrentKey("1");
		assertEquals("101", restored2.get(1));
		backend.setCurrentKey("2");
		assertEquals("102", restored2.get(102));
		backend.setCurrentKey("3");
		assertEquals("103", restored2.get(103));

		backend.dispose();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testValueState() throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		AbstractKeyedStateBackend<Integer> backend = createKeyedBackend(IntSerializer.INSTANCE);

		ValueStateDescriptor<String> kvId = new ValueStateDescriptor<>("id", String.class);

		TypeSerializer<Integer> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<VoidNamespace> namespaceSerializer = VoidNamespaceSerializer.INSTANCE;

		ValueState<String> state = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<Integer, VoidNamespace, String> kvState = (InternalKvState<Integer, VoidNamespace, String>) state;

		// this is only available after the backend initialized the serializer
		TypeSerializer<String> valueSerializer = kvId.getSerializer();

		// some modifications to the state
		backend.setCurrentKey(1);
		assertNull(state.value());
		state.update("1");
		backend.setCurrentKey(2);
		assertNull(state.value());
		state.update("2");
		backend.setCurrentKey(1);
		assertEquals("1", state.value());

		// draw a snapshot
		KeyedStateHandle snapshot1 = runSnapshot(
			backend.snapshot(682375462378L, 2, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);

		// make some more modifications
		backend.setCurrentKey(1);
		state.update("u1");
		backend.setCurrentKey(2);
		state.update("u2");
		backend.setCurrentKey(3);
		state.update("u3");

		// draw another snapshot
		KeyedStateHandle snapshot2 = runSnapshot(
			backend.snapshot(682375462379L, 4, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation()),
			sharedStateRegistry);

		// validate the original state
		backend.setCurrentKey(1);
		assertEquals("u1", state.value());
		backend.setCurrentKey(2);
		assertEquals("u2", state.value());
		backend.setCurrentKey(3);
		assertEquals("u3", state.value());

		backend.dispose();
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot1);

		snapshot1.discardState();

		ValueState<String> restored1 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<Integer, VoidNamespace, String> restoredKvState1 = (InternalKvState<Integer, VoidNamespace, String>) restored1;

		backend.setCurrentKey(1);
		assertEquals("1", restored1.value());
		backend.setCurrentKey(2);
		assertEquals("2", restored1.value());

		backend.dispose();
		backend = restoreKeyedBackend(IntSerializer.INSTANCE, snapshot2);

		snapshot2.discardState();

		ValueState<String> restored2 = backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, kvId);
		@SuppressWarnings("unchecked")
		InternalKvState<Integer, VoidNamespace, String> restoredKvState2 = (InternalKvState<Integer, VoidNamespace, String>) restored2;

		backend.setCurrentKey(1);
		assertEquals("u1", restored2.value());
		backend.setCurrentKey(2);
		assertEquals("u2", restored2.value());
		backend.setCurrentKey(3);
		assertEquals("u3", restored2.value());

		backend.dispose();
	}
}
