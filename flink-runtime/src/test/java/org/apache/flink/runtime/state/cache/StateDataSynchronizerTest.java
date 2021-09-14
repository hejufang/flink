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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.cache.sync.MapStateSynchronizer;
import org.apache.flink.runtime.state.cache.sync.StateSynchronizerFactory;
import org.apache.flink.runtime.state.cache.sync.ValueStateSynchronizer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapStateBackendTestBase;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.mock;

/**
 * Test for StateDataSynchronizer.
 */
public class StateDataSynchronizerTest {
	private static final String NO_USE_KEY = "no_use_key";
	private static final String TEST_KEY = "test_key";
	private static final String TEST_VALUE = "test_value";
	private static final String TEST_USER_KEY = "test_user_key";
	private static final String TEST_USER_VALUE = "test_user_value";

	@Test
	public void testValueStateDataSynchronizer() throws Exception {
		KeyedStateBackend<String> keyedStateBackend = createKeyedBackend(Collections.EMPTY_LIST);
		ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("value-state", String.class);
		ValueState<String> valueState = keyedStateBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
		ValueStateSynchronizer<String, VoidNamespace, String> synchronizer = StateSynchronizerFactory.createStateSynchronizer(keyedStateBackend, (InternalKvState<String, VoidNamespace, String>) valueState, descriptor.getType());

		Tuple3<String, VoidNamespace, Void> testKey = Tuple3.of(TEST_KEY, VoidNamespace.INSTANCE, null);

		// DataSynchronizer is only responsible for saving data,
		// so no matter whether it is dirty data or not, it will be written.
		synchronizer.saveState(testKey, TEST_VALUE);
		Assert.assertEquals(TEST_VALUE, synchronizer.loadState(testKey));
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertEquals(TEST_VALUE, valueState.value());

		// if it is dirty data, write to State.
		keyedStateBackend.setCurrentKey(NO_USE_KEY);
		synchronizer.saveState(testKey, TEST_VALUE);
		String loadData = synchronizer.loadState(testKey);
		Assert.assertNotNull(loadData);
		Assert.assertEquals(loadData, TEST_VALUE);
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertEquals(valueState.value(), TEST_VALUE);

		// regardless of whether it is dirty data, the state delete operation is called.
		synchronizer.removeState(testKey);
		loadData = synchronizer.loadState(testKey);
		Assert.assertNull(loadData);
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertNull(valueState.value());
	}

	@Test
	public void testMapStateDataSynchronizer() throws Exception {
		KeyedStateBackend<String> keyedStateBackend = createKeyedBackend(Collections.EMPTY_LIST);
		MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
		MapState<String, String> mapState = keyedStateBackend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);
		MapStateSynchronizer<String, VoidNamespace, String, String> synchronizer = StateSynchronizerFactory.createStateSynchronizer(keyedStateBackend, (InternalKvState<String, VoidNamespace, ?>) mapState, descriptor.getType());

		Tuple3<String, VoidNamespace, String> testKey = Tuple3.of(TEST_KEY, VoidNamespace.INSTANCE, TEST_USER_KEY);

		// DataSynchronizer is only responsible for saving data,
		// so no matter whether it is dirty data or not, it will be written.
		synchronizer.saveState(testKey, TEST_USER_VALUE);
		Assert.assertEquals(TEST_USER_VALUE, synchronizer.loadState(testKey));
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertEquals(TEST_USER_VALUE, mapState.get(TEST_USER_KEY));

		// if it is dirty data, write to State.
		keyedStateBackend.setCurrentKey(NO_USE_KEY);
		synchronizer.saveState(testKey, TEST_VALUE);
		String loadData = synchronizer.loadState(testKey);
		Assert.assertNotNull(loadData);
		Assert.assertEquals(loadData, TEST_VALUE);
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertEquals(mapState.get(TEST_USER_KEY), TEST_VALUE);

		// regardless of whether it is dirty data, the state delete operation is called.
		synchronizer.removeState(testKey);
		loadData = synchronizer.loadState(testKey);
		Assert.assertNull(loadData);
		keyedStateBackend.setCurrentKey(TEST_KEY);
		Assert.assertNull(mapState.get(TEST_USER_KEY));
	}

	public HeapKeyedStateBackend<String> createKeyedBackend(Collection<KeyedStateHandle> stateHandles) throws Exception {
		return createKeyedBackend(StringSerializer.INSTANCE, stateHandles);
	}

	public <K> HeapKeyedStateBackend<K> createKeyedBackend(
		TypeSerializer<K> keySerializer,
		Collection<KeyedStateHandle> stateHandles) throws Exception {
		final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
		final int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
		ExecutionConfig executionConfig = new ExecutionConfig();

		return new HeapKeyedStateBackendBuilder<>(
			mock(TaskKvStateRegistry.class),
			keySerializer,
			HeapStateBackendTestBase.class.getClassLoader(),
			numKeyGroups,
			keyGroupRange,
			executionConfig,
			TtlTimeProvider.DEFAULT,
			stateHandles,
			AbstractStateBackend.getCompressionDecorator(executionConfig),
			TestLocalRecoveryConfig.disabled(),
			new HeapPriorityQueueSetFactory(keyGroupRange, numKeyGroups, 128),
			false,
			new CloseableRegistry()).build();
	}
}
