/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * {@link OperatorSubtaskStateMeta} test.
 */
public class OperatorSubtaskStateMetaTest {

	@Test
	public void testMergeEmpty(){
		ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("test-state", LongSerializer.INSTANCE);
		RegisteredOperatorStateMeta.OperatorStateMetaData operatorStateMetaData = new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, "test-state", stateDescr.getType(), stateDescr);
		HashMap map = new HashMap();
		map.put("test-state", operatorStateMetaData);
		RegisteredOperatorStateMeta origin = new RegisteredOperatorStateMeta(BackendType.OPERATOR_STATE_BACKEND, map);
		RegisteredOperatorStateMeta empty = new RegisteredOperatorStateMeta(BackendType.OPERATOR_STATE_BACKEND, new HashMap<>());

		RegisteredOperatorStateMeta afterMerge = (RegisteredOperatorStateMeta) origin.merge(empty);
		assertEquals(1, afterMerge.getStateMetaData().size());
	}

	@Test
	public void testMergeDifferentState(){

		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state1", StringSerializer.INSTANCE, statefulSerializer);
		MapStateDescriptor mapStateDescriptor1 = new MapStateDescriptor("test-state2", String.class, String.class);

		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state1", mapStateDescriptor.getType(), mapStateDescriptor);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData1 = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor1.getType(), mapStateDescriptor1);

		HashMap map1 = new HashMap();
		map1.put(keyedStateMetaData.getName(), keyedStateMetaData);
		HashMap map = new HashMap();
		map.put(keyedStateMetaData.getName(), keyedStateMetaData);
		map1.put(keyedStateMetaData1.getName(), keyedStateMetaData1);

		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.HEAP_STATE_BACKEND, map);
		RegisteredKeyedStateMeta registeredKeyedStateMeta1 = new RegisteredKeyedStateMeta(keySerialzier, BackendType.HEAP_STATE_BACKEND, map1);

		registeredKeyedStateMeta.merge(registeredKeyedStateMeta1);
		assertEquals(2, registeredKeyedStateMeta.getStateMetaData().size());
		assertEquals(keyedStateMetaData, registeredKeyedStateMeta.getStateMetaData().get("test-state1"));
		assertEquals(keyedStateMetaData1, registeredKeyedStateMeta.getStateMetaData().get("test-state2"));
	}

	@Test
	public void testMergeStateWithDifferentBackendType(){

		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state1", StringSerializer.INSTANCE, statefulSerializer);
		MapStateDescriptor mapStateDescriptor1 = new MapStateDescriptor("test-state2", String.class, String.class);

		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state1", mapStateDescriptor.getType(), mapStateDescriptor);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData1 = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor1.getType(), mapStateDescriptor1);

		HashMap map1 = new HashMap();
		map1.put(keyedStateMetaData.getName(), keyedStateMetaData);
		HashMap map = new HashMap();
		map.put(keyedStateMetaData.getName(), keyedStateMetaData);
		map1.put(keyedStateMetaData1.getName(), keyedStateMetaData1);

		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.HEAP_STATE_BACKEND, map);
		RegisteredKeyedStateMeta registeredKeyedStateMeta1 = new RegisteredKeyedStateMeta(keySerialzier, BackendType.FULL_ROCKSDB_STATE_BACKEND, map1);

		try {
			registeredKeyedStateMeta.merge(registeredKeyedStateMeta1);
		} catch (Exception e) {
			assertThat(e.getMessage(), Matchers.containsString("The merge operation is allowed only if the BackendType is the same"));
		}
	}
}
