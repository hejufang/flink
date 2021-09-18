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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * {@link OperatorStateMeta } test.
 */
public class OperatorStateMetaTest {
	@Test
	public void testMergeEmptyStateMeta() {
		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10L,10L), "name", "uid");
		operatorStateMeta.mergeSubtaskStateMeta(OperatorSubtaskStateMeta.empty());
		Assert.assertNull(operatorStateMeta.getKeyedStateMeta());
		Assert.assertNull(operatorStateMeta.getOperatorStateMeta());
	}

	@Test
	public void testMergeOperatorState() {
		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10L,10L), "name", "uid");
		RegisteredOperatorStateMeta registeredOperatorStateMeta = createRegisteredOperatorStateMeta();
		OperatorSubtaskStateMeta operatorSubtaskStateMeta = OperatorSubtaskStateMeta.of(registeredOperatorStateMeta, null);
		operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta);
		Assert.assertEquals(null, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(registeredOperatorStateMeta, operatorStateMeta.getOperatorStateMeta());
	}

	@Test
	public void testMergeOperatorStateAndKeyedState() {

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10L,10L), "name", "uid");

		RegisteredOperatorStateMeta registeredOperatorStateMeta = createRegisteredOperatorStateMeta();
		RegisteredKeyedStateMeta registeredKeyedStateMeta = createRegisteredKeyedStateMeta();
		OperatorSubtaskStateMeta operatorSubtaskStateMeta = OperatorSubtaskStateMeta.of(registeredOperatorStateMeta, registeredKeyedStateMeta);
		operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta);

		Assert.assertEquals(registeredKeyedStateMeta, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(registeredOperatorStateMeta, operatorStateMeta.getOperatorStateMeta());
	}

	@Test
	public void testMergeDifferentStateMeta() {

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(new OperatorID(10L,10L), "name", "uid");

		RegisteredOperatorStateMeta registeredOperatorStateMeta = createRegisteredOperatorStateMeta();
		RegisteredKeyedStateMeta registeredKeyedStateMeta = createRegisteredKeyedStateMeta();
		RegisteredKeyedStateMeta registeredKeyedStateMeta1 = createRegisteredKeyedStateMeta1();

		OperatorSubtaskStateMeta emptyOperatorSubtaskStateMeta = OperatorSubtaskStateMeta.empty();
		operatorStateMeta.mergeSubtaskStateMeta(emptyOperatorSubtaskStateMeta);
		Assert.assertEquals(null, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(null, operatorStateMeta.getOperatorStateMeta());

		OperatorSubtaskStateMeta operatorSubtaskStateMeta = OperatorSubtaskStateMeta.of(createRegisteredOperatorStateMeta(), createRegisteredKeyedStateMeta());
		operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta);
		Assert.assertEquals(1, operatorStateMeta.getKeyedStateMeta().getStateMetaData().size());
		Assert.assertEquals(registeredKeyedStateMeta, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(registeredOperatorStateMeta, operatorStateMeta.getOperatorStateMeta());

		OperatorSubtaskStateMeta operatorSubtaskStateMeta1 = OperatorSubtaskStateMeta.of(createRegisteredOperatorStateMeta(), createRegisteredKeyedStateMeta1());
		operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta1);
		Assert.assertEquals(2, operatorStateMeta.getKeyedStateMeta().getStateMetaData().size());
		Assert.assertEquals(registeredKeyedStateMeta1, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(registeredOperatorStateMeta, operatorStateMeta.getOperatorStateMeta());

		OperatorSubtaskStateMeta operatorSubtaskStateMeta2 = OperatorSubtaskStateMeta.of(createRegisteredOperatorStateMeta(), createRegisteredKeyedStateMeta());
		operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta2);
		Assert.assertEquals(2, operatorStateMeta.getKeyedStateMeta().getStateMetaData().size());
		Assert.assertEquals(registeredKeyedStateMeta1, operatorStateMeta.getKeyedStateMeta());
		Assert.assertEquals(registeredOperatorStateMeta, operatorStateMeta.getOperatorStateMeta());

	}

	private RegisteredKeyedStateMeta createRegisteredKeyedStateMeta(){
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state1", StringSerializer.INSTANCE, statefulSerializer);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state1", mapStateDescriptor.getType(), mapStateDescriptor);
		HashMap map1 = new HashMap();
		map1.put("test-state1", keyedStateMetaData);
		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map1);
		return registeredKeyedStateMeta;
	}

	private RegisteredKeyedStateMeta createRegisteredKeyedStateMeta1(){
		final TypeSerializer<String> keySerialzier = StringSerializer.INSTANCE;
		TypeSerializer<String> statefulSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("test-state1", StringSerializer.INSTANCE, statefulSerializer);
		MapStateDescriptor mapStateDescriptor1 = new MapStateDescriptor("test-state2", String.class, String.class);

		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state1", mapStateDescriptor.getType(), mapStateDescriptor);
		RegisteredKeyedStateMeta.KeyedStateMetaData keyedStateMetaData1 = new RegisteredKeyedStateMeta.KeyedStateMetaData("test-state2", mapStateDescriptor1.getType(), mapStateDescriptor1);

		HashMap map1 = new HashMap();
		map1.put(keyedStateMetaData.getName(), keyedStateMetaData);
		map1.put(keyedStateMetaData1.getName(), keyedStateMetaData1);

		RegisteredKeyedStateMeta registeredKeyedStateMeta = new RegisteredKeyedStateMeta(keySerialzier, BackendType.MOCK_STATE_BACKEND, map1);
		return registeredKeyedStateMeta;
	}

	private RegisteredOperatorStateMeta createRegisteredOperatorStateMeta(){
		ListStateDescriptor<Long> stateDescr = new ListStateDescriptor<>("test-state", LongSerializer.INSTANCE);
		RegisteredOperatorStateMeta.OperatorStateMetaData operatorStateMetaData = new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, "test-state", stateDescr.getType(), stateDescr);
		HashMap map = new HashMap();
		map.put("test-state", operatorStateMetaData);
		RegisteredOperatorStateMeta registeredOperatorStateMeta = new RegisteredOperatorStateMeta(BackendType.OPERATOR_STATE_BACKEND, map);
		return registeredOperatorStateMeta;
	}
}
