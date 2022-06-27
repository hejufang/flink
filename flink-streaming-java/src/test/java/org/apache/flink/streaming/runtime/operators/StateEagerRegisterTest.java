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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateRegistry;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredKeyedStateMeta;
import org.apache.flink.runtime.checkpoint.RegisteredOperatorStateMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.tracker.BackendType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.OperatorStateMetaCollector;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests for stream operator chaining behaviour.
 */
@SuppressWarnings("serial")
public class StateEagerRegisterTest {

	@Test
	public void testGetStateMetaWithRegisterState() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream stream = env
		.addSource(new TestStatefulSource()).uid("source")
		.keyBy(x -> x)
		.flatMap(new StateFulFlatMapFunction()).uid("flatMap");
		stream.addSink(new DiscardingSink<>()).uid("sink");

		// build jobGraph
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		// get State meta from jobGraph
		ArrayList<OperatorStateMeta> registeredStateFromJobGraph = (ArrayList<OperatorStateMeta>) OperatorStateMetaCollector.getRegisteredStateFromJobGraph(jobGraph, getClass().getClassLoader());

		// flap map state meta test
		OperatorStateMeta flapMapStateMeta = registeredStateFromJobGraph.get(0);
		RegisteredKeyedStateMeta expectedKeyedStateMeta = new RegisteredKeyedStateMeta(StringSerializer.INSTANCE, BackendType.UNKOWN, new HashMap<>());
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.listStateDescriptor));
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.valueStateDescriptor));
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.mapStateDescriptor));
		Assert.assertEquals(flapMapStateMeta.getOperatorID().toString(), "4cf304da7f0eed0ff91c4cb6128e1ca7");
		Assert.assertEquals(flapMapStateMeta.getAllOperatorStateName().isEmpty(), true);
		Assert.assertEquals(flapMapStateMeta.getAllKeyedStateName().size(), 3);
		Assert.assertEquals(flapMapStateMeta.getKeyedStateMeta(), expectedKeyedStateMeta);

		// sink state meta test
		OperatorStateMeta sinkStateMeta = registeredStateFromJobGraph.get(1);
		Assert.assertEquals(sinkStateMeta.getOperatorID().toString(), "2e588ce1c86a9d46e2e85186773ce4fd");
		Assert.assertEquals(sinkStateMeta.getAllStateMeta().size(), 0);

		OperatorStateMeta sourceStateMeta = registeredStateFromJobGraph.get(2);
		RegisteredOperatorStateMeta expectedOperatorStateMeta = new RegisteredOperatorStateMeta(BackendType.UNKOWN, new HashMap<>());
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, TestStatefulSource.listStateDescriptor));
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.UNION, TestStatefulSource.unionStateDescriptor));
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.BROADCAST, TestStatefulSource.broadcastStateDescriptor));

		Assert.assertEquals(sourceStateMeta.getOperatorID().toString(), "e5a72f353fc1e6bbf3bd96a41384998c");
		Assert.assertEquals(sourceStateMeta.getAllStateMeta().size(), 3);
		Assert.assertEquals(sourceStateMeta.getOperatorStateMeta(), expectedOperatorStateMeta);
	}

	@Test
	public void testGetStateMetaExactFromJobGraph() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream stream = env
		.addSource(new TestStatefulSource()).uid("source")
		.keyBy(x -> x)
		.flatMap(new StateFulFlatMapFunction()).uid("flatMap");
		stream.addSink(new DiscardingSink<>()).uid("sink");

		// build jobGraph
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		// get State meta from jobGraph

		Map<OperatorID, OperatorStateMeta> operatorIDAndStateMeta = new LinkedHashMap<>();

		for (JobVertex vertex : jobGraph.getVertices()) {
			operatorIDAndStateMeta.putAll(vertex.getChainedOperatorIdAndStateMeta());
		}
		OperatorStateMeta[] operatorStateMetas = operatorIDAndStateMeta.values().toArray(new OperatorStateMeta[operatorIDAndStateMeta.size()]);
		OperatorStateMeta flapMapStateMeta = operatorStateMetas[1];
		RegisteredKeyedStateMeta expectedKeyedStateMeta = new RegisteredKeyedStateMeta(StringSerializer.INSTANCE, BackendType.UNKOWN, new HashMap<>());
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.listStateDescriptor));
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.valueStateDescriptor));
		expectedKeyedStateMeta.addStateMetaData(new RegisteredKeyedStateMeta.KeyedStateMetaData(StateFulFlatMapFunction.mapStateDescriptor));
		Assert.assertEquals(flapMapStateMeta.getOperatorID().toString(), "4cf304da7f0eed0ff91c4cb6128e1ca7");
		Assert.assertEquals(flapMapStateMeta.getAllOperatorStateName().isEmpty(), true);
		Assert.assertEquals(flapMapStateMeta.getAllKeyedStateName().size(), 3);
		Assert.assertEquals(flapMapStateMeta.getKeyedStateMeta(), expectedKeyedStateMeta);

		// sink state meta test
		OperatorStateMeta sinkStateMeta = operatorStateMetas[0];
		Assert.assertEquals(sinkStateMeta.getOperatorID().toString(), "2e588ce1c86a9d46e2e85186773ce4fd");
		Assert.assertEquals(sinkStateMeta.getAllStateMeta().size(), 0);

		OperatorStateMeta sourceStateMeta = operatorStateMetas[2];
		RegisteredOperatorStateMeta expectedOperatorStateMeta = new RegisteredOperatorStateMeta(BackendType.UNKOWN, new HashMap<>());
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE, TestStatefulSource.listStateDescriptor));
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.UNION, TestStatefulSource.unionStateDescriptor));
		expectedOperatorStateMeta.addStateMetaData(new RegisteredOperatorStateMeta.OperatorStateMetaData(OperatorStateHandle.Mode.BROADCAST, TestStatefulSource.broadcastStateDescriptor));

		Assert.assertEquals(sourceStateMeta.getOperatorID().toString(), "e5a72f353fc1e6bbf3bd96a41384998c");
		Assert.assertEquals(sourceStateMeta.getAllStateMeta().size(), 3);
		Assert.assertEquals(sourceStateMeta.getOperatorStateMeta(), expectedOperatorStateMeta);
	}

	private static class TestStatefulSource extends RichSourceFunction<String> {

		public static ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>(
		"LIST_STATE", Integer.TYPE);
		public static ListStateDescriptor<String> unionStateDescriptor = new ListStateDescriptor<>(
		"UNION_STATE", String.class);
		public static MapStateDescriptor<Integer, String> broadcastStateDescriptor = new MapStateDescriptor<>(
		"BROADCAST_STATE", Integer.TYPE, String.class);


		private ListState list;
		private ListState union;
		private BroadcastState broadcastState;

		@Override
		public void registerState(StateRegistry stateRegistry) throws Exception {
			list = stateRegistry.getOperatorStateStore().getListState(listStateDescriptor);
			union = stateRegistry.getOperatorStateStore().getUnionListState(unionStateDescriptor);
			broadcastState = stateRegistry.getOperatorStateStore().getBroadcastState(broadcastStateDescriptor);
		}

		@Override
		public void run(SourceContext ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}

	private static class StateFulFlatMapFunction extends RichFlatMapFunction<String, String> {

		public static ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("VAULE_STAET", Integer.TYPE);
		public static ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>(
		"LIST_STATE", Integer.TYPE);
		public static MapStateDescriptor<Integer, String> mapStateDescriptor = new MapStateDescriptor<>(
		"MAP_STATE", Integer.TYPE, String.class);

		private ValueState valueState;
		private ListState listState;
		private MapState mapState;

		@Override
		public void registerState(StateRegistry stateRegistry) throws Exception {
			valueState = stateRegistry.getState(valueStateDescriptor);
			listState = stateRegistry.getListState(listStateDescriptor);
			mapState = stateRegistry.getMapState(mapStateDescriptor);
		}

		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {

		}
	}
}
