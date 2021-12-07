/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.input.splits.OperatorStateInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.table.catalog.tables.OperatorStateTable;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test case for OperatorStateInputFormatV2.
 */
public class OperatorStateInputFormatV2Test {

	private static ListStateDescriptor<Integer> unionStateDescriptor = new ListStateDescriptor<>("union", Types.INT);
	private static MapStateDescriptor<Integer, Integer> broadCastStateDescriptor = new MapStateDescriptor<>("broadcast", Types.INT, Types.INT);
	private static ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("list", Types.INT);
	private static ListStateDescriptor<Integer> multiListStateDescriptor1 = new ListStateDescriptor<>("multiList1", Types.INT);
	private static ListStateDescriptor<Integer> multiListStateDescriptor2 = new ListStateDescriptor<>("multiList2", Types.INT);

	private static OperatorStateInputFormatV2.Builder builder;
	private static OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	public OperatorStateInputFormatV2 setUpOperatorStateInputFormat(List<StateDescriptor> stateDescriptors) throws Exception {

		builder = new OperatorStateInputFormatV2.Builder(temporaryFolder.newFolder().toURI().toString(), operatorID.toString(), stateDescriptors.stream().map(desc -> desc.getName()).collect(Collectors.toList()),
			OperatorStateTable.OPERATOR_STATE_TABLE_SCHEMA.toPhysicalRowDataType());

		AbstractStreamOperatorTestHarness testHarness;

		switch (stateDescriptors.get(0).getName()) {
			case "union":
				testHarness = getUnionStateTestHarness();
				break;
			case "broadcast":
				testHarness = getBroadCastTestHarness();
				break;
			case "list":
				testHarness = getListStateTestHarness();
				break;
			case "multiList1":
				testHarness = getMultiListStateTestHarness();
				break;
			default:
				throw new RuntimeException("Unsupported state descriptor");
		}

		OperatorSnapshotFinalizer state = testHarness.snapshotWithStateMeta(0, 0);
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state.getJobManagerOwnedState());

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(operatorID);
		operatorStateMeta.mergeSubtaskStateMeta(state.getJobManagerOwnedStateMeta());

		builder.setOperatorState(operatorState);
		builder.setOperatorStateMeta(operatorStateMeta);

		return builder.build();
	}

	public OperatorStateInputFormatV2 setUpOperatorStateInputFormat(StateDescriptor stateDescriptor) throws Exception {
		return setUpOperatorStateInputFormat(Collections.singletonList(stateDescriptor));
	}

	@Test
	public void testReadBroadcastState() throws Exception {

		OperatorStateInputFormatV2 format = setUpOperatorStateInputFormat(broadCastStateDescriptor);

		OperatorStateInputSplit inputSplit = format.createInputSplits(1)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
		format.open(inputSplit);
		List<RowData> results = getQueryResult(format);

		List<GenericRowData> expected = Arrays.asList(
				genResultRow("1=1"),
				genResultRow("2=2"),
				genResultRow("3=3"));

		Assert.assertEquals("Failed to read correct list state from state backend", expected, results);
	}

	@Test
	public void testReadListState() throws Exception {

		OperatorStateInputFormatV2 format = setUpOperatorStateInputFormat(listStateDescriptor);
		// without rescaling
		OperatorStateInputSplit inputSplit = format.createInputSplits(1)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));
		format.open(inputSplit);
		List<RowData> results = getQueryResult(format);
		List<GenericRowData> expected = Arrays.asList(
				genResultRow("1"),
				genResultRow("2"),
				genResultRow("3"));
		Assert.assertEquals("Failed to read correct list state from state backend", results, expected);
		// change the parallelism to 3
		OperatorStateInputSplit inputSplitAfterScaleUp = format.createInputSplits(3)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 3, 0));
		format.open(inputSplitAfterScaleUp);
		List<RowData> resultsAfterScaleUp = getQueryResult(format);
		List<GenericRowData> expectedAfterScaleUp = Arrays.asList(
				genResultRow("1"));
		Assert.assertEquals("Failed to read correct list state from state backend", resultsAfterScaleUp, expectedAfterScaleUp);

		// change the parallelism to 3
		OperatorStateInputSplit inputSplitAfterScaleUp2 = format.createInputSplits(3)[2];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 3, 0));
		format.open(inputSplitAfterScaleUp2);
		List<RowData> resultsAfterScaleUp2 = getQueryResult(format);
		List<GenericRowData> expectedAfterScaleUp2 = Arrays.asList(
				genResultRow("2"));
		Assert.assertEquals("Failed to read correct list state from state backend", resultsAfterScaleUp2, expectedAfterScaleUp2);
	}

	@Test
	public void testReadUnionState() throws Exception {
		OperatorStateInputFormatV2 format = setUpOperatorStateInputFormat(unionStateDescriptor);
		OperatorStateInputSplit inputSplit = format.createInputSplits(1)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 4, 0));
		format.open(inputSplit);

		List<RowData> results = getQueryResult(format);
		List<GenericRowData> expected = Arrays.asList(
				genResultRow("1"),
				genResultRow("2"),
				genResultRow("3"));
		Assert.assertEquals("Failed to read correct union state from state backend", expected, results);

		// change the parallelism to 3
		OperatorStateInputSplit inputSplitAfterScaleUp = format.createInputSplits(3)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 3, 0));
		format.open(inputSplitAfterScaleUp);
		List<RowData> resultsAfterScaleUp = getQueryResult(format);
		Assert.assertEquals("Failed to read correct union state from state backend", expected, resultsAfterScaleUp);
		// only will split 0 can read data
		OperatorStateInputSplit inputSplitAfterScaleUp2 = format.createInputSplits(3)[1];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 3, 0));
		format.open(inputSplitAfterScaleUp2);
		List<RowData> resultsAfterScaleUp2 = getQueryResult(format);
		Assert.assertEquals("Failed to read correct union state from state backend", Collections.emptyList(), resultsAfterScaleUp2);
	}

	@Test
	public void testReadMultiState() throws Exception {

		OperatorStateInputFormatV2 format = setUpOperatorStateInputFormat(Stream.of(multiListStateDescriptor1, multiListStateDescriptor2).collect(Collectors.toList()));
		OperatorStateInputSplit inputSplit = format.createInputSplits(1)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 4, 0));
		format.open(inputSplit);

		List<RowData> results = getQueryResult(format);

		List<GenericRowData> expected = Arrays.asList(
				genResultRow("1"),
				genResultRow("2"),
				genResultRow("3"),
				genResultRow("1"),
				genResultRow("2"),
				genResultRow("3"));
		Assert.assertEquals("Failed to read correct union state from state backend", expected, results);

		// change the parallelism to 3
		OperatorStateInputSplit inputSplitAfterScaleUp = format.createInputSplits(3)[0];
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 3, 0));
		format.open(inputSplitAfterScaleUp);
		List<GenericRowData> expectedAfterScaleUp = Arrays.asList(
			genResultRow("1"),
			genResultRow("1"));
		List<RowData> resultsAfterScaleUp = getQueryResult(format);
		Assert.assertEquals("Failed to read correct union state from state backend", expectedAfterScaleUp, resultsAfterScaleUp);
	}

	List<RowData> getQueryResult(OperatorStateInputFormatV2 format){
		List<RowData> results = new ArrayList<>();
		while (!format.reachedEnd()) {
			RowData rowData = format.nextRecord(null);
			results.add(rowData);
		}
		return results;
	}

	private GenericRowData genResultRow(String value){
		GenericRowData genericRowData = new GenericRowData(1);
		genericRowData.setField(0, BinaryStringData.fromString(value));
		return genericRowData;
	}

	private TwoInputStreamOperatorTestHarness<Void, Integer, Void> getBroadCastTestHarness() throws Exception {
		TwoInputStreamOperatorTestHarness testHarness = new TwoInputStreamOperatorTestHarness<>(
			new CoBroadcastWithNonKeyedOperator<>(
				new BroadCastStatefulFunction(), Collections.singletonList(broadCastStateDescriptor)));

		testHarness.open();
		testHarness.processElement2(new StreamRecord<>(1));
		testHarness.processElement2(new StreamRecord<>(2));
		testHarness.processElement2(new StreamRecord<>(3));

		return testHarness;
	}

	static class BroadCastStatefulFunction extends BroadcastProcessFunction<Void, Integer, Void> {

		@Override
		public void processElement(Void value, ReadOnlyContext ctx, Collector<Void> out) {}

		@Override
		public void processBroadcastElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
			ctx.getBroadcastState(broadCastStateDescriptor).put(value, value);
		}
	}

	private OneInputStreamOperatorTestHarness<Integer, Void> getListStateTestHarness() throws Exception {
		OneInputStreamOperatorTestHarness testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(new ListStatefulFunction()), IntSerializer.INSTANCE);
		testHarness.open();

		testHarness.processElement(1, 0);
		testHarness.processElement(2, 0);
		testHarness.processElement(3, 0);

		return testHarness;
	}

	static class ListStatefulFunction implements FlatMapFunction<Integer, Void>, CheckpointedFunction {
		ListState<Integer> state;

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			state.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(listStateDescriptor);
		}
	}

	static class MultiListStatefulFunction implements FlatMapFunction<Integer, Void>, CheckpointedFunction {
		ListState<Integer> state1;
		ListState<Integer> state2;

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			state1.add(value);
			state2.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state1 = context.getOperatorStateStore().getListState(multiListStateDescriptor1);
			state2 = context.getOperatorStateStore().getListState(multiListStateDescriptor2);

		}
	}

	private OneInputStreamOperatorTestHarness<Integer, Void> getMultiListStateTestHarness() throws Exception {
		OneInputStreamOperatorTestHarness testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(new MultiListStatefulFunction()), IntSerializer.INSTANCE);
		testHarness.open();

		testHarness.processElement(1, 0);
		testHarness.processElement(2, 0);
		testHarness.processElement(3, 0);

		return testHarness;
	}

	private OneInputStreamOperatorTestHarness<Integer, Void> getUnionStateTestHarness() throws Exception {

		OneInputStreamOperatorTestHarness testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(new UnionStatefulFunction()), IntSerializer.INSTANCE);
		testHarness.open();
		testHarness.processElement(1, 0);
		testHarness.processElement(2, 0);
		testHarness.processElement(3, 0);
		return testHarness;
	}

	static class UnionStatefulFunction implements FlatMapFunction<Integer, Void>, CheckpointedFunction {
		ListState<Integer> state;

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			state.add(value);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getUnionListState(unionStateDescriptor);
		}
	}
}
