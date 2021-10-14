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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.state.api.input.splits.KeyGroupRangeInputSplit;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.utils.ReduceSum;
import org.apache.flink.state.table.catalog.SavepointCatalogUtils;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.state.api.input.WindowReaderTest.getWindowOperator;

/**
 * Test case for KeyedStateInputFormatV2.
 */
public class KeyedStateInputFormatV2Test {

	private static ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Types.INT);

	private static MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", Types.STRING, Types.STRING);

	private static ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState", Types.STRING);

	private static OneInputStreamOperator keyedStateOperator = new StreamFlatMap<>(new KeyedStateInputFormatV2Test.StatefulFunction());

	private static KeyedStateInputFormatV2.Builder builder;
	private static OperatorID operatorID = OperatorIDGenerator.fromUid("uid");

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();
	private static DynamicTableSource.DataStructureConverter converter;

	@Before
	public void setUpConverter(){
		DataType keyedStateDataType = SavepointCatalogUtils.KEYED_STATE_TABLE_SCHEMA.toPhysicalRowDataType();
		converter = ScanRuntimeProviderContext.INSTANCE.createDataStructureConverter(keyedStateDataType);
	}

	public KeyedStateInputFormatV2 setUpKeyedStateInputFormat(StateDescriptor stateDescriptor, OneInputStreamOperator operator) throws Exception {

		builder = new KeyedStateInputFormatV2.Builder(temporaryFolder.newFolder().toURI().toString(), operatorID.toString(), stateDescriptor.getName(), converter);

		OperatorSnapshotFinalizer state = createOperatorSubtaskState(operator);
		OperatorState operatorState = new OperatorState(operatorID, 1, 128);
		operatorState.putState(0, state.getJobManagerOwnedState());

		OperatorStateMeta operatorStateMeta = new OperatorStateMeta(operatorID);
		operatorStateMeta.mergeSubtaskStateMeta(state.getJobManagerOwnedStateMeta());

		builder.setOperatorState(operatorState);
		builder.setOperatorStateMeta(operatorStateMeta);
		KeyedStateInputFormatV2 format = builder.build();

		return format;
	}

	@Test
	public void testCreatePartitionedInputSplits() throws Exception {

		KeyedStateInputFormatV2  format = setUpKeyedStateInputFormat(listStateDescriptor, keyedStateOperator);
		KeyGroupRangeInputSplit[] splits = format.createInputSplits(4);
		Assert.assertEquals(
			"Failed to properly partition operator state into input splits", 4, splits.length);
	}

	@Test
	public void testMaxParallelismRespected() throws Exception {

		KeyedStateInputFormatV2 format = setUpKeyedStateInputFormat(listStateDescriptor, keyedStateOperator);
		KeyGroupRangeInputSplit[] splits = format.createInputSplits(129);
		Assert.assertEquals(
				"Failed to properly partition operator state into input splits",
				128,
				splits.length);
	}

	@Test
	public void testReadValueState() throws Exception {

		KeyedStateInputFormatV2 format = setUpKeyedStateInputFormat(valueStateDescriptor, keyedStateOperator);
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];
		List<RowData> data = readInputSplit(format, split);

		List<GenericRowData> expect = Arrays.asList(
			genResultRow("1", "VoidNamespace", "1"),
			genResultRow("2", "VoidNamespace", "2"),
			genResultRow("3", "VoidNamespace", "3"));

		Assert.assertEquals("Incorrect data read from input split", expect, data);
	}

	@Test
	public void testReadMapState() throws Exception {

		KeyedStateInputFormatV2 format = setUpKeyedStateInputFormat(mapStateDescriptor, keyedStateOperator);
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];
		List<RowData> data = readInputSplit(format, split);

		List<GenericRowData> expect = Arrays.asList(
			genResultRow("1", "VoidNamespace", "userKey_0=value_0"),
			genResultRow("2", "VoidNamespace", "userKey_0=value_0"),
			genResultRow("2", "VoidNamespace", "userKey_1=value_1"),
			genResultRow("3", "VoidNamespace", "userKey_0=value_0"),
			genResultRow("3", "VoidNamespace", "userKey_1=value_1"),
			genResultRow("3", "VoidNamespace", "userKey_2=value_2"));

		Assert.assertEquals("Incorrect data read from input split", expect, data);
	}

	@Test
	public void testReadListState() throws Exception {

		KeyedStateInputFormatV2 format = setUpKeyedStateInputFormat(listStateDescriptor, keyedStateOperator);
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];
		List<RowData> data = readInputSplit(format, split);

		List<GenericRowData> expect = Arrays.asList(
			genResultRow("1", "VoidNamespace", "value_0"),
			genResultRow("2", "VoidNamespace", "value_0"),
			genResultRow("2", "VoidNamespace", "value_1"),
			genResultRow("3", "VoidNamespace", "value_0"),
			genResultRow("3", "VoidNamespace", "value_1"),
			genResultRow("3", "VoidNamespace", "value_2"));

		Assert.assertEquals("Incorrect data read from input split", expect, data);
	}

	@Test
	public void testReaderWindowState() throws Exception {
		OneInputStreamOperator windowOperator = getWindowOperator(stream -> stream.timeWindow(Time.milliseconds(1)).reduce(new ReduceSum()));

		ReducingStateDescriptor<Integer> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new ReduceSum(),
				Types.INT);

		KeyedStateInputFormatV2 format = setUpKeyedStateInputFormat(stateDesc, windowOperator);
		KeyGroupRangeInputSplit split = format.createInputSplits(1)[0];
		List<RowData> data = readInputSplit(format, split);

		List<GenericRowData> expect = Arrays.asList(
			genResultRow("1", "TimeWindow{start=1, end=2}", "1"),
			genResultRow("2", "TimeWindow{start=2, end=3}", "2"),
			genResultRow("3", "TimeWindow{start=3, end=4}", "3"));

		Assert.assertEquals("Incorrect data read from input split", expect, data);
	}

	@Nonnull
	private List<RowData> readInputSplit(
			KeyedStateInputFormatV2 format,
			KeyGroupRangeInputSplit split)
			throws IOException {

		List<RowData> data = new ArrayList<>();
		format.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 0));

		format.openInputFormat();
		format.open(split);

		while (!format.reachedEnd()) {
			data.add(format.nextRecord(null));
		}

		format.close();
		format.closeInputFormat();

		data.sort(Comparator.comparing(rowData -> rowData.toString()));
		return data;
	}

	private OperatorSnapshotFinalizer createOperatorSubtaskState(
			OneInputStreamOperator<Integer, Void> operator) throws Exception {
		try (KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						operator, id -> id, Types.INT, 128, 1, 0)) {

			testHarness.setup(VoidSerializer.INSTANCE);
			testHarness.open();

			testHarness.processElement(1, 1);
			testHarness.processElement(2, 2);
			testHarness.processElement(3, 3);

			return testHarness.snapshotWithStateMeta(0, 0);
		}
	}

	static class StatefulFunction extends RichFlatMapFunction<Integer, Void> {
		ValueState<Integer> valueState;
		MapState<String, String> mapState;
		ListState<String> listState;

		@Override
		public void open(Configuration parameters) {
			valueState = getRuntimeContext().getState(valueStateDescriptor);
			mapState = getRuntimeContext().getMapState(mapStateDescriptor);
			listState = getRuntimeContext().getListState(listStateDescriptor);
		}

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			valueState.update(value);

			ArrayList<String> listVaules = new ArrayList();

			for (Integer i = 0; i < value; i++) {
				String userKey = "userKey_" + i;
				String userValue = "value_" + i;
				mapState.put(userKey, userValue);
				listVaules.add(userValue);
			}

			listState.update(listVaules);
		}
	}

	private GenericRowData genResultRow(String key, String namespace, String value){

		GenericRowData genericRowData = new GenericRowData(3);
		genericRowData.setField(0, BinaryStringData.fromString(key));
		genericRowData.setField(1, BinaryStringData.fromString(namespace));
		genericRowData.setField(2, BinaryStringData.fromString(value));

		return genericRowData;
	}
}
