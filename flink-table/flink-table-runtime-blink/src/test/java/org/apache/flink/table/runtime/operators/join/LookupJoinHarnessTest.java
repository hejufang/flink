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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.functions.MiniBatchTableFunction;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollectorWrapper;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.operators.bundle.ListBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinBundleFunction;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinBundleWithCalcFunction;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRetryRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithRetryRunner;
import org.apache.flink.table.runtime.typeutils.BaseRowSerializer;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}.
 */
public class LookupJoinHarnessTest {

	private static final TypeInformation MINI_BATCH_PRODUCE_TYPE_INFO =
		new RowTypeInfo(Types.INT, Types.STRING);
	private static final RowType MINI_BATCH_TABLE_SOURCE_ROW_TYPE;

	static{
		DataType dataType = TypeConversions.fromLegacyInfoToDataType(
			new RowTypeInfo(Types.INT, Types.STRING)
		);
		MINI_BATCH_TABLE_SOURCE_ROW_TYPE = (RowType) TypeConversions.fromDataToLogicalType(dataType);
	}

	private final TypeSerializer<BaseRow> inSerializer = new BaseRowSerializer(
		new ExecutionConfig(),
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH));

	private final BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(new TypeInformation[]{
		Types.INT,
		Types.STRING,
		Types.INT,
		Types.STRING
	});

	@Test
	public void testTemporalInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithRetry() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			1000);

		testHarness.open();

		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testMiniBatchLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createMiniBatchHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			5,
			new TestMiniBatchTableFunction());
		testHarness.open();
		// batch 0
		testHarness.processElement(record(null, "a"));
		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(null, "a"));
		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(null, "a"));
		// batch 1
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		testHarness.processElement(record(5, "e"));
		testHarness.processElement(record(2, "b"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(record(6, "e"));
		// must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(null, "a", null, null));
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(null, "a", null, null));
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(null, "a", null, null));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(5, "e", null, null));
		expectedOutput.add(record(2, "b", null, null));
		expectedOutput.add(record(6, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testMiniBatchInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			3,
			new TestMiniBatchTableFunction());
		testHarness.open();
		// batch 0
		testHarness.processElement(record(null, "a"));
		testHarness.processElement(record(null, "a"));
		testHarness.processElement(record(1, "a"));
		//batch 1
		testHarness.processElement(record(2, "b"));
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		//batch 2, test last non-full batch before close
		testHarness.processElement(record(5, "e"));
		// must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testMiniBatchIntervalInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			2,
			new TestMiniBatchTableFunction());
		testHarness.open();
		//batch 1
		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		//batch 2
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(record(1, "a"));
		//mini batch interval will finish bundle
		testHarness.processWatermark(System.currentTimeMillis());

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jark"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));
		expectedOutput.add(record(1, "a", 1, "Julian"));

		List<Object> resultList = testHarness.getOutput().stream()
			//remove watermark from outputList
			.filter(x -> x instanceof StreamRecord)
			.collect(Collectors.toList());
		assertor.assertOutputEquals("output wrong.", expectedOutput, resultList);
		testHarness.close();
	}

	@Test
	public void testMiniBatchInnerJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER,
			2,
			new TestMiniBatchTableFunction());
		testHarness.open();
		//batch 1
		testHarness.processElement(record(1, "a"));
		testHarness.processElement(record(2, "b"));
		//batch 2
		testHarness.processElement(record(3, "c"));
		testHarness.processElement(record(4, "d"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(record(5, "e"));
		// must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record(1, "a", 1, "Julian"));
		expectedOutput.add(record(3, "c", 3, "Jackson"));
		expectedOutput.add(record(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testMiniBatchInnerJoinThrowWrongResultCount() throws Exception {
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER,
			2,
			new TestMiniBatchTableFunctionWrongResultCount());
		testHarness.open();
		//batch 1
		try {
			testHarness.processElement(record(1, "a"));
			testHarness.processElement(record(2, "b"));
		} finally {
			testHarness.close();
		}
	}


	// ---------------------------------------------------------------------------------

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable) throws Exception {
		return createHarness(joinType, filterOnTable, -1);
	}

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable,
			int laterLatencyMs) throws Exception {
		boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
		ProcessFunction<BaseRow, BaseRow> joinRunner;
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			if (laterLatencyMs < 0) {
				joinRunner = new LookupJoinRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					isLeftJoin,
					2);
			} else {
				joinRunner = new LookupJoinWithRetryRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					new BaseRowTypeInfo(new IntType(), new VarCharType()),
					isLeftJoin,
					2,
					laterLatencyMs);
			}
		} else {
			if (laterLatencyMs < 0) {
				joinRunner = new LookupJoinWithCalcRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					isLeftJoin,
					2);
			} else {
				joinRunner = new LookupJoinWithCalcRetryRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					new BaseRowTypeInfo(new IntType(), new VarCharType()),
					isLeftJoin,
					2,
					laterLatencyMs);
			}
		}

		ProcessOperator<BaseRow, BaseRow> operator = new ProcessOperator<>(joinRunner);
		return new OneInputStreamOperatorTestHarness<>(
			operator,
			inSerializer);
	}

	private OneInputStreamOperatorTestHarness<BaseRow, BaseRow> createMiniBatchHarness(
			JoinType joinType,
			FilterOnTable filterOnTable,
			int batchSize,
			MiniBatchTableFunction function) throws Exception {
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			LookupJoinBundleFunction bundleFunction = new LookupJoinBundleFunction(
				new GeneratedFunctionWrapper(new TestMiniBatchKeyConverterFunction()),
				new GeneratedCollectorWrapper(new TestingFetcherCollector()),
				function,
				MINI_BATCH_TABLE_SOURCE_ROW_TYPE,
				MINI_BATCH_PRODUCE_TYPE_INFO,
				joinType == JoinType.LEFT_JOIN,
				2);
			ListBundleOperator<BaseRow, BaseRow> bundleOperator = new ListBundleOperator<>(
				bundleFunction,
				new CountBundleTrigger(batchSize));
			return new OneInputStreamOperatorTestHarness<>(
				bundleOperator,
				inSerializer);
		} else {
			LookupJoinBundleWithCalcFunction bundleFunction = new LookupJoinBundleWithCalcFunction(
				new GeneratedFunctionWrapper(new TestMiniBatchKeyConverterFunction()),
				new GeneratedFunctionWrapper(new CalculateOnTemporalTable()),
				new GeneratedCollectorWrapper(new TestingFetcherCollector()),
				function,
				MINI_BATCH_TABLE_SOURCE_ROW_TYPE,
				MINI_BATCH_PRODUCE_TYPE_INFO,
				joinType == JoinType.LEFT_JOIN,
				2);
			ListBundleOperator<BaseRow, BaseRow> bundleOperator = new ListBundleOperator<>(
				bundleFunction,
				new CountBundleTrigger(batchSize));
			return new OneInputStreamOperatorTestHarness<>(
				bundleOperator,
				inSerializer);
		}
	}

	/**
	 * Whether this is a inner join or left join.
	 */
	private enum JoinType {
		INNER_JOIN,
		LEFT_JOIN
	}

	/**
	 * Whether there is a filter on temporal table.
	 */
	private enum FilterOnTable {
		WITH_FILTER,
		WITHOUT_FILTER
	}

	// ---------------------------------------------------------------------------------

	/**
	 * The {@link TestingFetcherFunction} only accepts a single integer lookup key and
	 * returns zero or one or more BaseRows.
	 */
	public static final class TestingFetcherFunction implements FlatMapFunction<BaseRow, BaseRow> {

		private static final long serialVersionUID = 4018474964018227081L;

		private static final Map<Integer, List<GenericRow>> data = new HashMap<>();

		static {
			data.put(1, Collections.singletonList(
				GenericRow.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
				GenericRow.of(3, fromString("Jark")),
				GenericRow.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
				GenericRow.of(4, fromString("Fabian"))));
		}

		@Override
		public void flatMap(BaseRow value, Collector<BaseRow> out) throws Exception {
			int id = value.getInt(0);
			List<GenericRow> rows = data.get(id);
			if (rows != null) {
				for (GenericRow row : rows) {
					out.collect(row);
				}
			}
		}
	}

	/**
	 * The {@link TestingFetcherCollector} is a simple implementation of
	 * {@link TableFunctionCollector} which combines left and right into a JoinedRow.
	 */
	public static final class TestingFetcherCollector extends TableFunctionCollector {
		private static final long serialVersionUID = -312754413938303160L;

		@Override
		public void collect(Object record) {
			BaseRow left = (BaseRow) getInput();
			BaseRow right = (BaseRow) record;
			outputResult(new JoinedRow(left, right));
		}
	}

	/**
	 * The {@link CalculateOnTemporalTable} is a filter on temporal table which only accepts
	 * length of name greater than or equal to 6.
	 */
	public static final class CalculateOnTemporalTable implements FlatMapFunction<BaseRow, BaseRow> {

		private static final long serialVersionUID = -1860345072157431136L;

		@Override
		public void flatMap(BaseRow value, Collector<BaseRow> out) throws Exception {
			BinaryString name = value.getString(1);
			if (name.getSizeInBytes() >= 6) {
				out.collect(value);
			}
		}
	}

	/**
	 * Test class for mini batch lookup fetch which extends {@link TestMiniBatchTableFunction }.
	 */
	public static final class TestMiniBatchTableFunction extends MiniBatchTableFunction<Row> {
		private static final Map<Integer, List<Row>> data = new HashMap<>();

		public TestMiniBatchTableFunction() {
			data.put(1, Collections.singletonList(
				Row.of(1, "Julian")));
			data.put(3, Arrays.asList(
				Row.of(3, "Jark"),
				Row.of(3, "Jackson")));
			data.put(4, Collections.singletonList(
				Row.of(4, "Fabian")));
		}

		@Override
		public int batchSize() {
			return 2;
		}

		@Override
		public List<Collection<Row>> eval(List<Object[]> keySequenceList) {
			List<Collection<Row>> resultRows = new ArrayList<>();
			for (Object[] keys : keySequenceList) {
				if (!data.containsKey(keys[0])) {
					resultRows.add(null);
				} else {
					List<Row> joinedRows = data.get(keys[0]);
					Collection<Row> rows = new ArrayList<>();
					for (Row joinedRow : joinedRows) {
						rows.add(joinedRow);
					}
					resultRows.add(rows);
				}
			}
			return resultRows;
		}
	}

	/**
	 * Test class for mini batch lookup fetch which extends {@link TestMiniBatchTableFunction }.
	 * It returns empty result.
	 */
	public static final class TestMiniBatchTableFunctionWrongResultCount
		extends MiniBatchTableFunction<BaseRow> {

		public static long serialVersionUID = 1L;

		@Override
		public List<Collection<BaseRow>> eval(List<Object[]> keySequenceList) {
			return new ArrayList<>();
		}
	}

	/**
	 * Test class for mini batch key converter function. Convert RowData to Object[] keys.
	 */
	public static final class TestMiniBatchKeyConverterFunction implements MapFunction<BaseRow, Object[]> {

		private static final long serialVersionUID = 1L;

		@Override
		public Object[] map(BaseRow value){
			if (value.isNullAt(0)) {
				return null;
			} else {
				return new Object[]{value.getInt(0)};
			}
		}
	}
}
