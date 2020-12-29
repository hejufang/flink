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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
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
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
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

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Harness tests for {@link LookupJoinRunner} and {@link LookupJoinWithCalcRunner}.
 */
public class LookupJoinHarnessTest {

	private final TypeSerializer<RowData> inSerializer = new RowDataSerializer(
		new ExecutionConfig(),
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH));

	private static final long LATER_TIME_MS = 3000L;
	private static final RowDataTypeInfo ROW_DATA_TYPE_INFO = new RowDataTypeInfo(new IntType(), new VarCharType());
	private static final RowType INPUT_ROW_TYPE = RowType.of(new IntType(), new VarCharType());

	private final RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(new TypeInformation[]{
		Types.INT,
		Types.STRING,
		Types.INT,
		Types.STRING
	});

	@Test
	public void testTemporalInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithLaterJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			LATER_TIME_MS,
			ROW_DATA_TYPE_INFO
			);

		testHarness.open();
		testHarness.setProcessingTime(0L);
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.setProcessingTime(LATER_TIME_MS);
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithLaterRetry2Join() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			LATER_TIME_MS,
			ROW_DATA_TYPE_INFO,
			2
		);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.setProcessingTime(LATER_TIME_MS);
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.setProcessingTime(LATER_TIME_MS * 2);
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalInnerJoinWithLaterJoinFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER,
			LATER_TIME_MS,
			ROW_DATA_TYPE_INFO);

		testHarness.open();
		testHarness.setProcessingTime(0);
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.setProcessingTime(LATER_TIME_MS);
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalWithLaterLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			LATER_TIME_MS,
			ROW_DATA_TYPE_INFO);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.setProcessingTime(LATER_TIME_MS);
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(5, "e"));
		testHarness.setProcessingTime(LATER_TIME_MS * 2);
		testHarness.processElement(insertRecord(4, "d"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(5, "e", null, null));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testTemporalLeftJoinWithLaterFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITH_FILTER,
			LATER_TIME_MS,
			ROW_DATA_TYPE_INFO);

		testHarness.open();

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.setProcessingTime(LATER_TIME_MS);
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(5, "e"));
		testHarness.setProcessingTime(LATER_TIME_MS * 2);
		testHarness.processElement(insertRecord(4, "d"));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(5, "e", null, null));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testMiniBatchLeftJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createMiniBatchHarness(
			JoinType.LEFT_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			5,
			new TestMiniBatchTableFunctionFeature());
		testHarness.open();
		// batch 0
		testHarness.processElement(insertRecord(null, "a"));
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(null, "a"));
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(null, "a"));
		// batch 1
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		testHarness.processElement(insertRecord(5, "e"));
		testHarness.processElement(insertRecord(2, "b"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(insertRecord(6, "e"));
		// must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(null, "a", null, null));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(null, "a", null, null));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(null, "a", null, null));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(5, "e", null, null));
		expectedOutput.add(insertRecord(2, "b", null, null));
		expectedOutput.add(insertRecord(6, "e", null, null));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testMiniBatchInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			3,
			new TestMiniBatchTableFunctionFeature());
		testHarness.open();
		// batch 0
		testHarness.processElement(insertRecord(null, "a"));
		testHarness.processElement(insertRecord(null, "a"));
		testHarness.processElement(insertRecord(1, "a"));
		//batch 1
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		//batch 2, test last non-full batch before close
		testHarness.processElement(insertRecord(5, "e"));
        // must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test
	public void testMiniBatchIntervalInnerJoin() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITHOUT_FILTER,
			2,
			new TestMiniBatchTableFunctionFeature());
		testHarness.open();
		//batch 1
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		//batch 2
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(insertRecord(1, "a"));
		//mini batch interval will finish bundle
		testHarness.processWatermark(System.currentTimeMillis());

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));

		List<Object> resultList = testHarness.getOutput().stream()
			//remove watermark from outputList
			.filter(x -> x instanceof StreamRecord)
			.collect(Collectors.toList());
		assertor.assertOutputEquals("output wrong.", expectedOutput, resultList);
		testHarness.close();
	}

	@Test
	public void testMiniBatchInnerJoinWithFilter() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER,
			2,
			new TestMiniBatchTableFunctionFeature());
		testHarness.open();
		//batch 1
		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		//batch 2
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		//batch 3, test last non-full batch before close
		testHarness.processElement(insertRecord(5, "e"));
		// must close it to let the last batch run
		testHarness.close();

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
		expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
		expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));

		assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
	}

	@Test(expected = FlinkRuntimeException.class)
	public void testMiniBatchInnerJoinThrowWrongResultCount() throws Exception {
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createMiniBatchHarness(
			JoinType.INNER_JOIN,
			FilterOnTable.WITH_FILTER,
			2,
			new TestMiniBatchTableFunctionFeatureWrongResultCount());
		testHarness.open();
		//batch 1
		try {
			testHarness.processElement(insertRecord(1, "a"));
			testHarness.processElement(insertRecord(2, "b"));
		} finally {
			testHarness.close();
		}
	}

	// ---------------------------------------------------------------------------------

	private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable) throws Exception {
		return createHarness(joinType, filterOnTable, -1L, new RowDataTypeInfo(new IntType(), new VarCharType()));
	}

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
		JoinType joinType,
		FilterOnTable filterOnTable,
		long laterJoinMs,
		RowDataTypeInfo rowType
	) throws Exception {
		return createHarness(joinType, filterOnTable, laterJoinMs, rowType, 1);
	}

	@SuppressWarnings("unchecked")
	private OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
			JoinType joinType,
			FilterOnTable filterOnTable,
			long laterJoinMs,
			RowDataTypeInfo rowType,
			int laterJoinRetryTimes) throws Exception {
		boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
		ProcessFunction<RowData, RowData> joinRunner;

		/* Set retry times, because GeneratedFunctionWrapper only support construct without args */
		TestingFetcherWithLaterFunction.laterRetryTimes = laterJoinRetryTimes;
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			if (laterJoinMs <= 0) {
				joinRunner = new LookupJoinRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					isLeftJoin,
					2);
			} else {
				joinRunner = new LookupJoinWithRetryRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherWithLaterFunction()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					rowType,
					isLeftJoin,
					2,
					laterJoinMs,
					laterJoinRetryTimes);
			}
		} else {
			if (laterJoinMs <= 0) {
				joinRunner = new LookupJoinWithCalcRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherFunction()),
					new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					isLeftJoin,
					2);
			} else {
				joinRunner = new LookupJoinWithCalcRetryRunner(
					new GeneratedFunctionWrapper<>(new TestingFetcherWithLaterFunction()),
					new GeneratedFunctionWrapper<>(new CalculateOnTemporalTable()),
					new GeneratedCollectorWrapper<>(new TestingFetcherCollector()),
					rowType,
					isLeftJoin,
					2,
					laterJoinMs,
					laterJoinRetryTimes);
			}
		}

		ProcessOperator<RowData, RowData> operator = new ProcessOperator<>(joinRunner);
		return new OneInputStreamOperatorTestHarness<>(
			operator,
			inSerializer);
	}

	private OneInputStreamOperatorTestHarness<RowData, RowData> createMiniBatchHarness(
			JoinType joinType,
			FilterOnTable filterOnTable,
			int batchSize,
			MiniBatchTableFunction feature) throws Exception {
		if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
			LookupJoinBundleFunction bundleFunction = new LookupJoinBundleFunction(
				new GeneratedFunctionWrapper(new TestMiniBatchKeyConverterFunction()),
				new GeneratedCollectorWrapper(new TestingFetcherCollector()),
				feature,
				INPUT_ROW_TYPE,
				joinType == JoinType.LEFT_JOIN,
				2);
			ListBundleOperator<RowData, RowData> bundleOperator = new ListBundleOperator<>(
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
				feature,
				INPUT_ROW_TYPE,
				joinType == JoinType.LEFT_JOIN,
				2);
			ListBundleOperator<RowData, RowData> bundleOperator = new ListBundleOperator<>(
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
	 * returns zero or one or more RowData.
	 */
	public static final class TestingFetcherFunction implements FlatMapFunction<RowData, RowData> {

		private static final long serialVersionUID = 4018474964018227081L;

		private static final Map<Integer, List<GenericRowData>> data = new HashMap<>();

		static {
			data.put(1, Collections.singletonList(
				GenericRowData.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
				GenericRowData.of(3, fromString("Jark")),
				GenericRowData.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
				GenericRowData.of(4, fromString("Fabian"))));
		}

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			int id = value.getInt(0);
			List<GenericRowData> rows = data.get(id);
			if (rows != null) {
				for (GenericRowData row : rows) {
					out.collect(row);
				}
			}
		}
	}

	/**
	 * The {@link TestingFetcherWithLaterFunction} only accepts a single integer lookup key and
	 * returns zero or one or more RowData.
	 */
	public static final class TestingFetcherWithLaterFunction implements FlatMapFunction<RowData, RowData> {

		private static final long serialVersionUID = 1L;

		private final Map<Integer, Tuple2<Integer, List<GenericRowData>>> data = new HashMap<>();

		public static int laterRetryTimes = 1;

		public TestingFetcherWithLaterFunction() {
			data.put(1, Tuple2.of(laterRetryTimes, Collections.singletonList(
				GenericRowData.of(1, fromString("Julian")))));
			data.put(3, Tuple2.of(0, Arrays.asList(
				GenericRowData.of(3, fromString("Jark")),
				GenericRowData.of(3, fromString("Jackson")))));
			data.put(4, Tuple2.of(0, Collections.singletonList(
				GenericRowData.of(4, fromString("Fabian")))));
		}

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			int id = value.getInt(0);
			Tuple2<Integer, List<GenericRowData>> rows = data.get(id);
			if (rows != null) {
				if (rows.f0 > 0) {
					rows.f0 -= 1;
					return;
				}
				for (GenericRowData row : rows.f1) {
					out.collect(row);
				}
			}
		}
	}

	/**
	 * The {@link TestingFetcherCollector} is a simple implementation of
	 * {@link TableFunctionCollector} which combines left and right into a JoinedRowData.
	 */
	public static final class TestingFetcherCollector extends TableFunctionCollector {
		private static final long serialVersionUID = -312754413938303160L;

		@Override
		public void collect(Object record) {
			RowData left = (RowData) getInput();
			RowData right = (RowData) record;
			outputResult(new JoinedRowData(left, right));
		}
	}

	/**
	 * The {@link CalculateOnTemporalTable} is a filter on temporal table which only accepts
	 * length of name greater than or equal to 6.
	 */
	public static final class CalculateOnTemporalTable implements FlatMapFunction<RowData, RowData> {

		private static final long serialVersionUID = -1860345072157431136L;

		@Override
		public void flatMap(RowData value, Collector<RowData> out) throws Exception {
			BinaryStringData name = (BinaryStringData) value.getString(1);
			if (name.getSizeInBytes() >= 6) {
				out.collect(value);
			}
		}
	}

	/**
	 * Test class for mini batch lookup fetch which extends TestMiniBatchTableFunctionFeature.
	 */
	public static final class TestMiniBatchTableFunctionFeature extends MiniBatchTableFunction<RowData> {
		private static final long serialVersionUID = 1L;
		private static final Map<Integer, List<GenericRowData>> data = new HashMap<>();

		public TestMiniBatchTableFunctionFeature (){
			data.put(1, Collections.singletonList(
				GenericRowData.of(1, fromString("Julian"))));
			data.put(3, Arrays.asList(
				GenericRowData.of(3, fromString("Jark")),
				GenericRowData.of(3, fromString("Jackson"))));
			data.put(4, Collections.singletonList(
				GenericRowData.of(4, fromString("Fabian"))));
		}

		@Override
		public int batchSize() {
			return 2;
		}

		@Override
		public List<Collection<RowData>> eval(List<Object[]> keySequenceList) {
			List<Collection<RowData>> resultRows = new ArrayList<>();
			for (Object[] keys : keySequenceList) {
				if (!data.containsKey(keys[0])) {
					resultRows.add(null);
				} else {
					List<GenericRowData> joinedRows = data.get(keys[0]);
					Collection<RowData> rows = new ArrayList<>();
					for (GenericRowData joinedRow : joinedRows) {
						rows.add(joinedRow);
					}
					resultRows.add(rows);
				}
			}
			return resultRows;
		}
	}

	/**
	 * Test class for mini batch lookup fetch which extends TestMiniBatchTableFunctionFeature.
	 * It returns empty result.
	 */
	public static final class TestMiniBatchTableFunctionFeatureWrongResultCount extends MiniBatchTableFunction<RowData> {
		private static final long serialVersionUID = 1L;
		@Override
		public List<Collection<RowData>> eval(List<Object[]> keySequenceList) {
			return new ArrayList<>();
		}
	}

	/**
	 * Test class for mini batch key converter function. Convert RowData to Object[] keys.
	 */
	public static final class TestMiniBatchKeyConverterFunction implements MapFunction<RowData, Object[]> {
		private static final long serialVersionUID = 1L;

		@Override
		public Object[] map(RowData value){
			if (value.isNullAt(0)) {
				return null;
			} else {
				return new Object[]{value.getInt(0)};
			}
		}
	}
}
