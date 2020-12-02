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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollectorWrapper;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRetryRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithRetryRunner;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private static final long LATER_OFFSET = 1000L;
	private static final RowDataTypeInfo ROW_DATA_TYPE_INFO = new RowDataTypeInfo(new IntType(), new VarCharType());

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

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		Thread.sleep(LATER_TIME_MS);
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

		testHarness.processElement(insertRecord(1, "a"));
		testHarness.processElement(insertRecord(2, "b"));
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(4, "d"));
		Thread.sleep(LATER_TIME_MS);
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
		Thread.sleep(LATER_TIME_MS);
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(5, "e"));
		Thread.sleep(LATER_TIME_MS);
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
		Thread.sleep(LATER_TIME_MS);
		testHarness.processElement(insertRecord(3, "c"));
		testHarness.processElement(insertRecord(5, "e"));
		Thread.sleep(LATER_TIME_MS);
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
		boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
		ProcessFunction<RowData, RowData> joinRunner;
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
					laterJoinMs - LATER_OFFSET);
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
					laterJoinMs - LATER_OFFSET);
			}
		}

		ProcessOperator<RowData, RowData> operator = new ProcessOperator<>(joinRunner);
		return new OneInputStreamOperatorTestHarness<>(
			operator,
			inSerializer);
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

		public TestingFetcherWithLaterFunction() {
			data.put(1, Tuple2.of(1, Collections.singletonList(
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
}
