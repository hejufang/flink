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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.over.SumAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.runtime.util.BaseRowRecordEqualiser;
import org.apache.flink.table.runtime.util.BinaryRowKeySelector;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.record;

/**
 * Tests for {@link MiniBatchGroupAggFunction}.
 */
public class MiniBatchGroupAggFunctionTest {
	LogicalType[] inputFieldTypes = new LogicalType[] {
		new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType()};

	BaseRowTypeInfo outputType = new BaseRowTypeInfo(
		new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType());

	LogicalType[] accTypes = new LogicalType[] { new BigIntType(), new BigIntType() };
	BinaryRowKeySelector keySelector = new BinaryRowKeySelector(new int[]{0}, inputFieldTypes);
	BaseRowTypeInfo keyType = keySelector.getProducedType();
	GeneratedRecordEqualiser equaliser = new GeneratedRecordEqualiser("", "", new Object[0]) {

		private static final long serialVersionUID = 1L;

		@Override
		public RecordEqualiser newInstance(ClassLoader classLoader) {
			return new BaseRowRecordEqualiser();
		}
	};

	GeneratedAggsHandleFunction function =
		new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
			@Override
			public AggsHandleFunction newInstance(ClassLoader classLoader) {
				return new SumAggsHandleFunction(1);
			}
		};

	BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(
		outputType.getFieldTypes(),
		new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

	private MiniBatchGroupAggFunction createFunction() throws Exception {
		return new MiniBatchGroupAggFunction(
			function,
			equaliser,
			accTypes,
			RowType.of(inputFieldTypes),
			-1,
			false,
			10);
	}

	@Test
	public void testMiniBatchGroupAggWithStateTtl() throws Exception {
		MiniBatchGroupAggFunction function = createFunction();
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeyedMapBundleOperator operator = new KeyedMapBundleOperator(function, trigger);
		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
		testHarness.open();
		testHarness.setup();

		testHarness.processElement(record("key1", 1L));
		testHarness.processElement(record("key2", 1L));
		testHarness.processElement(record("key1", 3L));

		//trigger expired state cleanup
		testHarness.setStateTtlProcessingTime(20);
		testHarness.processElement(record("key1", 4L));
		testHarness.processElement(record("key1", 5L));
		testHarness.processElement(record("key2", 6L));

		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(record("key1", 4L));
		expectedOutput.add(record("key2", 1L));
		//result doesn`t contain expired record with the same key
		expectedOutput.add(record("key1", 9L));
		expectedOutput.add(record("key2", 6L));

		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
