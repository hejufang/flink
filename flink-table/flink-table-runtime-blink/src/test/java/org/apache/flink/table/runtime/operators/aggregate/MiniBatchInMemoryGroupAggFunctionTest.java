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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link MiniBatchInMemoryGlobalGroupAggFunction}.
 */
public class MiniBatchInMemoryGroupAggFunctionTest extends GroupAggFunctionTestBase {

	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
			MiniBatchInMemoryGlobalGroupAggFunction aggFunction) throws Exception {
		CountBundleTrigger<Tuple2<String, String>> trigger = new CountBundleTrigger<>(3);
		KeyedMapBundleOperator operator = new KeyedMapBundleOperator(aggFunction, trigger);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}

	private MiniBatchInMemoryGlobalGroupAggFunction createFunction() {
		return new MiniBatchInMemoryGlobalGroupAggFunction(
			localFunction,
			function,
			-1);
	}

	@Test
	public void testGroupAggWithoutSnapshot() throws Exception {
		MiniBatchInMemoryGlobalGroupAggFunction groupAggFunction = createFunction();
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(groupAggFunction);
		testHarness.open();
		testHarness.setup();

		// these inputs are local agg results.
		testHarness.processElement(insertRecord("key1", 20L, 1L));
		testHarness.processElement(insertRecord("key1", 30L, 2L));
		testHarness.processElement(insertRecord("key1", 15L, 2L));

		testHarness.processElement(insertRecord("key2", 100L, 5L));
		testHarness.processElement(insertRecord("key2", 80L, 3L));
		testHarness.processElement(insertRecord("key2", 23L, 3L));

		List<Object> expectedOutput = new ArrayList<>();
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testGroupAggAfterSnapshot() throws Exception {
		MiniBatchInMemoryGlobalGroupAggFunction groupAggFunction = createFunction();
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(groupAggFunction);
		testHarness.open();
		testHarness.setup();
		// batch 1
		testHarness.processElement(insertRecord("key1", 20L, 1L));
		testHarness.processElement(insertRecord("key1", 30L, 2L));

		testHarness.processElement(insertRecord("key2", 100L, 5L));
		testHarness.processElement(insertRecord("key2", 80L, 3L));

		testHarness.processElement(insertRecord("key1", 15L, 2L));

		testHarness.prepareSnapshotPreBarrier(0);
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("key1", 65L, 5L));
		expectedOutput.add(insertRecord("key2", 180L, 8L));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		// batch 2
		testHarness.processElement(insertRecord("key1", 20L, 2L));
		testHarness.processElement(insertRecord("key2", 33L, 3L));

		testHarness.prepareSnapshotPreBarrier(1);
		expectedOutput.add(insertRecord("key1", 20L, 2L));
		expectedOutput.add(insertRecord("key2", 33L, 3L));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
