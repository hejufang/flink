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

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link InMemoryGroupAggFunction}.
 */
public class InMemoryGroupAggFunctionTest extends GroupAggFunctionTestBase {
	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
		InMemoryGroupAggFunction aggFunction) throws Exception {
		KeyedProcessOperator<RowData, RowData, RowData> operator = new KeyedProcessOperator<>(aggFunction);
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}

	private InMemoryGroupAggFunction createFunction() {
		return new InMemoryGroupAggFunction(
			function,
			-1);
	}

	@Test
	public void testGroupAggWithoutSnapshot() throws Exception {
		InMemoryGroupAggFunction groupAggFunction = createFunction();
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(groupAggFunction);
		testHarness.open();
		testHarness.setup();

		testHarness.processElement(insertRecord("key1", 1, 20L));
		testHarness.processElement(insertRecord("key1", 2, 0L));
		testHarness.processElement(insertRecord("key1", 3, 999L));

		testHarness.processElement(insertRecord("key2", 1, 3999L));
		testHarness.processElement(insertRecord("key2", 2, 3000L));
		testHarness.processElement(insertRecord("key2", 3, 1000L));

		List<Object> expectedOutput = new ArrayList<>();
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}

	@Test
	public void testGroupAggAfterSnapshot() throws Exception {
		InMemoryGroupAggFunction groupAggFunction = createFunction();
		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(groupAggFunction);
		testHarness.open();
		testHarness.setup();
		// batch 1
		testHarness.processElement(insertRecord("key1", 1, 20L));
		testHarness.processElement(insertRecord("key1", 2, 0L));

		testHarness.processElement(insertRecord("key2", 1, 3999L));
		testHarness.processElement(insertRecord("key2", 2, 3000L));

		testHarness.processElement(insertRecord("key1", 3, 999L));

		testHarness.prepareSnapshotPreBarrier(0);
		List<Object> expectedOutput = new ArrayList<>();
		expectedOutput.add(insertRecord("key1", 6L, 3L));
		expectedOutput.add(insertRecord("key2", 3L, 2L));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		// batch 2
		testHarness.processElement(insertRecord("key1", 2, 20L));
		testHarness.processElement(insertRecord("key2", 1, 3999L));

		testHarness.prepareSnapshotPreBarrier(1);
		expectedOutput.add(insertRecord("key1", 2L, 1L));
		expectedOutput.add(insertRecord("key2", 1L, 1L));
		assertor.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
		testHarness.close();
	}
}
