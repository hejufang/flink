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

package org.apche.flink.connector.bytetable.sink;

import org.apache.flink.connector.bytetable.sink.ByteTableSinkFunction;
import org.apache.flink.connector.bytetable.util.ByteArrayWrapper;
import org.apache.flink.connector.bytetable.util.ByteTableReduceUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ByteTableSinkFunction}.
 */
public class ByteTableSinkFunctionTest {

	@Test
	public void testMutateReduceWithCellVersion() throws Exception {
		// use same rowkey for in and out.
		byte[] rowkey = new byte[]{1};
		TimestampData timestampOut = TimestampData.fromEpochMillis(1);
		ByteArrayWrapper byteArrayWrapperOut = new ByteArrayWrapper(rowkey);
		GenericRowData rowOut = new GenericRowData(3);
		rowOut.setField(0, 1);
		rowOut.setField(1, timestampOut);
		rowOut.setField(2, "out2");
		ByteArrayWrapper byteArrayWrapperIn = new ByteArrayWrapper(rowkey);
		GenericRowData rowIn = new GenericRowData(3);
		TimestampData timestampIn = TimestampData.fromEpochMillis(2);
		rowIn.setField(0, 1);
		rowIn.setField(1, timestampIn);
		rowIn.setField(2, "In2");
		Map<ByteArrayWrapper, RowData> mutationInfoMap = new HashMap<>();
		// add first value to map.
		ByteTableReduceUtil.mutateReduce(1, byteArrayWrapperIn, rowIn, mutationInfoMap);
		for (Map.Entry<ByteArrayWrapper, RowData> entry : mutationInfoMap.entrySet()) {
			RowData row = entry.getValue();
			assertEquals(row, rowIn);
		}
		assertEquals(mutationInfoMap.size(), 1);
		// Add second value to map. Same rowkey, but older cellVersion.
		ByteTableReduceUtil.mutateReduce(1, byteArrayWrapperOut, rowOut, mutationInfoMap);
		// Check the cellVersion in mutationInfoMap.
		for (Map.Entry<ByteArrayWrapper, RowData> entry : mutationInfoMap.entrySet()) {
			RowData row = entry.getValue();
			// The second value should not overwrite the first one.
			assertEquals(row, rowIn);
		}
		assertEquals(mutationInfoMap.size(), 1);
		// Add third value to map. Different rowkey. So the size of mutationInfoMap should be 2.
		byte[] newRowkey = new byte[]{2};
		ByteArrayWrapper newByteArrayWrapperOut = new ByteArrayWrapper(newRowkey);
		GenericRowData newRow = new GenericRowData(3);
		TimestampData newTimestamp = TimestampData.fromEpochMillis(2);
		rowIn.setField(0, 1);
		rowIn.setField(1, newTimestamp);
		rowIn.setField(2, "newIn2");
		ByteTableReduceUtil.mutateReduce(1, newByteArrayWrapperOut, newRow, mutationInfoMap);
		assertEquals(mutationInfoMap.size(), 2);
	}

	@Test
	public void testMutateReduceWithoutCellVersion() {
		// use same rowkey for in and out.
		byte[] rowkey = new byte[]{1};
		ByteArrayWrapper byteArrayWrapperOut = new ByteArrayWrapper(rowkey);
		GenericRowData rowOut = new GenericRowData(2);
		rowOut.setField(0, 1);
		rowOut.setField(1, "out2");
		ByteArrayWrapper byteArrayWrapperIn = new ByteArrayWrapper(rowkey);
		GenericRowData rowIn = new GenericRowData(2);
		rowIn.setField(0, 1);
		rowIn.setField(1, "In2");
		Map<ByteArrayWrapper, RowData> mutationInfoMap = new HashMap<>();
		// add first value to map.
		ByteTableReduceUtil.mutateReduce(-1, byteArrayWrapperIn, rowIn, mutationInfoMap);
		assertEquals(mutationInfoMap.size(), 1);
		for (Map.Entry<ByteArrayWrapper, RowData> entry : mutationInfoMap.entrySet()) {
			RowData row = entry.getValue();
			assertEquals(row, rowIn);
		}

		// Add second value to map. Same rowkey.
		ByteTableReduceUtil.mutateReduce(-1, byteArrayWrapperOut, rowOut, mutationInfoMap);
		// Check the cellVersion in mutationInfoMap.
		assertEquals(mutationInfoMap.size(), 1);
		for (Map.Entry<ByteArrayWrapper, RowData> entry : mutationInfoMap.entrySet()) {
			RowData row = entry.getValue();
			// The second value should overwrite the first one.
			assertEquals(row, rowOut);
		}
		// Add third value to map. Different rowkey. So the size of mutationInfoMap should be 2.
		byte[] newRowkey = new byte[]{2};
		ByteArrayWrapper newByteArrayWrapperOut = new ByteArrayWrapper(newRowkey);
		GenericRowData newRow = new GenericRowData(2);
		rowIn.setField(0, 1);
		rowIn.setField(1, "newIn2");
		ByteTableReduceUtil.mutateReduce(-1, newByteArrayWrapperOut, newRow, mutationInfoMap);
		assertEquals(mutationInfoMap.size(), 2);
	}

}
