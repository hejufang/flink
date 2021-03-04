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

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ByteSQLOutputFormat}.
 */
public class ByteSQLOutputFormatTest {
	private ByteSQLOutputFormat byteSQLOutputFormat;
	private static String[] fieldNames = new String[] {"id", "name", "text"};
	StringData constantStringA = StringData.fromString("a");
	StringData constantStringB = StringData.fromString("b");
	private static DataType[] fieldDataTypes = new DataType[] {
		DataTypes.INT(),
		DataTypes.STRING(),
		DataTypes.STRING()
	};
	RowType rowType = RowType.of(Arrays.stream(fieldDataTypes).
		map(DataType::getLogicalType).toArray(LogicalType[]::new), fieldNames);
	private static final ByteSQLOptions.Builder optionBuilder = ByteSQLOptions.builder();
	private final ByteSQLInsertOptions.Builder insertOptionsBuilder = ByteSQLInsertOptions.builder();
	static {
		optionBuilder
			.setConsul("dummy")
			.setDatabaseName("test")
			.setTableName("test")
			.setUsername("test")
			.setPassword("test");
	}

	@Test
	public void testGenerateUpsertSQLWithoutNull() throws ByteSQLException {
		insertOptionsBuilder.setIgnoreNull(true);
		insertOptionsBuilder.setBufferFlushMaxRows(Integer.MAX_VALUE);
		insertOptionsBuilder.setKeyFields(new String[]{"id"});
		byteSQLOutputFormat = new ByteSQLOutputFormat(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			rowType,
			false
		);
		String sql = byteSQLOutputFormat
			.generateUpsertSQLWithoutNull(GenericRowData.of(1, null, StringData.fromString("x")));
		String expectedSQL = "INSERT INTO `test`(`id`, `text`) " +
			"VALUES (1, 'x') ON DUPLICATE KEY UPDATE `id`=VALUES(`id`), `text`=VALUES(`text`)";
		assertEquals(sql, expectedSQL);
	}

	@Test
	public void testMergeRow() {
		insertOptionsBuilder.setKeyFields(new String[]{"id"});
		byteSQLOutputFormat = new ByteSQLOutputFormat(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			rowType,
			false
		);
		RowData newRow = GenericRowData.of(1, null, constantStringB);
		RowData actual1 = byteSQLOutputFormat.mergeRow(newRow, null);
		assertEquals(GenericRowData.of(1, null, constantStringB), actual1);
		RowData oldRow2 = GenericRowData.of(1, null, constantStringA);
		RowData actual2 = byteSQLOutputFormat.mergeRow(newRow, oldRow2);
		assertEquals(GenericRowData.of(1, null, constantStringB), actual2);
		RowData oldRow3 = GenericRowData.of(null, constantStringB, constantStringA);
		RowData actual3 = byteSQLOutputFormat.mergeRow(newRow, oldRow3);
		assertEquals(GenericRowData.of(1, constantStringB, constantStringB), actual3);
	}

	@Test
	public void testAddRow() {
		insertOptionsBuilder.setIgnoreNull(true);
		insertOptionsBuilder.setBufferFlushMaxRows(Integer.MAX_VALUE);
		insertOptionsBuilder.setKeyFields(new String[]{"id"});
		byteSQLOutputFormat = new ByteSQLOutputFormat(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			rowType,
			false
		);
		List<RowData> recordBuffer = new ArrayList<>();
		Map<RowData, Tuple2<Boolean, RowData>> keyToRows = new HashMap<>();
		Map<RowData, Tuple2<Boolean, RowData>> expectedMap = new HashMap<>();
		//test insert
		recordBuffer.add(GenericRowData.ofKind(RowKind.INSERT, 0, constantStringA, constantStringB));
		recordBuffer.forEach(byteSQLOutputFormat.addRow(keyToRows));
		expectedMap.put(GenericRowData.of(0), new Tuple2<>(false, GenericRowData.of(0,
			constantStringA, constantStringB)));
		assertEquals(expectedMap, keyToRows);
		//test delete
		recordBuffer.add(GenericRowData.ofKind(RowKind.DELETE, 0, constantStringA, constantStringB));
		recordBuffer.forEach(byteSQLOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(GenericRowData.of(0), new Tuple2<>(true, null));
		assertEquals(expectedMap, keyToRows);
		//test delete, update
		recordBuffer.add(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, null, constantStringB));
		recordBuffer.forEach(byteSQLOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(GenericRowData.of(0), new Tuple2<>(true,
			GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, null, constantStringB)));
		assertEquals(expectedMap, keyToRows);
		//test delete, update, update
		recordBuffer.add(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, constantStringA, constantStringB));
		recordBuffer.forEach(byteSQLOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(GenericRowData.of(0), new Tuple2<>(true,
			GenericRowData.ofKind(RowKind.UPDATE_AFTER, 0, constantStringA, constantStringB)));
		assertEquals(expectedMap, keyToRows);
		//test delete, update, update, delete
		recordBuffer.add(GenericRowData.ofKind(RowKind.DELETE, 0, constantStringA, constantStringB));
		recordBuffer.forEach(byteSQLOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(GenericRowData.of(0), new Tuple2<>(true, null));
		assertEquals(expectedMap, keyToRows);
	}

	@Test
	public void testGetKeyToRowsValue() {
		// insert message
		RowData input1 = GenericRowData.ofKind(RowKind.INSERT, 0, "a", "b");
		Tuple2<Boolean, RowData> outputTuple1 = ByteSQLOutputFormat.getKeyToRowsValue(input1);
		Tuple2<Boolean, RowData> expectedTuple1 = new Tuple2<>(false,
			GenericRowData.ofKind(RowKind.INSERT, 0, "a", "b"));
		assertEquals(expectedTuple1, outputTuple1);
		// delete message
		RowData input2 = GenericRowData.ofKind(RowKind.DELETE, 0, "a", "b");
		Tuple2<Boolean, RowData> outputTuple2 = ByteSQLOutputFormat.getKeyToRowsValue(input2);
		Tuple2<Boolean, RowData> expectedTuple2 = new Tuple2<>(true, null);
		assertEquals(expectedTuple2, outputTuple2);
	}
}
