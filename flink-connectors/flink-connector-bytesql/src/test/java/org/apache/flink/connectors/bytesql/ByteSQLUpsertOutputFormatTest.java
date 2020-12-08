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

package org.apache.flink.connectors.bytesql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.types.Row;

import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ByteSQLUpsertOutputFormat}.
 */
public class ByteSQLUpsertOutputFormatTest {
	private ByteSQLUpsertOutputFormat byteSQLUpsertOutputFormat;
	private static final RowTypeInfo typeInfo = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.STRING, Types.STRING},
		new String[]{"id", "name", "text"});
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
		byteSQLUpsertOutputFormat = new ByteSQLUpsertOutputFormat(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			typeInfo.getFieldNames(),
			new int[]{0},
			false
		);
		String sql = byteSQLUpsertOutputFormat.generateUpsertSQLWithoutNull(Row.of(1, null, "x"));
		String expectedSQL = "INSERT INTO `test`(`id`, `text`) " +
			"VALUES (1, 'x') ON DUPLICATE KEY UPDATE `id`=VALUES(`id`), `text`=VALUES(`text`)";
		assertEquals(sql, expectedSQL);
	}

	@Test
	public void testMergeRow() {
		Row newRow = Row.of(1, null, 2);
		Row oldRow1 = null;
		Row expected1 = ByteSQLUpsertOutputFormat.mergeRow(newRow, oldRow1);
		assertEquals(Row.of(1, null, 2), expected1);
		Row oldRow2 = Row.of(1, null, 3);
		Row expected2 = ByteSQLUpsertOutputFormat.mergeRow(newRow, oldRow2);
		assertEquals(Row.of(1, null, 2), expected2);
		Row oldRow3 = Row.of(null, 2, 3);
		Row expected3 = ByteSQLUpsertOutputFormat.mergeRow(newRow, oldRow3);
		assertEquals(Row.of(1, 2, 2), expected3);
	}

	@Test
	public void testAddRow() {
		insertOptionsBuilder.setIgnoreNull(true);
		insertOptionsBuilder.setBufferFlushMaxRows(Integer.MAX_VALUE);
		byteSQLUpsertOutputFormat = new ByteSQLUpsertOutputFormat(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			typeInfo.getFieldNames(),
			new int[]{0},
			false
		);
		List<Tuple2<Boolean, Row>> recordBuffer = new ArrayList<>();
		Map<Row, Tuple2<Boolean, Row>> keyToRows = new HashMap<>();
		Map<Row, Tuple2<Boolean, Row>> expectedMap = new HashMap<>();
		//test update
		recordBuffer.add(new Tuple2<>(true, Row.of(0, "a", "b")));
		recordBuffer.forEach(byteSQLUpsertOutputFormat.addRow(keyToRows));
		expectedMap.put(Row.of(0), new Tuple2<>(false, Row.of(0, "a", "b")));
		assertEquals(expectedMap, keyToRows);
		//test delete
		recordBuffer.add(new Tuple2<>(false, Row.of(0, "a", "b")));
		recordBuffer.forEach(byteSQLUpsertOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(Row.of(0), new Tuple2<>(true, null));
		assertEquals(expectedMap, keyToRows);
		//test delete, update
		recordBuffer.add(new Tuple2<>(true, Row.of(0, null, "b")));
		recordBuffer.forEach(byteSQLUpsertOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(Row.of(0), new Tuple2<>(true, Row.of(0, null, "b")));
		assertEquals(expectedMap, keyToRows);
		//test delete, update, update
		recordBuffer.add(new Tuple2<>(true, Row.of(0, "a", "b")));
		recordBuffer.forEach(byteSQLUpsertOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(Row.of(0), new Tuple2<>(true, Row.of(0, "a", "b")));
		assertEquals(expectedMap, keyToRows);
		//test delete, update, update, delete
		recordBuffer.add(new Tuple2<>(false, Row.of(0, "a", "b")));
		recordBuffer.forEach(byteSQLUpsertOutputFormat.addRow(keyToRows));
		expectedMap.clear();
		expectedMap.put(Row.of(0), new Tuple2<>(true, null));
		assertEquals(expectedMap, keyToRows);
	}
}
