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

import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.table.data.binary.BinaryStringData;

import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ByteSQLUtils}.
 */
public class ByteSQLUtilsTest {

	@Test
	public void testGenerateActualSQLForSelect() throws ByteSQLException {
		List<String> selectFields = new ArrayList<>();
		selectFields.add("a");
		List<String> conditionFields = new ArrayList<>();
		conditionFields.add("b");
		String sqlQuery = ByteSQLUtils
			.getSelectFromStatement("testTable", selectFields, conditionFields);
		String actualSQL = ByteSQLUtils
			.generateActualSql(sqlQuery, new Object[]{new BinaryStringData("testVal")});
		String expectedSQL = "SELECT a FROM testTable WHERE b='testVal'";
		assertEquals(expectedSQL, actualSQL);
	}

	@Test
	public void testGenerateActualSQLForInsert() throws ByteSQLException {
		String[] fieldNames = new String[]{"a", "b"};
		String sqlQuery = ByteSQLUtils
			.getUpsertStatement("testTable", fieldNames, 0);
		String actualSQL = ByteSQLUtils
			.generateActualSql(sqlQuery, new Object[]{new BinaryStringData("testVal"), 200});
		String expectedSQL = "INSERT INTO `testTable`(`a`, `b`) VALUES ('testVal', 200) " +
			"ON DUPLICATE KEY UPDATE `a`=VALUES(`a`), `b`=VALUES(`b`)";
		assertEquals(expectedSQL, actualSQL);
	}

	@Test
	public void testGenerateActualSQLForDelete() throws ByteSQLException {
		String[] fieldNames = new String[]{"a"};
		String sqlQuery = ByteSQLUtils
			.getDeleteStatement("testTable", fieldNames);
		String actualSQL = ByteSQLUtils
			.generateActualSql(sqlQuery, new Object[]{new BinaryStringData("testVal")});
		String expectedSQL = "DELETE FROM `testTable` WHERE `a`='testVal'";
		assertEquals(expectedSQL, actualSQL);
	}
}
