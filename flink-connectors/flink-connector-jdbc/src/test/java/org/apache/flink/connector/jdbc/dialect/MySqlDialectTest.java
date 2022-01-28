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

package org.apache.flink.connector.jdbc.dialect;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for MySqlDialect.
 */
public class MySqlDialectTest {
	@Test
	public void testUpsertStatement() {
		MySQLDialect mySQLDialect = new MySQLDialect();
		String upsertStatement = mySQLDialect.getUpsertStatement(
			"my_table",
			new String[]{"col1", "col2", "col3", "col4", "col5"},
			new String[]{"col2", "col4"}
		).get();
		String expected = "INSERT INTO `my_table`(`col1`, `col2`, `col3`, `col4`, `col5`) " +
			"VALUES (?, ?, ?, ?, ?) " +
			"ON DUPLICATE KEY UPDATE " +
			"`col1`=VALUES(`col1`), `col3`=VALUES(`col3`), `col5`=VALUES(`col5`)";
		Assert.assertEquals(expected, upsertStatement);
	}

	@Test
	public void testUpdateByColumnStatement() {
		MySQLDialect mySQLDialect = new MySQLDialect();
		String upsertStatement = mySQLDialect.getUpdateByConditionStatement(
			"my_table",
			new String[]{"col1", "col2", "col3", "col4", "col5"},
			new String[]{"col2", "col4"}
		).get();
		String expected = "UPDATE my_table " +
			"SET col1 = ?, col2 = ?, col3 = ?, col4 = ?, col5 = ? " +
			"WHERE col2 = ?, col4 = ?";
		Assert.assertEquals(expected, upsertStatement);
	}
}
