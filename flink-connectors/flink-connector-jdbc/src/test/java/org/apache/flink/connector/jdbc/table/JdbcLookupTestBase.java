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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.JdbcTestFixture;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;

/**
 * Base class for JDBC lookup test.
 */
public class JdbcLookupTestBase extends AbstractTestBase {

	public static final String DB_URL = "jdbc:derby:memory:lookup";
	public static final String LOOKUP_TABLE = "lookup_table";

	@Before
	public void before() throws ClassNotFoundException, SQLException {
		System.setProperty("derby.stream.error.field", JdbcTestFixture.class.getCanonicalName() + ".DEV_NULL");

		Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
		try (
			Connection conn = DriverManager.getConnection(DB_URL + ";create=true");
			Statement stat = conn.createStatement()) {
			stat.executeUpdate("CREATE TABLE " + LOOKUP_TABLE + " (" +
					"id1 INT NOT NULL DEFAULT 0," +
					"id2 VARCHAR(20) NOT NULL," +
					"comment1 VARCHAR(1000)," +
					"comment2 VARCHAR(1000)," +
					"boolean_field BOOLEAN DEFAULT TRUE," +
					"smallint_field SMALLINT," +
					"int_field INT DEFAULT 2," +
					"bigint_field BIGINT DEFAULT 1," +
					"float_field FLOAT," +
					"double_field DOUBLE," +
					"decimal_field DECIMAL(30, 10)," +
					"real_field REAL," +
					"numeric_field NUMERIC," +
					"char_field CHAR(5)," +
					"date_field DATE," +
					"time_field TIME," +
					"timestamp_field TIMESTAMP)");

			Object[][] data = new Object[][] {
					new Object[] {1, "1", "11-c1-v1", "11-c2-v1", "true", "12", "12345", "12345678", "123.456", "123.456789", "123456789.987654321", "654.321", "135.246", "aaaaa", "2021-12-06", "20:12:35", "2021-12-06 20:12:35"},
					new Object[] {1, "1", "11-c1-v2", "11-c2-v2", "true", "12", "12345", "12345678", "123.456", "123.456789", "123456789.987654321", "654.321", "135.246", "bbbbb", "2021-12-06", "20:12:35", "2021-12-06 20:12:35"},
					new Object[] {2, "3", null, "23-c2", "true", "12", "12345", "12345678", "123.456", "123.456789", "123456789.987654321", "654.321", "135.246", "ccccc", "2021-12-06", "20:12:35", "2021-12-06 20:12:35"},
					new Object[] {2, "5", "25-c1", "25-c2", "true", "12", "12345", "12345678", "123.456", "123.456789", "123456789.987654321", "654.321", "135.246", "ddddd", "2021-12-06", "20:12:35", "2021-12-06 20:12:35"},
					new Object[] {3, "8", "38-c1", "38-c2", "true", "12", "12345", "12345678", "123.456", "123.456789", "123456789.987654321", "654.321", "135.246", "eeeee", "2021-12-06", "20:12:35", "2021-12-06 20:12:35"}
			};
			boolean[] surroundedByQuotes = new boolean[] {
				false, true, true, true, false, false, false, false, false, false, false, false, false, true, true, true, true
			};

			StringBuilder sqlQueryBuilder = new StringBuilder(
					"INSERT INTO " + LOOKUP_TABLE + " (id1, id2, comment1, comment2, boolean_field, smallint_field, " +
						"int_field, bigint_field, float_field, double_field, decimal_field, real_field, " +
						"numeric_field, char_field, date_field, time_field, timestamp_field) VALUES ");
			for (int i = 0; i < data.length; i++) {
				sqlQueryBuilder.append("(");
				for (int j = 0; j < data[i].length; j++) {
					if (data[i][j] == null) {
						sqlQueryBuilder.append("null");
					} else {
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
						sqlQueryBuilder.append(data[i][j]);
						if (surroundedByQuotes[j]) {
							sqlQueryBuilder.append("'");
						}
					}
					if (j < data[i].length - 1) {
						sqlQueryBuilder.append(", ");
					}
				}
				sqlQueryBuilder.append(")");
				if (i < data.length - 1) {
					sqlQueryBuilder.append(", ");
				}
			}
			stat.execute(sqlQueryBuilder.toString());
		}
	}

	@After
	public void clearOutputTable() throws Exception {
		Class.forName(DERBY_EBOOKSHOP_DB.getDriverClass());
		try (
				Connection conn = DriverManager.getConnection(DB_URL);
				Statement stat = conn.createStatement()) {
			stat.execute("DROP TABLE " + LOOKUP_TABLE);
		}
	}
}
