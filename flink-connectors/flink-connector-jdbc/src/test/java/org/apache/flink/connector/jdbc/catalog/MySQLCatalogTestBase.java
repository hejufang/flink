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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_BIGINT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_BIGINT_UNSIGNED;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_BIT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_CHAR;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_DATE;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_DATETIME;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_DECIMAL;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_DOUBLE;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_FLOAT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_INT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_INT_UNSIGNED;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_LONGTEXT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_MEDIUMINT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_MEDIUMTEXT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_SMALLINT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_SMALLINT_UNSIGNED;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TEXT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TIME;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TIMESTAMP;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TINYINT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TINYINT_UNSIGNED;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_TINYTEXT;
import static org.apache.flink.connector.jdbc.catalog.MySQLCatalog.MYSQL_VARCHAR;

/**
 * Test base for {@link MySQLCatalog}.
 */
public class MySQLCatalogTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	protected static final String TEST_CATALOG_NAME = "mysql";
	protected static final String TEST_WRONG_BASE_URL = "jdbc:mysql://localhost:3306/";
	protected static final String TEST_USERNAME = "root";
	protected static final String TEST_PWD = "root";
	protected static final String TEST_FETCH_SIZE = "10000";
	protected static final String TEST_DB = "test_01";
	protected static final String DEFAULT_DB = "test";
	protected static final String TEST_TABLE1 = "t1";
	protected static final String TEST_TABLE2 = "t2";
	protected static final String TEST_TABLE3 = "t3";
	protected static final String TEST_TABLE4 = "t4";
	protected static final String TEST_TABLE5 = "t5";

	protected static final String TEST_NUMERIC_TABLE = "t_numeric";
	protected static final String TEST_CHAR_TABLE = "t_char";
	protected static final String TEST_TIME_TABLE = "t_time";
	protected static final String TEST_UNSUPPORTED_TABLE = "t_unsupported";

	protected static MySQLCatalog catalog;
	protected static MySQLCatalog invalidCatalog;

	protected static DB db;

	protected static int port;
	protected static String baseUrl;

	private static int tryStartDb(int portStart, int portEnd) {
		if (portStart > portEnd) {
			throw new RuntimeException("start db failed!");
		}

		try {
			db = DB.newEmbeddedDB(portStart);
			db.start();

			return portStart;
		} catch (ManagedProcessException e) {
			return tryStartDb(portStart + 1, portEnd);
		}
	}

	@BeforeClass
	public static void init() throws SQLException {
		port = tryStartDb(33306, 33316);
		baseUrl = "jdbc:mysql://localhost:" + port + "/";

		catalog = new MySQLCatalog(TEST_CATALOG_NAME, DEFAULT_DB, TEST_USERNAME, TEST_PWD, TEST_FETCH_SIZE,
			new MySQLURL.Builder(baseUrl).build());
		invalidCatalog = new MySQLCatalog(TEST_CATALOG_NAME, DEFAULT_DB, TEST_USERNAME, TEST_PWD, TEST_FETCH_SIZE,
			new MySQLURL.Builder(TEST_WRONG_BASE_URL).build());

		createTable(TEST_TABLE1, getSimpleTable().schemaSql);
		createTable(TEST_TABLE4, getSimpleTable().schemaSql);
		createTable(TEST_TABLE5, getSimpleTable().schemaSql);

		createDatabase(TEST_DB);

		createTable(TEST_DB, TEST_TABLE2, getSimpleTable().schemaSql);
		createTable(TEST_DB, TEST_TABLE3, getSimpleTable().schemaSql);

		executeSql(DEFAULT_DB, String.format("insert into %s values (%s);", TEST_TABLE1, getSimpleTable().values));

		createTable(TEST_DB, TEST_NUMERIC_TABLE, getNumericTable().schemaSql);
		executeSql(TEST_DB, String.format("insert into %s values (%s);",
			TEST_NUMERIC_TABLE,
			getNumericTable().values));

		createTable(TEST_DB, TEST_CHAR_TABLE, getCharTable().schemaSql);
		executeSql(TEST_DB, String.format("insert into %s values (%s);", TEST_CHAR_TABLE, getCharTable().values));

		createTable(TEST_DB, TEST_TIME_TABLE, getTimeTable().schemaSql);
		executeSql(TEST_DB, String.format("insert into %s values (%s);", TEST_TIME_TABLE, getTimeTable().values));

		createTable(TEST_DB, TEST_UNSUPPORTED_TABLE, getUnsupportedTable().schemaSql);
		executeSql(TEST_DB, String.format("insert into %s values (%s);",
			TEST_UNSUPPORTED_TABLE,
			getUnsupportedTable().values));
	}

	@AfterClass
	public static void close() throws ManagedProcessException {
		db.stop();
	}

	public static void createDatabase(String database) throws SQLException {
		executeSql(String.format("CREATE DATABASE %s;", database));
	}

	public static void createTable(String table, String schemaSql) throws SQLException {
		executeSql(DEFAULT_DB, String.format("CREATE TABLE %s(%s);", table, schemaSql));
	}

	public static void createTable(String database, String table, String schemaSql) throws SQLException {
		executeSql(database, String.format("CREATE TABLE %s(%s);", table, schemaSql));
	}

	public static void executeSql(String sql) throws SQLException {
		executeSql("", sql);
	}

	public static void executeSql(String db, String sql) throws SQLException {
		try (Connection conn = DriverManager.getConnection(baseUrl + db, TEST_USERNAME, TEST_PWD)) {
			Statement statement = conn.createStatement();
			statement.executeUpdate(sql);
		}
	}

	/**
	 * Test table for {@link MySQLCatalog}.
	 */
	public static class TestTable {
		TableSchema schema;
		String schemaSql;
		String values;

		public TestTable(TableSchema schema, String schemaSql, String values) {
			this.schema = schema;
			this.schemaSql = schemaSql;
			this.values = values;
		}
	}

	public static MySQLCatalogTestBase.TestTable getUnsupportedTable() {
		return new MySQLCatalogTestBase.TestTable(
			TableSchema.builder()
				.fields(
					new String[] { "value" },
					new DataType[] { DataTypes.NULL() },
					new String[] { "BIT" }
				)
				.build(),
			"value bit(8)",
			"1"
		);
	}

	public static MySQLCatalogTestBase.TestTable getSimpleTable() {
		return new MySQLCatalogTestBase.TestTable(
			TableSchema.builder()
				.fields(
					new String[] { "id", "sex" },
					new DataType[] { DataTypes.INT().notNull(), DataTypes.TINYINT().notNull() },
					new String[] { MYSQL_INT, MYSQL_BIT }
				)
				.primaryKey("PRIMARY", new String[] {"id"})
				.build(),
			"id INT PRIMARY KEY," +
				"sex TINYINT(1) NOT NULL",
			"1, 1"
		);
	}

	public static MySQLCatalogTestBase.TestTable getTimeTable() {
		return new MySQLCatalogTestBase.TestTable(
			TableSchema.builder()
				.fields(
					new String[] { "date_t", "time_t", "datetime_t", "timestamp_t" },
					new DataType[] { DataTypes.DATE(),
						new AtomicDataType(new TimeType(), LocalTime.class),
						new AtomicDataType(new TimestampType(TimestampType.MAX_PRECISION), LocalDateTime.class),
						new AtomicDataType(new TimestampType(TimestampType.MAX_PRECISION), Timestamp.class).notNull() },
					new String[] { MYSQL_DATE, MYSQL_TIME, MYSQL_DATETIME, MYSQL_TIMESTAMP }
				)
				.build(),
			"`date_t` DATE," +
				"`time_t` TIME," +
				"`datetime_t` DATETIME," +
				"`timestamp_t` TIMESTAMP",
			"'2022-01-01', '22:00:00', '2022-01-01 22:00:00', '2022-01-01 22:00:00'"
		);
	}

	public static MySQLCatalogTestBase.TestTable getCharTable() {
		return new MySQLCatalogTestBase.TestTable(
			TableSchema.builder()
				.fields(
					new String[] { "char_t", "varchar_t", "tinytext_t", "text_t", "mediumtext_t", "longtext_t"},
					new DataType[] { DataTypes.VARCHAR(50), DataTypes.VARCHAR(100), DataTypes.STRING(),
						DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()},
					new String[] { MYSQL_CHAR, MYSQL_VARCHAR, MYSQL_TINYTEXT,
						MYSQL_TEXT, MYSQL_MEDIUMTEXT, MYSQL_LONGTEXT}
				)
				.build(),
			"`char_t` CHAR(50)," +
				"`varchar_t` VARCHAR(100)," +
				"`tinytext_t` TINYTEXT," +
				"`text_t` TEXT," +
				"`mediumtext_t` MEDIUMTEXT," +
				"`longtext_t` LONGTEXT",
			"'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext'"
		);
	}

	public static MySQLCatalogTestBase.TestTable getNumericTable() {
		return new MySQLCatalogTestBase.TestTable(
			TableSchema.builder()
				.fields(
					new String[] { "boolean_t", "tinyint_t", "smallint_t", "u_tinyint_t",
						"int_t", "integer_t", "mediumint_t",
						"u_smallint_t", "bigint_t", "u_int_t",
						"u_bigint_t", "float_t", "double_t", "double_precision_t", "numeric_t", "decimal_t" },
					new DataType[] { DataTypes.TINYINT(), DataTypes.TINYINT(), DataTypes.SMALLINT(),
						DataTypes.SMALLINT(),
						DataTypes.INT(), DataTypes.INT(), DataTypes.INT(),
						DataTypes.INT(), DataTypes.BIGINT(), DataTypes.BIGINT(),
						DataTypes.DECIMAL(20, 0), DataTypes.FLOAT(),
						DataTypes.DOUBLE(), DataTypes.DOUBLE(),
						DataTypes.DECIMAL(5, 2), DataTypes.DECIMAL(5, 2) },
					new String[] { MYSQL_BIT, MYSQL_TINYINT, MYSQL_SMALLINT,
						MYSQL_TINYINT_UNSIGNED, MYSQL_INT, MYSQL_INT, MYSQL_MEDIUMINT,
						MYSQL_SMALLINT_UNSIGNED, MYSQL_BIGINT, MYSQL_INT_UNSIGNED,
						MYSQL_BIGINT_UNSIGNED, MYSQL_FLOAT, MYSQL_DOUBLE, MYSQL_DOUBLE,
						MYSQL_DECIMAL, MYSQL_DECIMAL  }
				)
				.build(),
			"`boolean_t` TINYINT(1)," +
				"`tinyint_t` TINYINT(3)," +
				"`smallint_t` smallint(6)," +
				"`u_tinyint_t` TINYINT unsigned," +
				"`int_t` INT," +
				"`integer_t` integer," +
				"`mediumint_t` MEDIUMINT," +
				"`u_smallint_t` smallint UNSIGNED," +
				"`bigint_t` BIGINT," +
				"`u_int_t` INT UNSIGNED," +
				"`u_bigint_t` bigint(20) unsigned," +
				"`float_t` FLOAT," +
				"`double_t` DOUBLE," +
				"`double_precision_t` DOUBLE PRECISION," +
				"`numeric_t` NUMERIC(5,2)," +
				"`decimal_t` DECIMAL(5,2)",
			"1,2,3,4,5,6,7,8,9,10,11,123.1,123.12,123.12,999.99,999.99"
		);
	}
}
