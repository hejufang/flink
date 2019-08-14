/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Types;

/**
 * Testing ClickHouseOutputFormat.
 */
public class ClickHouseOutputFormatTest {
	private ClickHouseOutputFormat clickHouseOutputFormat;

	private static final String CH_DB_URL = "jdbc:clickhouse://10.11.182.139:8123/";
	private static final String CH_DRIVER_CLASS = "ru.yandex.clickhouse.ClickHouseDriver";
	private static final String CH_DRIVER_CLASS2 = "com.github.housepower.jdbc.ClickHouseDriver";

	private static final String CH_DB_NAME = "dts_tst";
	private static final String CH_TABLE_NAME = "test_update";

	@After
	public void tearDown() throws IOException {
		if (clickHouseOutputFormat != null) {
			clickHouseOutputFormat.close();
		}
		clickHouseOutputFormat = null;
	}

	@Test
	public void test_insert() throws IOException {
		String username = "";
		String password = "";
		String[] columnNames = new String[]{"user_id", "date"};

		clickHouseOutputFormat = ClickHouseOutputFormat.buildClickHouseOutputFormat()
			.setDrivername(CH_DRIVER_CLASS2)
			.setDbURL(CH_DB_URL)
			.setUserName(username)
			.setPassword(password)
			.setDbName(CH_DB_NAME)
			.setTableName(CH_TABLE_NAME)
			.setPrimaryKey(new String[]{"user_id"})
			.setColumnNames(columnNames)
			.setSqlTypes(new int[]{
				Types.INTEGER,
				Types.DATE,

			})
			.build();

		clickHouseOutputFormat.open(0, 0);
		Row row = new Row(3);
		row.setField(0, 777);
		row.setField(1, new Date(System.currentTimeMillis()));
		// 最后一列是binlog事件类型
		row.setField(2, "insert");
		clickHouseOutputFormat.writeRecord(row);

		clickHouseOutputFormat.close();

	}

	@Test
	public void test_delete() throws IOException {
		String username = "";
		String password = "";

		String[] columnNames = new String[]{"user_id", "date"};

		clickHouseOutputFormat = ClickHouseOutputFormat.buildClickHouseOutputFormat()
			.setDrivername(CH_DRIVER_CLASS)
			.setDbURL(CH_DB_URL)
			.setUserName(username)
			.setPassword(password)
			.setDbName(CH_DB_NAME)
			.setTableName(CH_TABLE_NAME)
			.setPrimaryKey(new String[]{"date", "user_id"})
			.setColumnNames(columnNames)
			.setSqlTypes(new int[]{
				Types.INTEGER,
				Types.DATE,

			})
			.build();

		clickHouseOutputFormat.open(0, 0);
		Row row = new Row(3);
		row.setField(0, 777);
		row.setField(1, new Date(System.currentTimeMillis()));
		row.setField(2, "delete");
		clickHouseOutputFormat.writeRecord(row);

		clickHouseOutputFormat.close();

	}

	@Test
	public void test_update() throws IOException {
		String username = "";
		String password = "";

		String[] columnNames = new String[]{"user_id", "date"};

		clickHouseOutputFormat = ClickHouseOutputFormat.buildClickHouseOutputFormat()
			.setDrivername(CH_DRIVER_CLASS2)
			.setDbURL(CH_DB_URL)
			.setUserName(username)
			.setPassword(password)
			.setDbName(CH_DB_NAME)
			.setTableName(CH_TABLE_NAME)
			.setPrimaryKey(new String[]{"date"})
			.setColumnNames(columnNames)
			.setSqlTypes(new int[]{
				Types.INTEGER,
				Types.DATE,

			})
			.build();

		clickHouseOutputFormat.open(0, 0);
		Row row = new Row(3);
		row.setField(0, 333);
		row.setField(1, new Date(System.currentTimeMillis()));
		row.setField(2, "update");
		clickHouseOutputFormat.writeRecord(row);

		clickHouseOutputFormat.close();

	}
}
