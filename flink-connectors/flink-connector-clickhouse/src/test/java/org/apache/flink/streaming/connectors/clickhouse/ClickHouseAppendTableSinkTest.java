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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Testing ClickHouseAppendTableSink.
 */
public class ClickHouseAppendTableSinkTest {
	private static final String[] FIELD_NAMES = new String[]{"id", "user_name", "auth", "executeTime", "type"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
		BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
	};
	private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

	@Test
	public void testAppendTableSink() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		List<Row> rows = genRows();

		DataStream<Row> ds = env.fromCollection(rows, ROW_TYPE);

		// convert DataStream to Table
		Table table = tEnv.fromDataStream(ds, "id, user_name, auth, executeTime, type");
		tEnv.registerTable("result", table);

		// sink ddl
		String sinkDDL = "create table priest_user_test(\n" +
			"    id int, \n" +
			"    user_name varchar, \n" +
			"    auth int, \n" +
			"    executeTime bigint, \n" +
			"    type varchar \n" +
			") with (\n" +
			"connector.type = 'clickhouse',\n" +
			"connector.driver = 'ru.yandex.clickhouse.ClickHouseDriver', \n" +
			"connector.url = 'jdbc:clickhouse://10.11.182.139:8123/', \n" +
			"connector.db = 'dts_tst', \n" +
			"`connector.table` = 'priest_user_test_local', \n" +
			"`connector.table.primary-key` = 'id', \n" +
			"`connector.username` = '', \n" +
			"`connector.password` = '' \n" +
			")";

		tEnv.sqlUpdate(sinkDDL);

		String sinkDML = "insert into priest_user_test SELECT id, user_name, auth,executeTime, type FROM `result` WHERE"
			+ " type in ('insert', 'update', 'delete')";
		tEnv.sqlUpdate(sinkDML);
		tEnv.execute("clickhouse jdbc output example");

	}

	private List<Row> genRows() {
		List<Row> list = new ArrayList<>();
		Row r1 = Row.of(777, "aaa", 0, 1563261863000L, "insert");
		Row r2 = Row.of(222, "bbb", 0, 1563261864000L, "insert");
		Row r3 = Row.of(222, "bbb", 0, 1563261865000L, "delete");
		Row r4 = Row.of(777, "ccc", 1, 1563261866000L, "update");
		list.add(r1);
		list.add(r2);
		list.add(r3);
		list.add(r4);
		return list;
	}
}
