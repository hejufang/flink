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

import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Testing ClickHouseAppendTableSink.
 */
@Ignore
public class ClickHouseAppendTableSinkTest {
	private Connection connection;
	// 测试用本地表连接信息
	private static final String LOCAL_CH_HOST = "10.11.182.107";
	private static final String LOCAL_CH_PORT = "8123";
	private static final String LOCAL_CH_DB_NAME = "dts_tst";
	private static final String LOCAL_CH_TABLE_WITH_NO_SIGN_COLUMN = "test_table_with_no_sign_column";
	private static final String LOCAL_CH_CREATE_TABLE_SQL = String.format("CREATE TABLE IF NOT EXISTS %s.%s " +
			" (`id` Int64, `user_name` Nullable(String), `auth` Nullable(Int64), `executeTime` Int64) ENGINE = TinyLog",
		LOCAL_CH_DB_NAME, LOCAL_CH_TABLE_WITH_NO_SIGN_COLUMN);
	private static final String LOCAL_CH_DROP_TABLE_SQL = String.format("DROP TABLE IF EXISTS %s.%s",
		LOCAL_CH_DB_NAME, LOCAL_CH_TABLE_WITH_NO_SIGN_COLUMN);

	private static final String[] FIELD_NAMES = new String[]{"id", "user_name", "auth", "executeTime", "type"};
	private static final TypeInformation[] FIELD_TYPES = new TypeInformation[]{
		BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
	};
	private static final RowTypeInfo ROW_TYPE = new RowTypeInfo(FIELD_TYPES, FIELD_NAMES);

	@Before
	public void setUp() throws Exception {
		ClickHouseProperties properties = new ClickHouseProperties();
		String url = String.format("jdbc:clickhouse://%s:%s", LOCAL_CH_HOST, LOCAL_CH_PORT);
		ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
		connection = dataSource.getConnection();
		connection.createStatement().execute(LOCAL_CH_CREATE_TABLE_SQL);
	}

	@After
	public void tearDown() throws Exception {
		connection.createStatement().execute(LOCAL_CH_DROP_TABLE_SQL);
	}

	private StreamTableEnvironment initTableEnv(List<Row> rows) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Row> ds = env.fromCollection(rows, ROW_TYPE);

		// convert DataStream to Table
		Table table = tEnv.fromDataStream(ds, "id, user_name, auth, executeTime, type");
		tEnv.registerTable("result", table);

		return tEnv;
	}

	@Test
	public void testChLocalTable() throws Exception {
		StreamTableEnvironment tEnv = initTableEnv(genRows());

		// sink ddl
		String sinkDDL = String.format("create table priest_user_test(\n" +
			"    id int, \n" +
			"    user_name varchar, \n" +
			"    auth int, \n" +
			"    executeTime bigint, \n" +
			"    type varchar \n" +
			") with (\n" +
			"'connector.type' = 'clickhouse',\n" +
			"'connector.driver' = 'ru.yandex.clickhouse.ClickHouseDriver', \n" +
			"'connector.url' = 'jdbc:clickhouse://%s:8123/', \n" +
			"'connector.db' = 'dts_tst', \n" +
			"'connector.table' = 'priest_user_test_local', \n" +
			"'connector.table.sign.column' = 'sign', \n" +
			"'connector.username' = '', \n" +
			"'connector.password' = '' \n" +
			")", LOCAL_CH_HOST);

		tEnv.sqlUpdate(sinkDDL);

		String sinkDML = "insert into priest_user_test SELECT id, user_name, auth,executeTime, type FROM `result` WHERE"
			+ " type in ('insert', 'update', 'delete')";
		tEnv.sqlUpdate(sinkDML);
		tEnv.execute("clickhouse jdbc output example");

		// check
		ResultSet rs = connection.createStatement().executeQuery("select count() as cnt from dts_tst.priest_user_test_local");
		rs.next();
		Assert.assertEquals(1, rs.getInt("cnt"));
		rs = connection.createStatement().executeQuery("select id, user_name from dts_tst.priest_user_test_local");
		rs.next();
		Assert.assertEquals(777, rs.getInt("id"));
		Assert.assertEquals("ccc", rs.getString("user_name"));

		// delete rows
		String deleteSql = "delete from dts_tst.priest_user_test_local where id=777";
		connection.createStatement().execute(deleteSql);

	}

	@Test
	public void testChDistributedTable() throws Exception {
		String chPsm = "olap.clickhouse.dorado_public.service.hl";
		String chDbName = "dts_tst";
		String chTableName = "priest_user_test_ch_sink";
		StreamTableEnvironment tEnv = initTableEnv(genRows());

		// sink ddl
		String sinkDDL = String.format("create table sink(\n" +
			"    id int, \n" +
			"    user_name varchar, \n" +
			"    auth int, \n" +
			"    executeTime bigint, \n" +
			"    type varchar \n" +
			") with (\n" +
			"'connector.type' = 'clickhouse',\n" +
			"'connector.driver' = 'ru.yandex.clickhouse.ClickHouseDriver', \n" +
			"'connector.psm' = '%s', \n" +
			"'connector.db' = '%s', \n" +
			"'connector.table' = '%s', \n" +
			"'connector.table.sign.column' = 'sign', \n" +
			"'connector.username' = 'hive2ch', \n" +
			"'connector.password' = 'FlLKuUrEIMlYWXEd' \n" +
			")", chPsm, chDbName, chTableName);

		tEnv.sqlUpdate(sinkDDL);

		String sinkDML = "insert into sink SELECT id, user_name, auth,executeTime, type FROM `result` WHERE"
			+ " type in ('insert', 'update', 'delete')";
		tEnv.sqlUpdate(sinkDML);
		tEnv.execute("clickhouse jdbc output example");

		// check
		ServiceNode node = getServiceNode(chPsm);
		String selectSql = String.format("select id, user_name from %s.%s where id=777", chDbName, chTableName);
		ClickHouseProperties properties = new ClickHouseProperties();
		String url = String.format("jdbc:clickhouse://%s:%s", node.getHost(), node.getPort());
		ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
		Connection connection = dataSource.getConnection();
		ResultSet rs = connection.createStatement().executeQuery(selectSql);
		rs.next();
		Assert.assertEquals(rs.getLong("id"), 777);
		Assert.assertEquals(rs.getString("user_name"), "ccc");

		// delete rows
		String deleteSql = String.format("delete from %s.%s where id=777", chDbName, chTableName);
		connection.createStatement().execute(deleteSql);
	}

	private static ServiceNode getServiceNode(String psm) {
		Discovery discovery = new Discovery();
		List<ServiceNode> serviceNodeList = discovery.translateOne(psm);
		if (serviceNodeList == null || 0 == serviceNodeList.size()) {
			throw new IllegalArgumentException("Invalid clickhouse psm: " + psm);
		}
		Collections.shuffle(serviceNodeList, new Random(System.currentTimeMillis()));
		ServiceNode sn = serviceNodeList.get(0);
		return sn;
	}

	private List<Row> genRows() {
		return Arrays.asList(
			Row.of(777, "aaa", 0, 1563261863000L, "insert"),
			Row.of(222, "bbb", 0, 1563261864000L, "insert"),
			Row.of(222, "bbb", 0, 1563261864000L, "delete"),
			// update （777, "aaa", 0, 1563261863000L) to (777, "ccc", 1, 1563261866000L)
			Row.of(777, "aaa", 0, 1563261863000L, "delete"),
			Row.of(777, "ccc", 1, 1563261866000L, "insert")
		);
	}

	private List<Row> genInsertRows() {
		return Arrays.asList(
			Row.of(2222, "a2", 0, 1563261863002L, "insert"),
			Row.of(2223, "b2", 0, 1563261864003L, "insert"),
			Row.of(2224, "c2", 1, 1563261866004L, "insert")
		);
	}

	@Test
	public void testChLocalTableWithoutSignColumnInProperty() throws Exception {
		StreamTableEnvironment tEnv = initTableEnv(genInsertRows());
		// sink ddl
		String sinkDDL = "create table sink(\n" +
			"    id int, \n" +
			"    user_name varchar, \n" +
			"    auth int, \n" +
			"    executeTime bigint, \n" +
			"    type varchar \n" +
			") with (\n" +
			"'connector.type' = 'clickhouse',\n" +
			"'connector.driver' = 'ru.yandex.clickhouse.ClickHouseDriver', \n" +
			"'connector.url' = 'jdbc:clickhouse://%s:%s/', \n" +
			"'connector.db' = '%s', \n" +
			"'connector.table' = '%s' \n" +
			")";

		tEnv.sqlUpdate(String.format(sinkDDL, LOCAL_CH_HOST, LOCAL_CH_PORT, LOCAL_CH_DB_NAME, LOCAL_CH_TABLE_WITH_NO_SIGN_COLUMN));
		String sinkDML = "insert into sink SELECT id, user_name, auth,executeTime, type FROM `result` WHERE type='insert'";
		tEnv.sqlUpdate(sinkDML);
		tEnv.execute("clickhouse jdbc output example");

		// count check
		String selectSql = String.format("select count() as cnt from %s.%s", LOCAL_CH_DB_NAME, LOCAL_CH_TABLE_WITH_NO_SIGN_COLUMN);
		ResultSet rs = connection.createStatement().executeQuery(selectSql);
		rs.next();
		Assert.assertEquals(rs.getInt("cnt"), 3);
	}
}
