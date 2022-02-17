/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;


/**
 * IT Tests for {@link ByteSQLDynamicTableSink}.
 */
public class ByteSQLDynamicTableSinkITTest extends ByteSQLTableTestBase {

	@Test
	public void testUpsert() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

		tEnv.createTemporaryView("T", t);
		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  cnt BIGINT," +
				"  lencnt BIGINT," +
				"  cTag INT," +
				"  ts TIMESTAMP(3)," +
				"  PRIMARY KEY (cnt, cTag) NOT ENFORCED" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.buffer-flush.interval' = '10s' " +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO upsertSink \n" +
			"SELECT num, COUNT(len) AS lencnt, MAX(id), MAX(ts) AS ts\n" +
			"FROM (SELECT *, CHAR_LENGTH(text) as len FROM T)\n" +
			"GROUP BY num");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(8));
	}

	@Test
	public void testAppend() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
		tEnv.createTemporaryView("T", t);
		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  id INT PRIMARY KEY NOT ENFORCED," +
				"  num BIGINT," +
				"  ts TIMESTAMP(3)" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.buffer-flush.interval' = '10s' " +
				")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO upsertSink SELECT id, num, ts FROM T WHERE id IN (2, 3, 4)");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(3));
	}

	@Test
	public void testIgnoreNull() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
		tEnv.createTemporaryView("T", t);

		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  id INT PRIMARY KEY NOT ENFORCED," +
				"  text VARCHAR," +
				"  ts TIMESTAMP(3)" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.ignore-null-columns' = 'true', " +
				"  'sink.buffer-flush.interval' = '10' " +
				")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO upsertSink SELECT id, text, ts FROM T");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(8));
	}

	@Test
	public void testNotIgnoreNull() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
		tEnv.createTemporaryView("T", t);

		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  id INT PRIMARY KEY NOT ENFORCED," +
				"  text VARCHAR," +
				"  ts TIMESTAMP(3)" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.ignore-null-columns' = 'false', " +
				"  'sink.buffer-flush.interval' = '10' " +
				")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO upsertSink SELECT id, text, ts FROM T");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(8));
	}

	@Test
	public void testIgnoreDelete() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
		tEnv.createTemporaryView("T", t);

		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  num BIGINT," +
				"  id INT," +
				"  PRIMARY KEY (num, id) NOT ENFORCED" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.ignore-delete' = 'true', " +
				"  'sink.buffer-flush.interval' = '10 s' " +
				")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO upsertSink\n" +
				"SELECT num, id\n" +
				"FROM (\n" +
				"  SELECT *,\n" +
				"     ROW_NUMBER() OVER (PARTITION BY num ORDER BY id desc) AS rownum\n" +
				"  FROM T" +
				") t\n" +
				"WHERE rownum <= 2");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(8));
	}

	@Test
	public void testNotIgnoreDelete() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));
		tEnv.createTemporaryView("T", t);

		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  num BIGINT," +
				"  id INT," +
				"  PRIMARY KEY (num, id) NOT ENFORCED" +
				") WITH (" +
				"  'connector' = 'bytesql'," +
				"  'consul' = 'test', " +
				"  'db.class'='org.apache.flink.connector.bytesql.table.client.ByteSQLDBMock'," +
				"  'database' = 'test', " +
				"  'table' = 'test', " +
				"  'username' = 'test', " +
				"  'password' = 'test', " +
				"  'sink.ignore-delete' = 'false', " +
				"  'sink.buffer-flush.interval' = '10 s' " +
				")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO upsertSink\n" +
				"SELECT num, id\n" +
				"FROM (\n" +
				"  SELECT *,\n" +
				"     ROW_NUMBER() OVER (PARTITION BY num ORDER BY id desc) AS rownum\n" +
				"  FROM T" +
				") t\n" +
				"WHERE rownum <= 2");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		verify(ByteSQLDBMock.getInstance(null).getExecutedSQLs(8));
	}

}
