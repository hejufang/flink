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

package org.apache.flink.bytedance.mysql.example;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * JDBCSinkExample.
 * Because bytedance mysql online server is not allowed to be requested offline,
 * so we need to run a real flink job on yarn to test it.
 * The step is shown below :
 * 1. prepare mysql online test db and table, in our case, we use 'flink_key_indicators' as db
 * 	and 'dynamicSinkForAppend' as table (this name may be confused and it will be renamed later).
 * 2. build your test job. A sample for jdbc is 'https://code.byted.org/zhangwei.114/test_jdbc'.
 * 3. prepare for submitting flink job. Related document is 'https://doc.bytedance.net/docs/1497/1645/47839/'.
 * 4. to avoid token empty exception, use doas tools, Related document is
 * 	https://bytedance.feishu.cn/docs/85d9fb0GEEHalMJqmNvJNg
 */
public class JDBCSinkExample {
	public static void main(String[] args) throws Exception {
		JDBCSinkExample jdbcSinkExample = new JDBCSinkExample();
		jdbcSinkExample.testSink();
	}

	public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(
		StreamExecutionEnvironment env) {
		List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1978-01-01 00:00:00.001")));
		data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1978-01-01 00:00:00.002")));
		data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1978-01-01 00:00:00.003")));
		data.add(new Tuple4<>(4, 3L, "Hello world, how are you?", Timestamp.valueOf("1978-01-01 00:00:00.004")));
		data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1978-01-01 00:00:00.005")));
		data.add(new Tuple4<>(6, 3L, "Luke Skywalker", Timestamp.valueOf("1978-01-01 00:00:00.006")));
		data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1978-01-01 00:00:00.007")));
		data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1978-01-01 00:00:00.008")));
		data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1978-01-01 00:00:00.009")));
		data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1978-01-01 00:00:00.010")));
		data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1978-01-01 00:00:00.011")));
		data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1978-01-01 00:00:00.012")));
		data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1978-01-01 00:00:00.013")));
		data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1978-01-01 00:00:00.014")));
		data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1978-01-01 00:00:00.015")));
		data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1978-01-01 00:00:00.016")));
		data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1978-01-01 00:00:00.017")));
		data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1978-01-01 00:00:00.018")));
		data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1978-01-01 00:00:00.019")));
		data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1978-01-01 00:00:00.020")));
		data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1978-01-01 00:00:00.021")));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	private void testSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env),
			$("id"), $("num"), $("text"), $("ts"));

		tEnv.registerTable("T", t);

		tEnv.executeSql("" +
			"CREATE TABLE sink (" +
			"	id INT," +
			"	num BIGINT," +
			"	ts TIMESTAMP(3)" +
			") WITH (" +
			"	'connector'='jdbc'," +
			"	'consul'='toutiao.mysql.flink_key_indicators_write'," +
			"	'psm'='data.inf.compute'," +
			" 	'dbname'='flink_key_indicators'," +
			"	'table-name'='dynamicSinkForAppend'" +
			")");

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO sink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
	}
}
