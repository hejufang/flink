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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * JDBCLookupExample, please refer to JDBCSinkExample for detailed test steps.
 */
public class JDBCLookupExample {
	public static void main(String[] args) throws Exception {
		JDBCLookupExample jdbcLookupExample = new JDBCLookupExample();
		jdbcLookupExample.testLookup();
	}

	private void testLookup() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		TableResult result = useDynamicTableFactory(env, tEnv);
		result.print();
	}

	private TableResult useDynamicTableFactory(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) throws Exception {
		List<Tuple2<Integer, String>> list = new ArrayList<>();
		for (int i = 0; i < 10000; i++) {
			list.add(new Tuple2<>(1, "1"));
			list.add(new Tuple2<>(2, "2"));
			list.add(new Tuple2<>(3, "3"));
		}
		Table t = tEnv.fromDataStream(env.fromCollection(list), $("id1"), $("id2"), $("proctime").proctime());

		tEnv.createTemporaryView("T", t);

		tEnv.executeSql("" +
			"create table lookup (" +
			"	id INT," +
			"	num BIGINT," +
			"	ts TIMESTAMP(3)" +
			") with(" +
			"	'connector'='jdbc'," +
			"	'consul'='toutiao.mysql.flink_key_indicators_write'," +
			"	'psm'='data.inf.compute'," +
			"	'dbname'='flink_key_indicators'," +
			"	'table-name'='dynamicSinkForAppend'" +
			")");

		String sqlQuery = "SELECT source.id1, source.id2, L.num, L.ts FROM T AS source " +
				"JOIN lookup for system_time as of source.proctime AS L " +
				"ON source.id1 = L.id";
		return tEnv.executeSql(sqlQuery);
	}
}
