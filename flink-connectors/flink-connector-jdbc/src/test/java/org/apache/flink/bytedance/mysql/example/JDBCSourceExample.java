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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * JDBCSourceExample, please refer to JDBCSinkExample for detailed test steps.
 */
public class JDBCSourceExample {
	public static void main(String[] args) throws Exception {
		JDBCSourceExample jdbcSourceExample = new JDBCSourceExample();
		jdbcSourceExample.testJdbcSource();
	}

	private void testJdbcSource() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		tEnv.executeSql("" +
			"create table jdbDynamicTableSource (" +
			"	id INT," +
			"	num BIGINT," +
			"	ts TIMESTAMP(3)" +
			") with(" +
			"	'connector'='jdbc'," +
			"	'use-bytedance-mysql'='true'," +
			"	'consul'='toutiao.mysql.flink_key_indicators_write'," +
			"	'psm'='data.inf.compute'," +
			"	'dbname'='flink_key_indicators'," +
			"	'table-name'='dynamicSinkForAppend'" +
			")");

		TableResult result = tEnv.executeSql("SELECT * FROM jdbDynamicTableSource");
		result.print();
	}
}
