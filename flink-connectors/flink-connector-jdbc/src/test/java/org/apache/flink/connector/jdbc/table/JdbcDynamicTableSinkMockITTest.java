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

import org.apache.flink.connector.jdbc.mock.MockDriver;
import org.apache.flink.connector.jdbc.mock.MockPreparedStatement;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ITCase for {@link JdbcDynamicTableSink}.
 */
public class JdbcDynamicTableSinkMockITTest extends AbstractTestBase {

	public static final String DB_URL = "jdbc:mock:memory";
	public static final String OUTPUT_TABLE = "REAL_TABLE";

	@Test
	public void testMockDriver() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		final String mockDataId = "testMockDriver"; // ensure the connection dataId is unique
		tEnv.executeSql(
			"CREATE TABLE upsertSink (" +
				"  id bigint," +
				"  name varchar," +
				"  age int," +
				"  street varchar," +
				"  primary key(id) not enforced" +
				") WITH (" +
				"  'connector'='jdbc'," +
				"  'url'='" + DB_URL + ":" + mockDataId + "'," +
				"  'use-bytedance-mysql'='false'," +
				"  'driver'='" + MockDriver.class.getName() + "'," +
				"  'sink.ignore-null-columns'='true'," +
				"  'table-name'='" + OUTPUT_TABLE + "'" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO upsertSink\n" +
			"SELECT * FROM\n" +
			"(VALUES (1, 'Tom', 18, 'Street 1'),\n" +
			"  (2, 'Lily', cast(null as int), 'Street 2'),\n" +
			"  (3, cast(null as varchar), 20, cast(null as varchar)),\n" +
			"  (4, 'a', cast(null as int), cast(null as varchar)),\n" +
			"  (4, 'b', cast(null as int), cast(null as varchar)),\n" +
			"  (4, 'c', cast(null as int), cast(null as varchar)))\n" +
			"AS T(id, name, age, street)");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		Map<String, List<Object[]>> expected = new HashMap<>(4);
		expected.put("INSERT INTO `REAL_TABLE`(`id`, `name`, `street`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `street`=VALUES(`street`)",
			Collections.singletonList(new Object[]{2L, "Lily", "Street 2"}));
		expected.put("INSERT INTO `REAL_TABLE`(`id`, `age`) VALUES(?, ?) ON DUPLICATE KEY UPDATE `age`=VALUES(`age`)",
			Collections.singletonList(new Object[]{3L, 20}));
		expected.put("INSERT INTO `REAL_TABLE`(`id`, `name`) VALUES(?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)",
			Collections.singletonList(new Object[]{4L, "c"}));
		expected.put("INSERT INTO `REAL_TABLE`(`id`, `name`, `age`, `street`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`), `age`=VALUES(`age`), `street`=VALUES(`street`)",
			Collections.singletonList(new Object[]{1L, "Tom", 18, "Street 1"}));

		for (Map.Entry<String, List<Object[]>> entry : expected.entrySet()) {
			List<Object[]> result = MockPreparedStatement.DATA.get(mockDataId).get(entry.getKey());
			Assert.assertNotNull(result);
			Assert.assertEquals(result.size(), entry.getValue().size());
			for (int i = 0; i < result.size(); ++i) {
				Assert.assertArrayEquals(entry.getValue().get(i), result.get(i));
			}
		}
	}
}
