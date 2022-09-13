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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Test;

/**
 * Plan tests for JDBC connector, for example, testing projection push down.
 */
public class JdbcTablePlanTest extends TableTestBase {

	private final StreamTableTestUtil util = streamTestUtil(new TableConfig());

	@Test
	public void testProjectionPushDown() {
		util.tableEnv().executeSql(
			"CREATE TABLE jdbc (" +
				"id BIGINT," +
				"timestamp6_col TIMESTAMP(6)," +
				"timestamp9_col TIMESTAMP(9)," +
				"time_col TIME," +
				"real_col FLOAT," +
				"double_col DOUBLE," +
				"decimal_col DECIMAL(10, 4)" +
				") WITH (" +
				"  'connector'='jdbc'," +
				"  'url'='jdbc:derby:memory:test'," +
				"  'table-name'='test_table'" +
				")"
		);
		util.verifyPlan("SELECT decimal_col, timestamp9_col, id FROM jdbc");
	}

	@Test
	public void testFilterPushDown1() {
		util.getStreamEnv().setParallelism(12);
		util.tableEnv().executeSql(
			"CREATE TABLE jdbc (" +
				"id BIGINT," +
				"timestamp6_col TIMESTAMP(6)," +
				"timestamp9_col TIMESTAMP(9)," +
				"time_col TIME," +
				"real_col FLOAT," +
				"double_col DOUBLE," +
				"decimal_col DECIMAL(10, 4)" +
				") WITH (" +
				"  'connector'='jdbc'," +
				"  'url'='jdbc:derby:memory:test'," +
				"  'table-name'='test_table'" +
				")"
		);
		util.verifyTransformation("SELECT decimal_col, timestamp9_col, id FROM jdbc" +
			" WHERE id IN (1,2,3,4,5,6) AND double_col BETWEEN 0.0 AND 1.0 AND (decimal_col > 10 OR decimal_col < 10)");
	}

	@Test
	public void testFilterPushDown2() {
		util.getStreamEnv().setParallelism(12);
		util.tableEnv().executeSql(
			"CREATE TABLE jdbc (" +
				"id BIGINT," +
				"timestamp6_col TIMESTAMP(6)," +
				"timestamp9_col TIMESTAMP(9)," +
				"time_col TIME," +
				"real_col FLOAT," +
				"double_col DOUBLE," +
				"decimal_col DECIMAL(10, 4)" +
				") WITH (" +
				"  'connector'='jdbc'," +
				"  'url'='jdbc:derby:memory:test'," +
				"  'table-name'='test_table'" +
				")"
		);
		util.verifyTransformation("SELECT decimal_col, timestamp9_col, id FROM jdbc" +
			" WHERE cast(id as varchar) = '1'" +
			" AND IF(double_col > 0.0, 1, 0) = 1" +
			" AND decimal_col > 10");
	}

}
