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

package org.apache.flink.table.util;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.rel.RelNode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * Tests for {@link ValidateUtil}.
 */
public class ValidateUtilTest extends TableTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testValidateTableSourceWithUserAndPsmSuccess() {
		RelNode relNode = getRelNode();
		ValidateUtil.validateTableSourceWithUserAndPsm(relNode, "authorized_user", "");
		ValidateUtil.validateTableSourceWithUserAndPsm(relNode, "", "authorized_psm");
	}

	@Test
	public void testValidateTableSourceWithUserAndPsmFailed() {
		RelNode relNode = getRelNode();
		thrown.expect(RuntimeException.class);
		ValidateUtil.validateTableSourceWithUserAndPsm(relNode, "aaa", "a.b.c");
	}

	private RelNode getRelNode() {
		TableConfig config = new TableConfig();
		config.getConfiguration().setBoolean(
			CalciteConfig$.MODULE$.CALCITE_SQL_TO_REL_CONVERTER_CONVERT_TABLE_ACCESS_ENABLED(), true);
		BatchTableTestUtil tableTestUtil = batchTestUtil(config);
		TableEnvironment tableEnv = tableTestUtil.getTableEnv();
		String ddl =
			"create table test_table (" +
				"   a varchar" +
				") with (" +
				"   'connector' = 'test-connector'," +
				"   'target' = 'test'," +
				"   'format' = 'test-format'," +
				"   'test-format.delimiter' = ','" +
				"" +
				")";
		tableEnv.executeSql(ddl);
		String query = "select * from test_table";
		Table table = tableEnv.sqlQuery(query);
		return TableTestUtil.toRelNode(table);
	}
}
