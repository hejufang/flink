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

package org.apache.flink.table.api;

import org.apache.flink.streaming.api.graph.PlanJSONGenerator;

import org.junit.Test;

/**
 * Test for {@link PlanJSONGenerator}.
 */
public class PlanJSONGeneratorTest extends PlanJsonTestBase {
	@Test
	public void testAggSQL() {
		doVerify("INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1\n" +
			"GROUP BY id, name");
	}

	@Test
	public void testJoinSQL() {
		doVerify("INSERT INTO sink\n" +
			"SELECT T.id,\n" +
			"	T2.desc,\n" +
			"	T.num\n" +
			"FROM " +
			"	source1 AS T\n" +
			"JOIN\n" +
			"	source2 AS T2\n" +
			"ON T.id = T2.id AND T.eventtime\n" +
			"BETWEEN T2.eventtime - INTERVAL '1' HOUR AND T2.eventtime + INTERVAL '1' HOUR");
	}

	@Test
	public void testLookupJoinSQL() {
		doVerify("INSERT INTO sink\n" +
			"SELECT T.id,\n" +
			"	D.desc,\n" +
			"	T.num\n" +
			"FROM (\n" +
			"	SELECT *,\n" +
			"		PROCTIME() as proctime\n" +
			"	FROM source1\n" +
			") AS T\n" +
			"JOIN dim for system_time as of T.proctime AS D\n" +
			"ON T.id = D.id");
	}

	@Test
	public void testWindowAggSQL() {
		doVerify("INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1 " +
			"GROUP BY id, name, TUMBLE(eventtime, INTERVAL '6' HOUR)");

	}

	private void doVerify(String sql) {
		String jsonPlan = util.tableEnv().generatePlanGraphJson(sql);
		verifyJsonPlan(jsonPlan);
	}
}
