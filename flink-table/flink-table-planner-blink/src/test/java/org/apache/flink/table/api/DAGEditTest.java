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

import org.apache.flink.streaming.api.graph.PlanGraph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests for APIs in {@link TableEnvironment} which is related to DAG editing.
 */
public class DAGEditTest extends PlanJsonTestBase {
	@Test
	public void testApplyOldGraphAfterEditingParallelism() {
		String sql = "INSERT INTO sink\n" +
			"SELECT T.id,\n" +
			"	LAST_VALUE(desc),\n" +
			"	SUM(num)\n" +
			"FROM (\n" +
			"		SELECT\n" +
			"			*,\n" +
			"			PROCTIME() as proc\n" +
			"		FROM source1\n" +
			") T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON D.id = T.id\n" +
			"GROUP BY T.id, name";

		String oldPlanGraphJson = util.tableEnv().generatePlanGraphJson(sql);
		ObjectMapper mapper = new ObjectMapper();
		try {
			PlanGraph oldPlanGraph = mapper.readValue(oldPlanGraphJson, PlanGraph.class);
			oldPlanGraph.getStreamNodes().get(2).setParallelism(8);
			oldPlanGraph.getStreamNodes().get(3).setParallelism(8);
			String newPlanGraphJson = mapper.writeValueAsString(oldPlanGraph);
			verifyJsonPlan(util.tableEnv().applyOldGraph(newPlanGraphJson, sql));
		} catch (JsonProcessingException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testUpdateGraphAfterEditingParallelism() {
		String sql = "INSERT INTO sink\n" +
			"SELECT T.id,\n" +
			"	LAST_VALUE(desc),\n" +
			"	SUM(num)\n" +
			"FROM (\n" +
			"		SELECT\n" +
			"			*,\n" +
			"			PROCTIME() as proc\n" +
			"		FROM source1\n" +
			") T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON D.id = T.id\n" +
			"GROUP BY T.id, name";

		String oldPlanGraphJson = util.tableEnv().generatePlanGraphJson(sql);
		ObjectMapper mapper = new ObjectMapper();
		try {
			PlanGraph oldPlanGraph = mapper.readValue(oldPlanGraphJson, PlanGraph.class);
			oldPlanGraph.getStreamNodes().get(2).setParallelism(8);
			oldPlanGraph.getStreamNodes().get(3).setParallelism(8);
			String newPlanGraphJson = mapper.writeValueAsString(oldPlanGraph);
			verifyJsonPlan(util.tableEnv().updateGraph(newPlanGraphJson, sql));
		} catch (JsonProcessingException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testApplyOldGraphAfterEditingHash() {
		String sql = "INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1\n" +
			"GROUP BY id, name";

		String oldPlanGraphJson = util.tableEnv().generatePlanGraphJson(sql);
		ObjectMapper mapper = new ObjectMapper();
		try {
			PlanGraph oldPlanGraph = mapper.readValue(oldPlanGraphJson, PlanGraph.class);
			oldPlanGraph.getStreamNodes().get(2).setUserProvidedHash("4ab008489d4c8ed0fe577883438cc1ff");
			String newPlanGraphJson = mapper.writeValueAsString(oldPlanGraph);
			verifyJsonPlan(util.tableEnv().applyOldGraph(newPlanGraphJson, sql));
		} catch (JsonProcessingException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testApplyOldGraphAfterChangingSQL() {
		String oldSql = "INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1\n" +
			"GROUP BY id, name";
		String oldPlanGraphJson = util.tableEnv().generatePlanGraphJson(oldSql);
		String newSql = "INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM\n" +
			"(SELECT id, name, num FROM source1\n" +
			"UNION ALL\n" +
			"SELECT id, desc as name, CHAR_LENGTH(desc) as num FROM source2)\n" +
			"GROUP BY id, name";
		verifyJsonPlan(util.tableEnv().applyOldGraph(oldPlanGraphJson, newSql));
	}
}
