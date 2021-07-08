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
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link PlanJSONGenerator}.
 */
public class PlanJSONGeneratorTest extends TableTestBase {
	private final StreamTableTestUtil util = streamTestUtil(new TableConfig());
	private static final String RESOURCE_PATH = "./src/test/resources/json-plan/";
	private static final String FILE_NAME_PATTERN = "%s.out";
	@Before
	public void before() {
		util.addTable("CREATE TABLE source1(\n" +
			"	id INT,\n" +
			"	name VARCHAR,\n" +
			"	num BIGINT,\n" +
			"	eventtime TIMESTAMP(3),\n" +
			"	WATERMARK FOR eventtime AS eventtime - INTERVAL '5' SECOND\n" +
			") WITH (\n" +
			"	'connector' = 'values'\n" +
			")");
		util.addTable("CREATE TABLE source2(\n" +
			"	id INT,\n" +
			"	desc VARCHAR,\n" +
			"	eventtime TIMESTAMP(3),\n" +
			"	WATERMARK FOR eventtime AS eventtime - INTERVAL '5' SECOND\n" +
			") WITH (\n" +
			"	'connector' = 'values'\n" +
			")");
		util.addTable("CREATE TABLE dim(\n" +
			"	id INT,\n" +
			"	desc VARCHAR\n" +
			") WITH (\n" +
			"	'connector' = 'values',\n" +
			"	'bounded' = 'true'\n" +
			")");
		util.addTable("CREATE TABLE sink(\n" +
			"	id INT,\n" +
			"	name VARCHAR,\n" +
			"	num BIGINT\n" +
			") WITH (\n" +
			"	'connector' = 'print'\n" +
			")");
		util.getTableEnv().getConfig().getConfiguration()
			.setString("table.exec.resource.default-parallelism", "4");
	}

	@Test
	public void testAggSQL() {
		doVerifyJsonPlan("INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1\n" +
			"GROUP BY id, name");
	}

	@Test
	public void testJoinSQL() {
		doVerifyJsonPlan("INSERT INTO sink\n" +
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
		doVerifyJsonPlan("INSERT INTO sink\n" +
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
		doVerifyJsonPlan("INSERT INTO sink\n" +
			"SELECT id,\n" +
			"	name,\n" +
			"	SUM(num)\n" +
			"FROM source1 " +
			"GROUP BY id, name, TUMBLE(eventtime, INTERVAL '6' HOUR)");

	}

	private void doVerifyJsonPlan(String sql) {
		String jsonPlan = util.tableEnv().generatePlanGraphJson(sql);
		String plainJson = readJSONStringFromFile();
		assertEquals(plainJson, jsonPlan);
	}

	private String readJSONStringFromFile() {
		File file = new File(RESOURCE_PATH, String.format(FILE_NAME_PATTERN, testName().getMethodName()));
		StringBuilder stringBuilder = new StringBuilder();
		try (
			InputStreamReader read = new InputStreamReader(new FileInputStream(file));
			BufferedReader bufferedReader = new BufferedReader(read);) {
			String line;
			String ls = System.getProperty("line.separator");
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
			String json = stringBuilder.toString();
			ObjectMapper mapper = new ObjectMapper();
			Object jsonObject = mapper.readValue(json, Object.class);
			return mapper.writeValueAsString(jsonObject);
		} catch (IOException ignored) {
		}
		return stringBuilder.toString();
	}
}
