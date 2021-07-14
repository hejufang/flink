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

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
		ObjectMapper mapper = new ObjectMapper();
		boolean res = false;
		String errorMsg = "expected:<" + plainJson + "> but was:<" + jsonPlan + ">";
		try {
			JsonNode expected = mapper.readTree(plainJson);
			JsonNode actual = mapper.readTree(jsonPlan);
			res = compareJsonTree(expected, actual);
		} catch (Throwable e) {
			errorMsg = e.getMessage() + errorMsg;
			fail(errorMsg);
		}
		assertTrue(errorMsg, res);
	}

	private boolean compareJsonTree(JsonNode a, JsonNode b) {
		if (a == null || b == null || a.size() != b.size() || a.getClass() != b.getClass()) {
			return false;
		}
		if (a.isArray()) {
			List<JsonNode> aList = Lists.newArrayList(a.elements());
			List<JsonNode> bList = Lists.newArrayList(b.elements());
			aList.sort(Comparator.comparingInt(o -> o.toString().hashCode()));
			bList.sort(Comparator.comparingInt(o -> o.toString().hashCode()));
			for (int i = 0; i < aList.size(); i++) {
				if (!compareJsonTree(aList.get(i), bList.get(i))) {
					return false;
				}
			}
			return true;
		} else if (a.isObject()){
			for (Iterator<String> it = a.fieldNames(); it.hasNext(); ) {
				String field = it.next();
				if (!compareJsonTree(a.get(field), b.get(field))) {
					return false;
				}
			}
			return true;
		} else {
			return a.equals(b);
		}
	}

	private Set<JsonNode> convertJSONArrayToSet(ArrayNode input) {
		Set<JsonNode> retVal = new TreeSet<>();
		for (int i = 0; i < input.size(); i++) {
			retVal.add(input.get(i));
		}
		return retVal;
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
