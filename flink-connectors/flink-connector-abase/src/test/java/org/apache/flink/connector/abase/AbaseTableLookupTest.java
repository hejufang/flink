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

package org.apache.flink.connector.abase;

import org.apache.flink.connector.abase.utils.ResourceUtil;
import org.apache.flink.table.api.TableResult;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * The ITCase for {@link AbaseLookupFunction}.
 */
public class AbaseTableLookupTest extends AbaseTestBase {
	private static final String PATH = "lookup/";

	private final ByteArrayOutputStream out = new ByteArrayOutputStream();
	private final PrintStream originalOut = System.out;

	@Before
	public void setStreams() {
		System.setOut(new PrintStream(out));
	}

	@After
	public void restoreInitialStreams() {
		System.setOut(originalOut);
	}

	@Test
	public void testGeneralType() throws Exception {
		Mockito.when(abaseClientWrapper.get("Bob")).thenReturn("43");
		Mockito.when(abaseClientWrapper.get("Lucy")).thenReturn("68");

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` BIGINT\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`score`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testGeneralTypeWithMultiplePKs() throws Exception {
		Mockito.when(abaseClientWrapper.get("race_game:47:Kim:10")).thenReturn("43");
		Mockito.when(abaseClientWrapper.get("race_game:34:Tom:15")).thenReturn("21");
		Mockito.when(abaseClientWrapper.get("race_game:68:Bob:20")).thenReturn("76");
		Mockito.when(abaseClientWrapper.get("race_game:19:Lucy:20")).thenReturn("91");

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` INT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `rank`  INT,\n" +
				"	 PRIMARY KEY (`score`, `name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${score}:${name}:${bonus}',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`score`, T.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`bonus`=T.`bonus`\n" +
			"AND D.`name`=T.`name`\n" +
			"AND D.`score`=T.`score`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testGeneralTypeWithSchema() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("score", 79);
		root.put("bonus", 10);
		root.put("rank", 8);
		byte[] d1 = objectMapper.writeValueAsBytes(root);
		Mockito.when(abaseClientWrapper.get("Kim".getBytes())).thenReturn(d1);

		root = objectMapper.createObjectNode();
		root.put("score", 83);
		root.put("bonus", 20);
		root.put("rank", 7);
		byte[] d2 = objectMapper.writeValueAsBytes(root);
		Mockito.when(abaseClientWrapper.get("Tom".getBytes())).thenReturn(d2);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` INT,\n" +
				"    `name`  VARCHAR PRIMARY KEY NOT ENFORCED,\n" +
				"    `score` BIGINT,\n" +
				"    `rank`  INT\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'format' = 'json',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testGeneralTypeWithSchemaAndMultiplePKs() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bonus", 10);
		root.put("rank", 8);
		byte[] d1 = objectMapper.writeValueAsBytes(root);
		Mockito.when(abaseClientWrapper.get("race_game:Kim:47".getBytes())).thenReturn(d1);

		root = objectMapper.createObjectNode();
		root.put("bonus", 20);
		root.put("rank", 7);
		byte[] d2 = objectMapper.writeValueAsBytes(root);
		Mockito.when(abaseClientWrapper.get("race_game:Tom:72".getBytes())).thenReturn(d2);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` INT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `rank`  INT,\n" +
				"	 PRIMARY KEY (`name`, `score`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${score}',\n" +
				"    'format' = 'json',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`score`=T.`score`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testGeneralTypeWithSchemaAndMultiplePKs2() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode root = objectMapper.createObjectNode();
		root.put("bonus", 10);
		root.put("rank", 8);
		byte[] d1 = objectMapper.writeValueAsBytes(root);
		Mockito.when(abaseClientWrapper.get("race_game:Kim:47".getBytes())).thenReturn(d1);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` INT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `rank`  INT,\n" +
				"	 PRIMARY KEY (`name`, `score`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${score}',\n" +
				"    'format' = 'json',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`score`=T.`score`\n" +
			"AND D.`bonus` = 10");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testHashType() throws Exception {
		Map<String, String> val = new HashMap<>();
		val.put("score", "47");
		val.put("bonus", "10");
		val.put("rank", "9");
		Mockito.when(abaseClientWrapper.hgetAll("Kim")).thenReturn(val);

		val.put("score", "34");
		val.put("bonus", "15");
		val.put("rank", "11");
		Mockito.when(abaseClientWrapper.hgetAll("Tom")).thenReturn(val);

		val.put("score", "68");
		val.put("bonus", "20");
		val.put("rank", "7");
		Mockito.when(abaseClientWrapper.hgetAll("Bob")).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   MAP<VARCHAR, VARCHAR>\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'value-type' = 'hash',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, CAST(D.val['score'] as BIGINT), CAST(D.val['bonus'] AS INT), CAST(D.val['rank'] as INT)\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testHashTypeWithMultiplePKs() throws Exception {
		Map<String, String> val = new HashMap<>();
		val.put("score", "47");
		val.put("bonus", "10");
		val.put("rank", "9");
		Mockito.when(abaseClientWrapper.hgetAll("race_game:Tom:22")).thenReturn(val);

		val.put("score", "34");
		val.put("bonus", "15");
		val.put("rank", "11");
		Mockito.when(abaseClientWrapper.hgetAll("race_game:Lucy:19")).thenReturn(val);

		val.put("score", "68");
		val.put("bonus", "20");
		val.put("rank", "7");
		Mockito.when(abaseClientWrapper.hgetAll("race_game:Bob:37")).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `score` BIGINT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   MAP<VARCHAR, VARCHAR>,\n" +
				"	 PRIMARY KEY (`name`, `score`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${score}',\n" +
				"    'value-type' = 'hash',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`score`, CAST(D.val['bonus'] AS INT), CAST(D.val['rank'] as INT)\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`score`=T.`score`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testHashTypeWithKeySpecified() throws Exception {
		String[] fields = {"score", "bonus", "rank"};
		List<String> results = Arrays.asList("15", "10", "18");
		Mockito.when(abaseClientWrapper.hmget("race_game:Kim", fields)).thenReturn(results);

		results = Arrays.asList("72", "20", "6");
		Mockito.when(abaseClientWrapper.hmget("race_game:Tom", fields)).thenReturn(results);

		results = Arrays.asList("19", "5", "20");
		List<String> results2 = Arrays.asList("79", "25", "4");
		Mockito.when(abaseClientWrapper.hmget("race_game:Lucy", fields)).thenReturn(results).thenReturn(results2);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR PRIMARY KEY NOT ENFORCED,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}',\n" +
				"	 'value-type' = 'hash',\n" +
				"	 'lookup.specify-hash-keys' = 'true',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testHashTypeWithKeySpecified2() throws Exception {
		String[] fields = {"score", "bonus", "rank"};
		List<String> results = Arrays.asList("15", "10", "18");
		Mockito.when(abaseClientWrapper.hmget("race_game:Kim", fields)).thenReturn(results);

		results = Arrays.asList("72", "20", "6");
		Mockito.when(abaseClientWrapper.hmget("race_game:Tom", fields)).thenReturn(results);

		results = Arrays.asList("19", "5", "20");
		List<String> results2 = Arrays.asList("79", "25", "4");
		Mockito.when(abaseClientWrapper.hmget("race_game:Lucy", fields)).thenReturn(results).thenReturn(results2);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR PRIMARY KEY NOT ENFORCED,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}',\n" +
				"	 'value-type' = 'hash',\n" +
				"	 'specify-hash-keys' = 'true',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + "testHashTypeWithKeySpecified.out"), out.toString());
	}

	@Test
	public void testHashTypeWithKeySpecifiedAndMultiplePKs() throws Exception {
		String[] fields = {"bonus", "rank"};
		List<String> results = Arrays.asList("10", "18");
		Mockito.when(abaseClientWrapper.hmget("race_game:Kim:15", fields)).thenReturn(results);

		results = Arrays.asList("20", "6");
		Mockito.when(abaseClientWrapper.hmget("race_game:Tom:72", fields)).thenReturn(results);

		results = Arrays.asList("5", "20");
		Mockito.when(abaseClientWrapper.hmget("race_game:Lucy:19", fields)).thenReturn(results);

		results = Arrays.asList("25", "4");
		Mockito.when(abaseClientWrapper.hmget("race_game:Lucy:79", fields)).thenReturn(results);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` INT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `rank`  INT,\n" +
				"	 PRIMARY KEY (`name`, `score`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${score}',\n" +
				"	 'value-type' = 'hash',\n" +
				"	 'lookup.specify-hash-keys' = 'true',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `score` BIGINT,\n" +
				"    `bonus` INT,\n" +
				"    `rank`  INT\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`score`, D.`bonus`, D.`rank`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`score`=T.`score`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testListType() throws Exception {
		List<String> val = Arrays.asList("20", "5", "15", "10");
		Mockito.when(abaseClientWrapper.lrange("Kim", 0, -1)).thenReturn(val);

		val = Arrays.asList("5", "10", "15");
		Mockito.when(abaseClientWrapper.lrange("Tom", 0, -1)).thenReturn(val);

		val = Arrays.asList("20", "25", "15", "10", "5", "10");
		Mockito.when(abaseClientWrapper.lrange("Bob", 0, -1)).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'value-type' = 'list',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `scores` ARRAY<VARCHAR>\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`val`\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testListTypeWithMultiplePKs() throws Exception {
		List<String> val = Arrays.asList("20", "5", "15", "10");
		Mockito.when(abaseClientWrapper.lrange("race_game:Bob:5", 0, -1)).thenReturn(val);
		val = Arrays.asList("5", "10", "15");
		Mockito.when(abaseClientWrapper.lrange("race_game:Tom:10", 0, -1)).thenReturn(val);
		val = Arrays.asList("20", "25", "15", "10", "5", "10");
		Mockito.when(abaseClientWrapper.lrange("race_game:Bob:20", 0, -1)).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` BIGINT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>,\n" +
				"	 PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${bonus}',\n" +
				"    'value-type' = 'list',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `bonus` BIGINT,\n" +
				"    `frist_score` VARCHAR\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`bonus`, D.`val`[1]\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`bonus`=T.`bonus`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testSetType() throws Exception {
		Set<String> val = new HashSet<>(Arrays.asList("20", "5", "15", "10"));
		Mockito.when(abaseClientWrapper.smembers("Kim")).thenReturn(val);

		val = new HashSet<>(Arrays.asList("5", "10", "15"));
		Mockito.when(abaseClientWrapper.smembers("Tom")).thenReturn(val);

		val = new HashSet<>(Arrays.asList("20", "25", "15", "10", "5"));
		Mockito.when(abaseClientWrapper.smembers("Bob")).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'value-type' = 'set',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` VARCHAR\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`val`[1]\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testSetTypeWithMultiplePKs() throws Exception {
		Set<String> val = new HashSet<>(Arrays.asList("20", "5", "15", "10"));
		Mockito.when(abaseClientWrapper.smembers("race_game:Bob:5")).thenReturn(val);
		val = new HashSet<>(Arrays.asList("5", "10", "15"));
		Mockito.when(abaseClientWrapper.smembers("race_game:Tom:10")).thenReturn(val);
		val = new HashSet<>(Arrays.asList("20", "25", "15", "10", "5"));
		Mockito.when(abaseClientWrapper.smembers("race_game:Bob:20")).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` BIGINT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>,\n" +
				"	 PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${bonus}',\n" +
				"    'value-type' = 'set',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `bonus` BIGINT,\n" +
				"    `score` VARCHAR\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`bonus`, D.`val`[1]\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`bonus`=T.`bonus`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testZSetType() throws Exception {
		Set<String> val = new HashSet<>(Arrays.asList("20", "5", "15", "10"));
		Mockito.when(abaseClientWrapper.zrange("Kim", 0, -1)).thenReturn(val);

		val = new HashSet<>(Arrays.asList("5", "10", "15"));
		Mockito.when(abaseClientWrapper.zrange("Tom", 0, -1)).thenReturn(val);

		val = new HashSet<>(Arrays.asList("20", "25", "15", "10", "5"));
		Mockito.when(abaseClientWrapper.zrange("Bob", 0, -1)).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'value-type' = 'zset',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name`  VARCHAR,\n" +
				"    `score` VARCHAR\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, D.`val`[1]\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}

	@Test
	public void testZSetTypeWithMultiplePKs() throws Exception {
		Set<String> val = new HashSet<>(Arrays.asList("20", "5", "15", "10"));
		Mockito.when(abaseClientWrapper.zrange("race_game:Bob:5", 0, -1)).thenReturn(val);
		val = new HashSet<>(Arrays.asList("5", "10", "15"));
		Mockito.when(abaseClientWrapper.zrange("race_game:Tom:10", 0, -1)).thenReturn(val);
		val = new HashSet<>(Arrays.asList("20", "25", "15", "10", "5"));
		Mockito.when(abaseClientWrapper.zrange("race_game:Bob:20", 0, -1)).thenReturn(val);

		tEnv.executeSql(
			"CREATE TABLE dim (\n" +
				"    `bonus` BIGINT,\n" +
				"    `name`  VARCHAR,\n" +
				"    `val`   ARRAY<VARCHAR>,\n" +
				"	 PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'byte-abase',\n" +
				"    'cluster' = 'test',\n" +
				"    'table' = 'test',\n" +
				"    'key_format' = 'race_game:${name}:${bonus}',\n" +
				"    'value-type' = 'zset',\n" +
				"    'lookup.cache.max-rows' = '10',\n" +
				"    'lookup.cache.ttl' = '3 min'\n" +
				")");
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"    `name` VARCHAR,\n" +
				"    `bonus` BIGINT,\n" +
				"    `score` VARCHAR\n" +
				") WITH(\n" +
				"	 'connector' = 'print',\n" +
				"	 'print-sample-ratio' = '1'\n" +
				")");
		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT T.`name`, T.`bonus`, D.`val`[1]\n" +
			"FROM (select T1.`name`, T1.`score`, T1.`bonus`, T1.`time`, PROCTIME() as proc from T1) T\n" +
			"LEFT JOIN dim FOR SYSTEM_TIME AS OF T.proc AS D\n" +
			"ON D.`name`=T.`name`\n" +
			"AND D.`bonus`=T.`bonus`");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		assertEquals(ResourceUtil.readFileContent(PATH + name.getMethodName() + ".out"), out.toString());
	}
}
