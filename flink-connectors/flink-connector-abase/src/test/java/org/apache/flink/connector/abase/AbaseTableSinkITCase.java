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

import org.apache.flink.table.api.TableResult;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.eq;

/**
 * The ITCase for {@link AbaseTableSink}.
 */
public class AbaseTableSinkITCase extends AbaseTestBase {

	// final result of source data stream
	private static final Map<String, Map<String, String>> results;
	private static final Map<String, Map<String, String>> results2;
	private static final Map<String, Map<String, String>> multiPKsResults;
	private static final Map<String, Map<String, String>> insertions;

	static {
		results = initResults();
		results2 = initResults2();
		multiPKsResults = initMulPKResults();
		insertions = initInsertions();
	}

	/**
	 * Test of general type SETEX command.
	 * @throws Exception
	 */
	@Test
	public void testSETEX() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '10 min',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '10 min'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, score\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySETEX(600, results2);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of general type SETEX command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testSETEXWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT,\n" +
				"  bonus INT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '2 min',\n" +
				"  'sink.buffer-flush.max-rows' = '5',\n" +
				"  'sink.buffer-flush.interval' = '20 min'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, score, bonus\n" +
			"FROM T3");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySETEX(120, multiPKsResults);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of general type SET command with serialization of primary key skipped.
	 * @throws Exception
	 */
	@Test
	public void testSETWithSerSkipPKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT,\n" +
				"  bonus INT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'format' = 'json',\n" +
				"  'sink.record.ttl' = '1 min',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, score, bonus\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySETEXWithJsonFormat(60, results);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of general type SET command with serialization of primary key remained.
	 * @throws Exception
	 */
	@Test
	public void testSETWithSer() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT,\n" +
				"  bonus INT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'format' = 'json',\n" +
				"  'value.format.skip-key' = 'false',\n" +
				"  'sink.record.ttl' = '2 min',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, score, bonus\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		Map<String, Map<String, String>> res = initResults();
		for (String key : res.keySet()) {
			Map<String, String> val = res.get(key);
			val.put("name", key);
		}
		verifySETEXWithJsonFormat(120, res);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of general type SET command with serialization of primary keys skipped.
	 * @throws Exception
	 */
	@Test
	public void testSETWithSerSkipPKsAndMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `score` BIGINT,\n" +
				"  `bonus` INT,\n" +
				"  `rank`  INT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'format' = 'json',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '1 min',\n" +
				"  'sink.buffer-flush.max-rows' = '16',\n" +
				"  'sink.buffer-flush.interval' = '20 min'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `score`, `bonus`, `rank`\n" +
			"FROM T3");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySETEXWithJsonFormat(60, multiPKsResults);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HSET command.
	 * @throws Exception
	 */
	@Test
	public void testHSET() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  key   VARCHAR,\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '10 min',\n" +
				"  'sink.buffer-flush.max-rows' = '16',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, 'score', score\n" +
			"FROM T1");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHSET(results);
		verifyHashTTL(results.keySet(), 600);
		Mockito.verify(clientPipeline, Mockito.times(2)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HMSET command.
	 * @throws Exception
	 */
	@Test
	public void testHMSET() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  key VARCHAR,\n" +
				"  map Map<VARCHAR, VARCHAR>\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '60 s',\n" +
				"  'sink.buffer-flush.max-rows' = '16',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT name, MAP['score', CAST(score AS VARCHAR), 'bonus', CAST(bonus AS VARCHAR)]\n" +
			"FROM T1");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHMSET(results);
		verifyHashTTL(results.keySet(), 60);
		Mockito.verify(clientPipeline, Mockito.times(2)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HSET & HDEL command.
	 * @throws Exception
	 */
	@Test
	public void testHSETWithRetract() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  key   VARCHAR,\n" +
				"  name  VARCHAR,\n" +
				"  score BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'sink.ignore-delete' = 'false',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
				"SELECT\n" +
				"    `name`,\n" +
				"    'score',\n" +
				"    `score`\n" +
				"FROM\n" +
				"    (\n" +
				"        SELECT\n" +
				"            `name`,\n" +
				"            `score`,\n" +
				"            row_number() OVER (\n" +
				"                PARTITION BY `name`\n" +
				"                ORDER BY `score` DESC\n" +
				"            ) AS rownum\n" +
				"        FROM T2\n" +
				"    )\n" +
				"WHERE\n" +
				"    rownum <= 1 ");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHSETWithRetract();
		Mockito.verify(clientPipeline, Mockito.times(2)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(2)).close();
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HMSET & HDEL command.
	 * @throws Exception
	 */
	@Test
	public void testHMSETWithRetract() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  key VARCHAR,\n" +
				"  map Map<VARCHAR, VARCHAR>\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '60 s',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'sink.ignore-delete' = 'false',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
				"SELECT\n" +
				"    `name`,\n" +
				"    CASE WHEN `score` < 20\n" +
				"    THEN MAP['score', CAST(`score` AS VARCHAR), 'bonus', CAST(`bonus` AS VARCHAR)]\n" +
				"    ELSE MAP['time', DATE_FORMAT(`time`, 'yyyy-MM-dd HH:mm:ss'), 'bonus', CAST(`bonus` AS VARCHAR)]\n" +
				"    END\n" +
				"FROM\n" +
				"    (\n" +
				"        SELECT\n" +
				"            `name`,\n" +
				"            `score`,\n" +
				"            `bonus`,\n" +
				"            `time`,\n" +
				"            row_number() OVER (\n" +
				"                PARTITION BY `name`\n" +
				"                ORDER BY `score` DESC\n" +
				"            ) AS rownum\n" +
				"        FROM T2\n" +
				"    )\n" +
				"WHERE\n" +
				"    rownum <= 1 ");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHMSETWithRetract();
		verifyHashTTL(new HashSet<>(Arrays.asList("Bob", "Tom")), 60);
		Mockito.verify(clientPipeline, Mockito.times(4)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(2)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HSET command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testHSETWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `bonus` INT,\n" +
				"  `key`   VARCHAR,\n" +
				"  `score` BIGINT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '1 min',\n" +
				"  'sink.buffer-flush.max-rows' = '5',\n" +
				"  'sink.buffer-flush.interval' = '10 min',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `bonus`, 'score', `score`\n" +
			"FROM T3");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHSET(multiPKsResults);
		verifyHashTTL(multiPKsResults.keySet(), 60);
		Mockito.verify(clientPipeline, Mockito.times(2)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of hash type HMSET command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testHMSETWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `bonus` INT,\n" +
				"  `val`   Map<VARCHAR, VARCHAR>,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '60 s',\n" +
				"  'sink.buffer-flush.max-rows' = '5',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'hash'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `bonus`, MAP['score', CAST(`score` AS VARCHAR), 'rank', CAST(`rank` AS VARCHAR)]\n" +
			"FROM T3");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHMSET(multiPKsResults);
		verifyHashTTL(multiPKsResults.keySet(), 60);
		Mockito.verify(clientPipeline, Mockito.times(2)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of list type LPUSH command.
	 * @throws Exception
	 */
	@Test
	public void testLPUSH() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `score` BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '10 s',\n" +
				"  'sink.buffer-flush.max-rows' = '5',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'list'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `score`\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyLPUSH(results);
		verifyListTTL(results.keySet(), 10);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of list type LPUSH command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testLPUSHWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `bonus` INT,\n" +
				"  `score` BIGINT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '30 s',\n" +
				"  'sink.buffer-flush.max-rows' = '3',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'list'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `bonus`, `score`\n" +
			"FROM T5");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyLPUSH(multiPKsResults);
		verifyListTTL(multiPKsResults.keySet(), 30);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of set type SADD command.
	 * @throws Exception
	 */
	@Test
	public void testSADD() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `score` BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '10 s',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'set'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `score`\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySADD(results);
		verifySetTTL(results.keySet(), 10);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of set type SADD command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testSADDWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `bonus` INT,\n" +
				"  `score` BIGINT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '5 s',\n" +
				"  'sink.buffer-flush.max-rows' = '3',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'set'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `bonus`, `score`\n" +
			"FROM T5");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifySADD(multiPKsResults);
		verifySetTTL(multiPKsResults.keySet(), 5);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of zset type ZADD command.
	 * @throws Exception
	 */
	@Test
	public void testZADD() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `rank` INT,\n" +
				"  `score` BIGINT\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'sink.record.ttl' = '10 s',\n" +
				"  'sink.buffer-flush.max-rows' = '4',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'zset'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `rank`, `score`\n" +
			"FROM T4");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyZADD(results2);
		verifyZSetTTL(results2.keySet(), 10);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	/**
	 * Test of zset type ZADD command with multiple primary keys.
	 * @throws Exception
	 */
	@Test
	public void testZADDWithMultiplePKs() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE sink (\n" +
				"  `name`  VARCHAR,\n" +
				"  `bonus` INT,\n" +
				"  `rank`  INT,\n" +
				"  `score` BIGINT,\n" +
				"  PRIMARY KEY (`name`, `bonus`) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'byte-abase',\n" +
				"  'cluster' = 'test',\n" +
				"  'table' = 'test',\n" +
				"  'key_format' = 'race_game:${name}:${bonus}',\n" +
				"  'sink.record.ttl' = '4 min',\n" +
				"  'sink.buffer-flush.max-rows' = '3',\n" +
				"  'sink.buffer-flush.interval' = '20 min',\n" +
				"  'value-type' = 'zset'\n" +
				")");

		TableResult tableResult = tEnv.executeSql("INSERT INTO sink\n" +
			"SELECT `name`, `bonus`, `rank`, `score`\n" +
			"FROM T5");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyZADD(multiPKsResults);
		verifyZSetTTL(multiPKsResults.keySet(), 240);
		Mockito.verify(clientPipeline, Mockito.times(1)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(1)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	private void verifySETEX(int ttl, Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> ttlArg = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.setex(keyArg.capture(), ttlArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<Integer> ttlIterator = ttlArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String k = new String(keyIterator.next());
			String v = new String(valIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Assert.assertEquals(ttl, (int) ttlIterator.next());
			Assert.assertEquals(results.get(k).get("score"), v);
		}
	}

	private void verifySETEXWithJsonFormat(int ttl, Map<String, Map<String, String>> results) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> ttlArg = ArgumentCaptor.forClass(Integer.class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.setex(keyArg.capture(), ttlArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<Integer> ttlIterator = ttlArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			Assert.assertEquals(ttl, (int) ttlIterator.next());
			String k = new String(keyIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Map<String, String> val = objectMapper.readValue(valIterator.next(),
				new TypeReference<Map<String, String>>() {});
			Assert.assertEquals(results.get(k), val);
		}
	}

	private void verifyHSET(Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<byte[]> fieldArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.hset(keyArg.capture(), fieldArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<byte[]> fieldIterator = fieldArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String k = new String(keyIterator.next());
			String f = new String(fieldIterator.next());
			String v = new String(valIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Assert.assertEquals("score", f);
			Assert.assertEquals(results.get(k).get(f), v);
		}
	}

	private void verifyHMSET(Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Map<byte[], byte[]>> valArg = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.hmset(keyArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<Map<byte[], byte[]>> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String user = new String(keyIterator.next());
			Assert.assertTrue(results.containsKey(user));
			Map<byte[], byte[]> props = valIterator.next();
			Assert.assertEquals(results.get(user).size(), props.size());
			for (Map.Entry<byte[], byte[]> entry : props.entrySet()) {
				String k = new String(entry.getKey());
				String v = new String(entry.getValue());
				Assert.assertTrue(results.get(user).containsKey(k));
				Assert.assertEquals(results.get(user).get(k), v);
			}
		}
	}

	/**
	 * The datastream is:
	 *
	 * <p>-- batch 1
	 *
	 * <p>INSERT: Bob, score, 10
	 *
	 * <p>DELETE: Bob, score, 10
	 *
	 * <p>INSERT: Bob, score, 25
	 *
	 * <p>DELETE: Bob, score, 25
	 *
	 * <p>-- batch 2
	 *
	 * <p>INSERT: Bob, score, 68
	 *
	 * <p>INSERT: Tom, score, 15
	 *
	 * <p>DELETE: Tom, score, 15
	 *
	 * <p>INSERT: Tom, score, 72
	 *
	 * <p>flush buffer size is 4,
	 *
	 * <p>thus, there is one deletion and two insertions.
	 */
	private void verifyHSETWithRetract() {
		Mockito.verify(clientPipeline, Mockito.times(1))
			.hdel("Bob".getBytes(), "score".getBytes());
		Mockito.verify(clientPipeline, Mockito.times(1))
			.hset("Bob".getBytes(), "score".getBytes(), "68".getBytes());
		Mockito.verify(clientPipeline, Mockito.times(1))
			.hset("Tom".getBytes(), "score".getBytes(), "72".getBytes());
	}

	/**
	 * The datastream is:
	 *
	 * <p>-- batch 1
	 *
	 * <p>INSERT: Bob, score=10,bonus=0
	 *
	 * <p>DELETE: Bob, score=10,bonus=0
	 *
	 * <p>INSERT: Bob, bonus=5,time=2022-01-10 00:10:00
	 *
	 * <p>DELETE: Bob, bonus=5,time=2022-01-10 00:10:00
	 *
	 * <p>-- batch 2
	 *
	 * <p>INSERT: Bob, bonus=20,time=2022-01-10 00:12:00
	 *
	 * <p>INSERT: Tom, score=15,bonus=10
	 *
	 * <p>DELETE: Tom, score=15,bonus=10
	 *
	 * <p>INSERT: Tom, bonus=30,time=2022-01-10 00:13:00
	 *
	 * <p>And flush buffer size is 4,
	 *
	 * <p>therefore, there is two deletions and two insertions.
	 */
	private void verifyHMSETWithRetract() {
		// check two deletions
		ArgumentCaptor<byte[]> delValArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(1))
			.hdel(eq("Bob".getBytes()), delValArg.capture());
		Assert.assertEquals(new HashSet<>(Arrays.asList("score", "time", "bonus")),
			delValArg.getAllValues().stream().map(String::new).collect(Collectors.toSet()));

		ArgumentCaptor<byte[]> delValArg2 = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(1))
			.hdel(eq("Tom".getBytes()), delValArg2.capture());
		Assert.assertEquals(1, delValArg2.getAllValues().size());
		Assert.assertEquals("score", new String(delValArg2.getValue()));

		// check two insertions
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Map<byte[], byte[]>> valArg = ArgumentCaptor.forClass(Map.class);
		Mockito.verify(clientPipeline, Mockito.times(2))
			.hmset(keyArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<Map<byte[], byte[]>> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String user = new String(keyIterator.next());
			Assert.assertTrue(insertions.containsKey(user));
			Map<byte[], byte[]> props = valIterator.next();
			Assert.assertEquals(insertions.get(user).size(), props.size());
			for (Map.Entry<byte[], byte[]> entry : props.entrySet()) {
				String k = new String(entry.getKey());
				String v = new String(entry.getValue());
				Assert.assertTrue(insertions.get(user).containsKey(k));
				Assert.assertEquals(insertions.get(user).get(k), v);
			}
		}
	}

	private void verifyHashTTL(Set<String> keys, int ttl) {
		ArgumentCaptor<byte[]> ttlKeyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Integer> ttlValArg = ArgumentCaptor.forClass(Integer.class);
		Mockito.verify(clientPipeline, Mockito.times(keys.size()))
			.hexpires(ttlKeyArg.capture(), ttlValArg.capture());
		for (byte[] key : ttlKeyArg.getAllValues()) {
			Assert.assertTrue(keys.contains(new String(key)));
		}
		for (int t : ttlValArg.getAllValues()) {
			Assert.assertEquals(ttl, t);
		}
	}

	private void verifyLPUSH(Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.lpush(keyArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String k = new String(keyIterator.next());
			String v = new String(valIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Assert.assertEquals(results.get(k).get("score"), v);
		}
	}

	private void verifyListTTL(Set<String> keys, int ttl) {
		ArgumentCaptor<String> ttlKeyArg = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<Integer> ttlValArg = ArgumentCaptor.forClass(Integer.class);
		Mockito.verify(clientPipeline, Mockito.times(keys.size()))
			.lexpires(ttlKeyArg.capture(), ttlValArg.capture());
		for (String key : ttlKeyArg.getAllValues()) {
			Assert.assertTrue(keys.contains(key));
		}
		for (int t : ttlValArg.getAllValues()) {
			Assert.assertEquals(ttl, t);
		}
	}

	private void verifySADD(Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.sadd(keyArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String k = new String(keyIterator.next());
			String v = new String(valIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Assert.assertEquals(results.get(k).get("score"), v);
		}
	}

	private void verifySetTTL(Set<String> keys, int ttl) {
		ArgumentCaptor<String> ttlKeyArg = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<Integer> ttlValArg = ArgumentCaptor.forClass(Integer.class);
		Mockito.verify(clientPipeline, Mockito.times(keys.size()))
			.sexpires(ttlKeyArg.capture(), ttlValArg.capture());
		for (String key : ttlKeyArg.getAllValues()) {
			Assert.assertTrue(keys.contains(key));
		}
		for (int t : ttlValArg.getAllValues()) {
			Assert.assertEquals(ttl, t);
		}
	}

	private void verifyZADD(Map<String, Map<String, String>> results) {
		ArgumentCaptor<byte[]> keyArg = ArgumentCaptor.forClass(byte[].class);
		ArgumentCaptor<Double> rankArg = ArgumentCaptor.forClass(Double.class);
		ArgumentCaptor<byte[]> valArg = ArgumentCaptor.forClass(byte[].class);
		Mockito.verify(clientPipeline, Mockito.times(results.size()))
			.zadd(keyArg.capture(), rankArg.capture(), valArg.capture());
		Iterator<byte[]> keyIterator = keyArg.getAllValues().listIterator();
		Iterator<Double> rankIterator = rankArg.getAllValues().listIterator();
		Iterator<byte[]> valIterator = valArg.getAllValues().listIterator();
		while (keyIterator.hasNext()) {
			String k = new String(keyIterator.next());
			double rank = rankIterator.next();
			String v = new String(valIterator.next());
			Assert.assertTrue(results.containsKey(k));
			Assert.assertEquals(Integer.parseInt(results.get(k).get("rank")), (int) rank);
			Assert.assertEquals(results.get(k).get("score"), v);
		}
	}

	private void verifyZSetTTL(Set<String> keys, int ttl) {
		ArgumentCaptor<String> ttlKeyArg = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<Integer> ttlValArg = ArgumentCaptor.forClass(Integer.class);
		Mockito.verify(clientPipeline, Mockito.times(keys.size()))
			.zexpires(ttlKeyArg.capture(), ttlValArg.capture());
		for (String key : ttlKeyArg.getAllValues()) {
			Assert.assertTrue(keys.contains(key));
		}
		for (int t : ttlValArg.getAllValues()) {
			Assert.assertEquals(ttl, t);
		}
	}

	private static Map<String, Map<String, String>> initResults() {
		Map<String, Map<String, String>> res = new HashMap<>();
		Map<String, String> props = new HashMap<>();
		props.put("score", "68");
		props.put("bonus", "20");
		res.put("Bob", props);

		Map<String, String> props1 = new HashMap<>();
		props1.put("score", "72");
		props1.put("bonus", "30");
		res.put("Tom", props1);

		Map<String, String> props2 = new HashMap<>();
		props2.put("score", "79");
		props2.put("bonus", "35");
		res.put("Lucy", props2);

		Map<String, String> props3 = new HashMap<>();
		props3.put("score", "63");
		props3.put("bonus", "15");
		res.put("Kim", props3);
		return res;
	}

	private static Map<String, Map<String, String>> initInsertions() {
		Map<String, Map<String, String>> res = new HashMap<>();
		Map<String, String> props = new HashMap<>();
		props.put("time", "2022-01-10 00:12:00");
		props.put("bonus", "20");
		res.put("Bob", props);

		Map<String, String> props1 = new HashMap<>();
		props1.put("time", "2022-01-10 00:13:00");
		props1.put("bonus", "30");
		res.put("Tom", props1);
		return res;
	}

	private static Map<String, Map<String, String>> initResults2() {
		Map<String, Map<String, String>> res = new HashMap<>();
		Map<String, String> props = new HashMap<>();
		props.put("score", "68");
		props.put("bonus", "20");
		props.put("rank", "8");
		res.put("Bob", props);

		Map<String, String> props1 = new HashMap<>();
		props1.put("score", "72");
		props1.put("bonus", "30");
		props1.put("rank", "6");
		res.put("Tom", props1);

		Map<String, String> props2 = new HashMap<>();
		props2.put("score", "79");
		props2.put("bonus", "35");
		props2.put("rank", "4");
		res.put("Lucy", props2);

		Map<String, String> props3 = new HashMap<>();
		props3.put("score", "63");
		props3.put("bonus", "15");
		props3.put("rank", "10");
		res.put("Kim", props3);
		return res;
	}

	private static Map<String, Map<String, String>> initMulPKResults() {
		Map<String, Map<String, String>> res = new HashMap<>();
		Map<String, String> props = new HashMap<>();
		props.put("score", "20");
		props.put("rank", "6");
		res.put("race_game:Bob:5", props);

		Map<String, String> props1 = new HashMap<>();
		props1.put("score", "35");
		props1.put("rank", "3");
		res.put("race_game:Bob:10", props1);

		Map<String, String> props2 = new HashMap<>();
		props2.put("score", "45");
		props2.put("rank", "2");
		res.put("race_game:Tom:10", props2);
		return res;
	}

}
