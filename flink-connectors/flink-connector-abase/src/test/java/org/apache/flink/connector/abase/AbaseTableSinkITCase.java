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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.abase.client.AbaseClientWrapper;
import org.apache.flink.connector.abase.client.ClientPipeline;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;

/**
 * The ITCase for {@link AbaseTableSink}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbaseClientTableUtils.class)
public class AbaseTableSinkITCase {

	// final result of source data stream
	private static final Map<String, Map<String, String>> results;
	private static final Map<String, Map<String, String>> insertions;

	static {
		results = initResults();
		insertions = initInsertions();
	}

	@Mock
	private AbaseClientWrapper abaseClientWrapper;

	@Mock
	private ClientPipeline clientPipeline;

	private StreamTableEnvironment tEnv;

	@Before
	public void setUp() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setParallelism(1);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		tEnv = StreamTableEnvironment.create(env, envSettings);

		Table t1 = tEnv.fromDataStream(get4TupleDataStream1(env),
			$("name"), $("score"), $("bonus"), $("time"));
		tEnv.createTemporaryView("T1", t1);

		Table t2 = tEnv.fromDataStream(get4TupleDataStream2(env),
			$("name"), $("score"), $("bonus"), $("time"));
		tEnv.createTemporaryView("T2", t2);

		PowerMockito.mockStatic(AbaseClientTableUtils.class);
		BDDMockito.given(AbaseClientTableUtils.getClientWrapper(notNull())).willReturn(abaseClientWrapper);
		Mockito.when(abaseClientWrapper.pipelined()).thenReturn(clientPipeline);
	}

	/**
	 * Test of hash type HSET command.
	 * @throws Exception
	 */
	@Test
	public void testHSET() throws Exception {
		tEnv.executeSql(
			"CREATE TABLE abase_hset (\n" +
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

		TableResult tableResult = tEnv.executeSql("INSERT INTO abase_hset\n" +
			"SELECT name, 'score', score\n" +
			"FROM T1");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHSET();
		verifyTTL(results.keySet(), 600);
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
			"CREATE TABLE abase_hmset (\n" +
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

		TableResult tableResult = tEnv.executeSql("INSERT INTO abase_hmset\n" +
			"SELECT name, MAP['score', CAST(score AS VARCHAR), 'bonus', CAST(bonus AS VARCHAR)]\n" +
			"FROM T1");

		// wait to finish
		Assert.assertTrue(tableResult.getJobClient().isPresent());
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();

		// verify pipeline method calls
		verifyHMSET();
		verifyTTL(results.keySet(), 60);
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
			"CREATE TABLE abase_hset (\n" +
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

		TableResult tableResult = tEnv.executeSql("INSERT INTO abase_hset\n" +
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
			"CREATE TABLE abase_hmset (\n" +
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

		TableResult tableResult = tEnv.executeSql("INSERT INTO abase_hmset\n" +
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
		verifyTTL(new HashSet<>(Arrays.asList("Bob", "Tom")), 60);
		Mockito.verify(clientPipeline, Mockito.times(4)).syncAndReturnAll();
		Mockito.verify(clientPipeline, Mockito.times(2)).close(); // check if close as expected.
		Mockito.verifyNoMoreInteractions(clientPipeline);
	}

	private void verifyHSET() {
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

	private void verifyHMSET() {
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

	private void verifyTTL(Set<String> keys, int ttl) {
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

	private static DataStream<Tuple4<String, Long, Integer, Timestamp>> get4TupleDataStream1(StreamExecutionEnvironment env) {
		List<Tuple4<String, Long, Integer, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple4<>("Bob", 10L, 0, Timestamp.valueOf("2022-01-10 00:01:00.000")));
		data.add(new Tuple4<>("Tom", 22L, 10, Timestamp.valueOf("2022-01-10 00:02:00.000")));
		data.add(new Tuple4<>("Kim", 15L, 5, Timestamp.valueOf("2022-01-10 00:03:00.000")));
		data.add(new Tuple4<>("Kim", 47L, 10, Timestamp.valueOf("2022-01-10 00:04:00.000")));
		data.add(new Tuple4<>("Lucy", 19L, 20, Timestamp.valueOf("2022-01-10 00:05:00.000")));
		data.add(new Tuple4<>("Tom", 34L, 15, Timestamp.valueOf("2022-01-10 00:06:00.000")));
		data.add(new Tuple4<>("Lucy", 76L, 30, Timestamp.valueOf("2022-01-10 00:07:00.000")));
		data.add(new Tuple4<>("Kim", 54L, 10, Timestamp.valueOf("2022-01-10 00:08:00.000")));
		data.add(new Tuple4<>("Lucy", 78L, 30, Timestamp.valueOf("2022-01-10 00:09:00.000")));
		data.add(new Tuple4<>("Bob", 25L, 5, Timestamp.valueOf("2022-01-10 00:10:00.000")));
		data.add(new Tuple4<>("Bob", 37L, 10, Timestamp.valueOf("2022-01-10 00:11:00.000")));
		data.add(new Tuple4<>("Bob", 68L, 20, Timestamp.valueOf("2022-01-10 00:12:00.000")));
		data.add(new Tuple4<>("Tom", 72L, 30, Timestamp.valueOf("2022-01-10 00:13:00.000")));
		data.add(new Tuple4<>("Lucy", 79L, 35, Timestamp.valueOf("2022-01-10 00:14:00.000")));
		data.add(new Tuple4<>("Kim", 60L, 15, Timestamp.valueOf("2022-01-10 00:15:00.000")));
		data.add(new Tuple4<>("Kim", 63L, 15, Timestamp.valueOf("2022-01-10 00:16:00.000")));
		return env.fromCollection(data);
	}

	private static DataStream<Tuple4<String, Long, Integer, Timestamp>> get4TupleDataStream2(StreamExecutionEnvironment env) {
		List<Tuple4<String, Long, Integer, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple4<>("Bob", 10L, 0, Timestamp.valueOf("2022-01-10 00:01:00.000")));
		data.add(new Tuple4<>("Bob", 25L, 5, Timestamp.valueOf("2022-01-10 00:10:00.000")));
		data.add(new Tuple4<>("Bob", 68L, 20, Timestamp.valueOf("2022-01-10 00:12:00.000")));
		data.add(new Tuple4<>("Tom", 15L, 10, Timestamp.valueOf("2022-01-10 00:02:00.000")));
		data.add(new Tuple4<>("Tom", 72L, 30, Timestamp.valueOf("2022-01-10 00:13:00.000")));
		return env.fromCollection(data);
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

}
