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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.abase.client.AbaseClientWrapper;
import org.apache.flink.connector.abase.client.ClientPipeline;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.mockito.ArgumentMatchers.notNull;

/**
 * Base class for abase test.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AbaseClientTableUtils.class)
public abstract class AbaseTestBase {

	@Rule
	public TestName name = new TestName();

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Mock
	protected AbaseClientWrapper abaseClientWrapper;

	@Mock
	protected ClientPipeline clientPipeline;

	protected StreamTableEnvironment tEnv;

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

		Table t3 = tEnv.fromDataStream(get5TupleDataStream(env),
			$("name"), $("score"), $("bonus"), $("rank"), $("time"));
		tEnv.createTemporaryView("T3", t3);

		Table t4 = tEnv.fromDataStream(get5TupleDataStream2(env),
			$("name"), $("score"), $("bonus"), $("rank"), $("time"));
		tEnv.createTemporaryView("T4", t4);

		Table t5 = tEnv.fromDataStream(get5TupleDataStream3(env),
			$("name"), $("score"), $("bonus"), $("rank"), $("time"));
		tEnv.createTemporaryView("T5", t5);

		PowerMockito.mockStatic(AbaseClientTableUtils.class);
		BDDMockito.given(AbaseClientTableUtils.getClientWrapper(notNull())).willReturn(abaseClientWrapper);
		Mockito.when(abaseClientWrapper.pipelined()).thenReturn(clientPipeline);
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

	private static DataStream<Tuple5<String, Long, Integer, Integer, Timestamp>> get5TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple5<String, Long, Integer, Integer, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple5<>("Bob", 10L, 5, 7, Timestamp.valueOf("2022-01-10 00:01:00.000")));
		data.add(new Tuple5<>("Tom", 15L, 10, 4, Timestamp.valueOf("2022-01-10 00:02:00.000")));
		data.add(new Tuple5<>("Bob", 20L, 5, 6, Timestamp.valueOf("2022-01-10 00:10:00.000")));
		data.add(new Tuple5<>("Bob", 35L, 10, 3, Timestamp.valueOf("2022-01-10 00:12:00.000")));
		data.add(new Tuple5<>("Tom", 45L, 10, 2, Timestamp.valueOf("2022-01-10 00:13:00.000")));
		return env.fromCollection(data);
	}

	private static DataStream<Tuple5<String, Long, Integer, Integer, Timestamp>> get5TupleDataStream2(StreamExecutionEnvironment env) {
		List<Tuple5<String, Long, Integer, Integer, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple5<>("Bob", 68L, 20, 8, Timestamp.valueOf("2022-01-10 00:12:00.000")));
		data.add(new Tuple5<>("Tom", 72L, 30, 6, Timestamp.valueOf("2022-01-10 00:13:00.000")));
		data.add(new Tuple5<>("Lucy", 79L, 35, 4, Timestamp.valueOf("2022-01-10 00:14:00.000")));
		data.add(new Tuple5<>("Kim", 63L, 15, 10, Timestamp.valueOf("2022-01-10 00:16:00.000")));
		return env.fromCollection(data);
	}

	private static DataStream<Tuple5<String, Long, Integer, Integer, Timestamp>> get5TupleDataStream3(StreamExecutionEnvironment env) {
		List<Tuple5<String, Long, Integer, Integer, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple5<>("Bob", 20L, 5, 6, Timestamp.valueOf("2022-01-10 00:10:00.000")));
		data.add(new Tuple5<>("Bob", 35L, 10, 3, Timestamp.valueOf("2022-01-10 00:12:00.000")));
		data.add(new Tuple5<>("Tom", 45L, 10, 2, Timestamp.valueOf("2022-01-10 00:13:00.000")));
		return env.fromCollection(data);
	}
}
