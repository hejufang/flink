/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for {@link RedisUpsertTableSink}.
 */
public class RedisUpsertTableSinkITCase extends AbstractTestBase {
	@Test
	public void testUpsert() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple2<Integer, String>> ds = getTuple2DataStream(env);
		Table t = tEnv.fromDataStream(ds, "id, text");
		tEnv.registerTable("source", t);
		tEnv.sqlUpdate("create table redis_test(" +
			"id int," +
			"text varchar" +
			") with (" +
			"'connector.type' = 'redis'," +
			"'connector.cluster' = 'toutiao.redis.data_inf_dtop.service.lf'," +
			"'connector.batch-size' = '1'" +
			")");

		tEnv.sqlUpdate("insert into redis_test select id, text from source");
		tEnv.execute("Redis upsert sink test");
	}

	public static DataStream<Tuple2<Integer, String>> getTuple2DataStream(StreamExecutionEnvironment env) {
		List<Tuple2<Integer, String>> data = new ArrayList<>();
		data.add(new Tuple2<>(1, "Hi"));
		data.add(new Tuple2<>(2, "Hello"));
		data.add(new Tuple2<>(3, "Hello world"));
		data.add(new Tuple2<>(4, "Hello world, how are you?"));
		data.add(new Tuple2<>(5, "I am fine."));
		data.add(new Tuple2<>(6, "Luke Skywalker"));
		data.add(new Tuple2<>(7, "Comment#1"));
		data.add(new Tuple2<>(8, "Comment#2"));
		data.add(new Tuple2<>(9, "Comment#3"));
		data.add(new Tuple2<>(10, "Comment#4"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	@Test
	public void testUpsertList() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple2<Integer, String>> ds = getTuple2DataStreamList(env);
		Table t = tEnv.fromDataStream(ds, "id, text");
		tEnv.registerTable("source", t);
		tEnv.sqlUpdate("create table redis_test(" +
			"id int," +
			"text varchar" +
			") with (" +
			"'connector.type' = 'redis'," +
			"'connector.cluster' = 'toutiao.redis.data_inf_dtop.service.lf'," +
			"'connector.redis-data-type' = 'list'," +
			"'connector.ttl-seconds' = '20'," +
			"'connector.batch-size' = '1'" +
			")");

		tEnv.sqlUpdate("insert into redis_test select id, text from source");
		tEnv.execute("Redis upsert sink test");
	}

	public static DataStream<Tuple2<Integer, String>> getTuple2DataStreamList(StreamExecutionEnvironment env) {
		List<Tuple2<Integer, String>> data = new ArrayList<>();

		data.add(new Tuple2<>(747879583, "Hi")); //list
		data.add(new Tuple2<>(747879583, "Hello"));
		data.add(new Tuple2<>(747879583, "Hello world"));
		data.add(new Tuple2<>(747879583, "Hello world, how are you?"));
		data.add(new Tuple2<>(747879583, "I am fine."));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	@Test
	public void testUpsertHash() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple3<Integer, String, String>> ds = getTuple3DataStreamHash(env);
		Table t = tEnv.fromDataStream(ds, "id, hash_key, hash_value");
		tEnv.registerTable("source", t);
		tEnv.sqlUpdate("create table redis_test(" +
			"id int," +
			"hash_key VARCHAR," +
			"hash_value VARCHAR" +
			") with (" +
			"'connector.type' = 'redis'," +
			"'connector.cluster' = 'toutiao.redis.data_inf_dtop.service.lf'," +
			"'connector.redis-data-type' = 'hash'," +
			"'connector.ttl-seconds' = '20'," +
			"'connector.batch-size' = '1'" +
			")");

		tEnv.sqlUpdate("insert into redis_test select id, hash_key, hash_value from source");
		tEnv.execute("Redis upsert sink test");
	}

	public static DataStream<Tuple3<Integer, String, String>>
			getTuple3DataStreamHash(StreamExecutionEnvironment env) {
		List<Tuple3<Integer, String, String>> data = new ArrayList<>();

		data.add(new Tuple3<>(747879584, "1", "Hi")); //hash
		data.add(new Tuple3<>(747879584, "2", "Hello"));
		data.add(new Tuple3<>(747879584, "3", "Hello world"));
		data.add(new Tuple3<>(747879584, "4", "Hello world, how are you?"));
		data.add(new Tuple3<>(747879584, "5", "I am fine."));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	@Test
	public void testUpsertSet() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple2<Integer, String>> ds = getTuple2DataStreamSet(env);
		Table t = tEnv.fromDataStream(ds, "id, text");
		tEnv.registerTable("source", t);
		tEnv.sqlUpdate("create table redis_test(" +
			"id int," +
			"text varchar" +
			") with (" +
			"'connector.type' = 'redis'," +
			"'connector.cluster' = 'toutiao.redis.data_inf_dtop.service.lf'," +
			"'connector.redis-data-type' = 'set'," +
			"'connector.ttl-seconds' = '20'," +
			"'connector.batch-size' = '1'" +
			")");

		tEnv.sqlUpdate("insert into redis_test select id, text from source");
		tEnv.execute("Redis upsert sink test");
	}

	public static DataStream<Tuple2<Integer, String>> getTuple2DataStreamSet(StreamExecutionEnvironment env) {
		List<Tuple2<Integer, String>> data = new ArrayList<>();

		data.add(new Tuple2<>(747879585, "Hi")); //set
		data.add(new Tuple2<>(747879585, "Hi"));
		data.add(new Tuple2<>(747879585, "Hello world"));
		data.add(new Tuple2<>(747879585, "Hello world, how are you?"));
		data.add(new Tuple2<>(747879585, "Hello world"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}

	@Test
	public void testUpsertZset() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple3<Integer, Double, String>> ds = getTuple3DataStreamZset(env);
		Table t = tEnv.fromDataStream(ds, "id, score, text");
		tEnv.registerTable("source", t);
		tEnv.sqlUpdate("create table redis_test(" +
			"id int," +
			"score DOUBLE," +
			"text VARCHAR" +
			") with (" +
			"'connector.type' = 'redis'," +
			"'connector.cluster' = 'toutiao.redis.data_inf_dtop.service.lf'," +
			"'connector.redis-data-type' = 'zset'," +
			"'connector.ttl-seconds' = '20'," +
			"'connector.batch-size' = '1'" +
			")");

		tEnv.sqlUpdate("insert into redis_test select id, score, text from source");
		tEnv.execute("Redis upsert sink test");
	}

	public static DataStream<Tuple3<Integer, Double, String>>
			getTuple3DataStreamZset(StreamExecutionEnvironment env) {
		List<Tuple3<Integer, Double, String>> data = new ArrayList<>();

		data.add(new Tuple3<>(747879586, 2.0, "second")); //zset
		data.add(new Tuple3<>(747879586, 3.0, "third1"));
		data.add(new Tuple3<>(747879586, 1.0, "first"));
		data.add(new Tuple3<>(747879586, 3.0, "third2"));
		data.add(new Tuple3<>(747879586, 4.0, "forth"));

		Collections.shuffle(data);
		return env.fromCollection(data);
	}
}
