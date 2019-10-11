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
}
