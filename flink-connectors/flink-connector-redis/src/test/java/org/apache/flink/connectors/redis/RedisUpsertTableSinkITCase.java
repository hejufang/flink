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
			"'connector.batch-size' = '1'," +
			"'connector.psm' = 'inf.compute.test'" +
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
