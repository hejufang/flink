/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.rpc.table;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.rpc.thrift.client.RPCServiceClientMock;
import org.apache.flink.connector.rpc.thrift.generated.Base;
import org.apache.flink.connector.rpc.thrift.generated.InnerTestStruct;
import org.apache.flink.connector.rpc.thrift.generated.SimpleStruct;
import org.apache.flink.connector.rpc.thrift.generated.TestStruct;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * IT Tests for {@link RPCDynamicTableSource}.
 * Only sync mode is tested here.
 */
public class RPCDynamicTableSourceITTest {
	private final Base commonBase = new Base("", "test", "", "");
	private Field baseField;
	private Field logIDField;

	private static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
		data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
		return env.fromCollection(data);
	}

	@Before
	public void setup() {
		try {
			baseField = TestStruct.class.getField("base");
			logIDField = baseField.getType().getField("LogID");
		} catch (NoSuchFieldException ignored) {
		}
		commonBase.setExtra(new HashMap<String, String>(){{
			put("idc", "");
			put("user_extra", "{\"RPC_TRANSIT_gdpr-token\":null,\"dest_service\":\"test\"," +
				"\"dest_cluster\":\"default\",\"dest_method\":\"testFunc\"}");
		}});
	}

	@Test
	public void testLookupWithSchemaInfer() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

		tEnv.createTemporaryView("source", t);
		tEnv.sql("CREATE TABLE rpc_table\n" +
			"        WITH(\n" +
			"            'connector' = 'rpc',\n" +
			"            'psm' = 'test',\n" +
			"            'cluster' = 'default',\n" +
			"            'consul' = 'test',\n" +
			"            'service-client-impl.class' = 'org.apache.flink.connector.rpc.thrift.client.RPCServiceClientMock',\n" +
			"            'thrift.service-class' = 'org.apache.flink.connector.rpc.thrift.generated.TestService',\n" +
			"            'thrift.method' = 'testFunc'\n" +
			"        );\n" +
			"CREATE  TABLE print_sink (\n" +
			"            innerTestStruct ROW<boolVal BOOLEAN, intVal INT, mapVal MAP<VARCHAR, BIGINT>, listVal ARRAY<BIGINT>>\n" +
			"        )\n" +
			"        WITH (\n" +
			"			 'connector' = 'values',\n" +
			"   		 'sink-insert-only' = 'false'\n" +
			"		 );\n" +
			"CREATE  VIEW join_view AS\n" +
			"SELECT" +
			"       D.innerTestStruct\n" +
			"FROM    (\n" +
			"            SELECT" +
			"				 text,\n" +
			"				 proctime() as proc,\n" +
			"				 ROW(\n" +
			"				     true,\n" +
			"				     id,\n" +
			"				     MAP['num', num],\n" +
			"				     ARRAY[num]\n" +
			"				 ) AS innerTestStruct\n" +
			"            FROM source\n" +
			"        ) T\n" +
			"LEFT JOIN rpc_table FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON      D.strVal = text\n" +
			"AND     D.innerTestStruct = T.innerTestStruct;\n"
		);

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO print_sink\n" +
				"SELECT  *\n" +
				"FROM    join_view");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		List<TestStruct> expectedRequests = new ArrayList<>();
		TestStruct first = new TestStruct();
		first.setBase(commonBase);
		first.setStrVal("Hi");
		InnerTestStruct firstInnerStruct = new InnerTestStruct(true);
		firstInnerStruct.setIntVal(1);
		firstInnerStruct.setMapVal(new HashMap<String, Long>(){{put("num", 1L); }});
		firstInnerStruct.setListVal(new ArrayList<Long>(){{add(1L); }});
		first.setInnerTestStruct(firstInnerStruct);
		expectedRequests.add(first);
		TestStruct second = new TestStruct();
		second.setBase(commonBase);
		second.setStrVal("Hello");
		InnerTestStruct secondInnerStruct = new InnerTestStruct(true);
		secondInnerStruct.setIntVal(2);
		secondInnerStruct.setMapVal(new HashMap<String, Long>(){{put("num", 2L); }});
		secondInnerStruct.setListVal(new ArrayList<Long>(){{add(2L); }});
		second.setInnerTestStruct(secondInnerStruct);
		expectedRequests.add(second);
		List<TestStruct> actualRequests = (List<TestStruct>) RPCServiceClientMock
			.getInstance(null, null, null)
			.getRequests(2);
		verifyRequests(expectedRequests, actualRequests, baseField, logIDField);
		List<String> expectedOutput = new ArrayList<String>(){{
			add("true,1,{num=1},[1]");
			add("true,2,{num=2},[2]");
		}};
		assertEquals(expectedOutput, TestValuesTableFactory.getResults("print_sink"));
	}

	@Test
	public void testLookupWithoutSchemaInfer() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

		tEnv.createTemporaryView("source", t);
		tEnv.sql("CREATE TABLE rpc_table(\n" +
			"			  base ROW<LogID VARCHAR, Caller VARCHAR, Addr VARCHAR, Client VARCHAR, Extra MAP<VARCHAR, VARCHAR>>,\n" +
			"			  strVal VARCHAR,\n" +
			"			  mapWithList MAP<VARCHAR, ARRAY<BIGINT>>,\n" +
			"			  innerTestStruct ROW<boolVal BOOLEAN, mapVal MAP<VARCHAR, BIGINT>>,\n" +
			"			  response ROW<mapWithList MAP<VARCHAR, ARRAY<BIGINT>>>\n" +
			"		 )\n" +
			"        WITH(\n" +
			"            'connector' = 'rpc',\n" +
			"            'psm' = 'test',\n" +
			"            'cluster' = 'default',\n" +
			"            'consul' = 'test',\n" +
			"            'service-client-impl.class' = 'org.apache.flink.connector.rpc.thrift.client.RPCServiceClientMock',\n" +
			"            'thrift.service-class' = 'org.apache.flink.connector.rpc.thrift.generated.TestService',\n" +
			"            'thrift.method' = 'testFunc',\n" +
			"			 'lookup.infer-schema' = 'false'\n" +
			"        );\n" +
			"CREATE  TABLE print_sink (\n" +
			"            res ARRAY<BIGINT>\n" +
			"        )\n" +
			"        WITH (\n" +
			"			 'connector' = 'values',\n" +
			"   		 'sink-insert-only' = 'false'\n" +
			"		 );\n" +
			"CREATE  VIEW join_view AS\n" +
			"SELECT" +
			"       D.mapWithList[text]\n" +
			"FROM    (\n" +
			"            SELECT" +
			"				 text,\n" +
			"				 proctime() as proc,\n" +
			"				 MAP[text, ARRAY[num]] AS mapWithList,\n" +
			"				 ROW(\n" +
			"				     false,\n" +
			"				     MAP['num', num]\n" +
			"				 ) AS innerTestStruct\n" +
			"            FROM source\n" +
			"        ) T\n" +
			"LEFT JOIN rpc_table FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON      D.strVal = text\n" +
			"AND     D.innerTestStruct = T.innerTestStruct\n" +
			"AND	 D.mapWithList = T.mapWithList;\n"
		);

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO print_sink\n" +
				"SELECT  *\n" +
				"FROM    join_view");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		List<TestStruct> expectedRequests = new ArrayList<>();
		TestStruct first = new TestStruct();
		first.setBase(commonBase);
		first.setStrVal("Hi");
		InnerTestStruct firstInnerStruct = new InnerTestStruct(false);
		firstInnerStruct.setMapVal(new HashMap<String, Long>(){{put("num", 1L); }});
		first.setInnerTestStruct(firstInnerStruct);
		first.setMapWithList(new HashMap<String, List<Long>>(){{
			put("Hi", new ArrayList<Long>(){{ add(1L); }});
		}});
		expectedRequests.add(first);
		TestStruct second = new TestStruct();
		second.setBase(commonBase);
		second.setStrVal("Hello");
		InnerTestStruct secondInnerStruct = new InnerTestStruct(false);
		secondInnerStruct.setMapVal(new HashMap<String, Long>(){{put("num", 2L); }});
		second.setInnerTestStruct(secondInnerStruct);
		second.setMapWithList(new HashMap<String, List<Long>>(){{
			put("Hello", new ArrayList<Long>(){{ add(2L); }});
		}});
		expectedRequests.add(second);
		List<TestStruct> actualRequests = (List<TestStruct>) RPCServiceClientMock
			.getInstance(null, null, null)
			.getRequests(2);
		verifyRequests(expectedRequests, actualRequests, baseField, logIDField);
		List<String> expectedOutput = new ArrayList<String>(){{
			add("[1]");
			add("[2]");
		}};
		assertEquals(expectedOutput, TestValuesTableFactory.getResults("print_sink"));
	}

	@Test
	public void testBatchLookup() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.resource.default-parallelism", "1");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "300s");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

		tEnv.createTemporaryView("source", t);
		tEnv.sql("CREATE TABLE rpc_table\n" +
			"        WITH(\n" +
			"            'connector' = 'rpc',\n" +
			"            'psm' = 'test',\n" +
			"            'cluster' = 'default',\n" +
			"            'consul' = 'test',\n" +
			"            'service-client-impl.class' = 'org.apache.flink.connector.rpc.thrift.client.RPCServiceClientMock',\n" +
			"            'thrift.service-class' = 'org.apache.flink.connector.rpc.thrift.generated.TestService',\n" +
			"            'thrift.method' = 'testFunc',\n" +
			"            'lookup.batch-mode.enabled' = 'true',\n" +
			"            'lookup.batch-size' = '2',\n" +
			"            'lookup.batch-request-field-name' = 'listWithStruct',\n" +
			"            'lookup.batch-response-field-name' = 'listWithStruct'\n" +
			"        );\n" +
			"CREATE  TABLE print_sink (\n" +
			"            longVal BIGINT,\n" +
			"            biVal VARBINARY\n" +
			"        )\n" +
			"        WITH (\n" +
			"			 'connector' = 'values',\n" +
			"   		 'sink-insert-only' = 'false'\n" +
			"		 );\n" +
			"CREATE  VIEW join_view AS\n" +
			"SELECT" +
			"       D.longVal,\n" +
			"       D.biVal\n" +
			"FROM    (\n" +
			"            SELECT\n" +
			"				 num,\n" +
			"				 text,\n" +
			"				 proctime() as proc\n" +
			"            FROM source\n" +
			"        ) T\n" +
			"LEFT JOIN rpc_table FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON      D.longVal = T.num\n" +
			"AND     D.biVal = ENCODE(T.text, 'US-ASCII');\n"
		);

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO print_sink\n" +
				"SELECT  *\n" +
				"FROM    join_view");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		List<TestStruct> expectedRequests = new ArrayList<>();
		TestStruct first = new TestStruct();
		first.setBase(commonBase);
		List<SimpleStruct> firstInnerList = new ArrayList<SimpleStruct>(){{
			add(new SimpleStruct(1, ByteBuffer.wrap(new byte[]{72, 105})));
			add(new SimpleStruct(2, ByteBuffer.wrap(new byte[]{72, 101, 108, 108, 111})));
		}};
		first.setListWithStruct(firstInnerList);
		expectedRequests.add(first);
		List<TestStruct> actualRequests = (List<TestStruct>) RPCServiceClientMock
			.getInstance(null, null, null)
			.getRequests(1);
		verifyRequests(expectedRequests, actualRequests, baseField, logIDField);
		List<String> expectedOutput = new ArrayList<String>(){{
			add("1,[72, 105]");
			add("2,[72, 101, 108, 108, 111]");
		}};
		assertEquals(expectedOutput, TestValuesTableFactory.getResults("print_sink"));
	}

	@Test
	public void testBatchLookupWithCache() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		env.getConfig().setParallelism(1);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.resource.default-parallelism", "1");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "300s");
		tEnv.getConfig().getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		Table t = tEnv.fromDataStream(get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

		tEnv.createTemporaryView("source", t);
		tEnv.sql("CREATE TABLE rpc_table\n" +
			"        WITH(\n" +
			"            'connector' = 'rpc',\n" +
			"            'psm' = 'test',\n" +
			"            'cluster' = 'default',\n" +
			"            'consul' = 'test',\n" +
			"            'service-client-impl.class' = 'org.apache.flink.connector.rpc.thrift.client.RPCServiceClientMock',\n" +
			"            'thrift.service-class' = 'org.apache.flink.connector.rpc.thrift.generated.TestService',\n" +
			"            'thrift.method' = 'testFunc',\n" +
			"    		 'lookup.cache.max-rows' = '5000000',\n" +
			"    		 'lookup.cache.ttl' = '3600000',\n" +
			"            'lookup.batch-mode.enabled' = 'true',\n" +
			"            'lookup.batch-size' = '2',\n" +
			"            'lookup.batch-request-field-name' = 'listWithStruct',\n" +
			"            'lookup.batch-response-field-name' = 'listWithStruct'\n" +
			"        );\n" +
			"CREATE  TABLE print_sink (\n" +
			"            longVal BIGINT,\n" +
			"            biVal VARBINARY\n" +
			"        )\n" +
			"        WITH (\n" +
			"			 'connector' = 'values',\n" +
			"   		 'sink-insert-only' = 'false'\n" +
			"		 );\n" +
			"CREATE VIEW nest_view AS\n" +
			"SELECT\n" +
			"		ARRAY[ROW(12, ENCODE('Hi', 'US-ASCII')), ROW(num, ENCODE(text, 'US-ASCII'))] as key_array,\n" +
			"		proctime() as proc\n" +
			"FROM source;\n" +
			"CREATE VIEW join_view AS\n" +
			"SELECT\n" +
			"       D.longVal,\n" +
			"       D.biVal\n" +
			"FROM    (\n" +
			"            SELECT *\n" +
			"            FROM nest_view CROSS JOIN UNNEST(key_array) as t(longVal, biVal)\n" +
			"        ) T\n" +
			"LEFT JOIN rpc_table FOR SYSTEM_TIME AS OF proc AS D\n" +
			"ON      D.longVal = T.longVal\n" +
			"AND     D.biVal = T.biVal;\n"
		);

		TableResult tableResult = tEnv.executeSql(
			"INSERT INTO print_sink\n" +
				"SELECT  *\n" +
				"FROM    join_view");
		// wait to finish
		tableResult.getJobClient().get().getJobExecutionResult(Thread.currentThread().getContextClassLoader()).get();
		List<TestStruct> expectedRequests = new ArrayList<>();
		TestStruct first = new TestStruct();
		first.setBase(commonBase);
		List<SimpleStruct> firstInnerList = new ArrayList<SimpleStruct>(){{
			add(new SimpleStruct(12, ByteBuffer.wrap(new byte[]{72, 105})));
			add(new SimpleStruct(1, ByteBuffer.wrap(new byte[]{72, 105})));
		}};
		first.setListWithStruct(firstInnerList);
		expectedRequests.add(first);
		TestStruct second = new TestStruct();
		second.setBase(commonBase);
		List<SimpleStruct> secondInnerList = new ArrayList<SimpleStruct>(){{
			add(new SimpleStruct(2, ByteBuffer.wrap(new byte[]{72, 101, 108, 108, 111})));
		}};
		second.setListWithStruct(secondInnerList);
		expectedRequests.add(second);
		List<TestStruct> actualRequests = (List<TestStruct>) RPCServiceClientMock
			.getInstance(null, null, null)
			.getRequests(2);
		verifyRequests(expectedRequests, actualRequests, baseField, logIDField);
		List<String> expectedOutput = new ArrayList<String>(){{
			add("12,[72, 105]");
			add("1,[72, 105]");
			add("12,[72, 105]");
			add("2,[72, 101, 108, 108, 111]");
		}};
		assertEquals(expectedOutput, TestValuesTableFactory.getResults("print_sink"));
	}


	/**
	 * Used to compare requests. Note that non-deterministic fields like generated logID will be ignored.
	 */
	public <T> void verifyRequests(
			List<T> expectedList,
			List<T> actualList,
			Field baseField,
			Field logIDField) throws Exception {
		if (expectedList.size() != actualList.size()) {
			throw new ComparisonFailure(String.format("The size of the expected list was %d, but actual one was %d.",
				expectedList.size(), actualList.size()), expectedList.toString(), actualList.toString());
		}
		for (int i = 0; i < expectedList.size(); i++) {
			T expected = expectedList.get(i);
			T actual = actualList.get(i);
			// Ignore the logID field by setting expected logID as actual.
			Object actualLogID = logIDField.get(baseField.get(actual));
			logIDField.set(baseField.get(expected), actualLogID);
			if (!expected.equals(actual)) {
				throw new ComparisonFailure(String.format("Elements at pos %d are different.", i), expected.toString(), actual.toString());
			}
		}
	}
}
