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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * RPC sink example.
 */
public class RPCTableSinkExample {
	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.STRING, Types.LONG, Types.DOUBLE, Types.BOOLEAN,
			Types.OBJECT_ARRAY(Types.STRING), Types.MAP(Types.STRING, Types.STRING),
			Types.ROW(Types.STRING, Types.ROW(Types.BOOLEAN, Types.STRING))},
		new String[]{"intTest", "stringTest", "longTest", "doubleTest", "booleanTest", "arrayTest", "mapTest", "rowTest"});

	private static final List<Row> testData2 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo2 = new RowTypeInfo(
		new TypeInformation[]{Types.OBJECT_ARRAY(Types.STRING), Types.OBJECT_ARRAY(Types.LONG),
			Types.OBJECT_ARRAY(Types.INT), Types.OBJECT_ARRAY(Types.INT), Types.OBJECT_ARRAY(Types.LONG),
			Types.INT, Types.STRING, Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING,
				Types.ROW(Types.BOOLEAN, Types.STRING), Types.MAP(Types.STRING, Types.STRING))},
		new String[]{"counterNames", "itemIds", "timeArray", "dailyArray", "keyArray", "cacheExpireTime", "cluster", "Base"});

	private static final List<Row> testData3 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo3 = new RowTypeInfo(
		new TypeInformation[]{Types.OBJECT_ARRAY(Types.STRING), Types.OBJECT_ARRAY(Types.LONG),
			Types.INT, Types.STRING, Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING,
			Types.ROW(Types.BOOLEAN, Types.STRING), Types.MAP(Types.STRING, Types.STRING))},
		new String[]{"counterNames", "itemIds", "cacheExpireTime", "cluster", "Base"});

	static {
		String[] testArray = {"test1", "test2", "test3"};
		Map<String, String> testMap = new HashMap<>();
		testMap.put("key1", "value1");
		testMap.put("key2", "value2");
		Row row = Row.of("baseId", Row.of(true, "innerBaseValue"));
		testData1.add(Row.of(1, "Hello-1", 100L, 1.01, false, testArray, testMap, row));
		testData1.add(Row.of(2, "Hello-2", 200L, 2.02, true,  testArray, testMap, row));
		testData1.add(Row.of(3, "Hello-3", 300L, 3.03, false, testArray, testMap, row));
		testData1.add(Row.of(4, "Hello-4", 400L, 4.04, true,  testArray, testMap, row));
		testData1.add(Row.of(5, "Hello-1", 100L, 1.01, false, testArray, testMap, row));
		testData1.add(Row.of(6, "Hello-2", 200L, 2.02, true,  testArray, testMap, row));
		testData1.add(Row.of(7, "Hello-3", 300L, 3.03, false, testArray, testMap, row));
		testData1.add(Row.of(8, "Hello-4", 400L, 4.04, true,  testArray, testMap, row));
		String[] counterNames = {"group_impression_count"};
		Long[] itemIds = {6492274619347632398L};
		Row base = Row.of("", "data.online_joiner.test", "", "", null, null);
		testData2.add(Row.of(counterNames, itemIds, null, null, null, 10000, "ONLINE", base));
		testData3.add(Row.of(counterNames, itemIds, 10000, "ONLINE", base));
	}

	/**
	 * This is an example of rpc sink in single situation. This example only executable in local environment.
	 * So it doesn't support consul. And user need to change the code in ThriftRPCClient to make hostPorts which
	 * should add manually instead of use RPCDiscovery to find.
	 * @throws Exception
	 */
	public void testSingleRPCSink() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);

		String sinkDDL = "create table rpc_test with (\n" +
			"'connector.type' = 'rpc', \n" +
			"'connector.consul' = 'TestLocal', \n" +
			"'connector.thrift-service-class' = 'com.bytedance.flink.test.thrift.HelloWordService', \n" +
			"'connector.thrift-method' = 'doAction', \n" +
			"'connector.response-value' = '{\"head\":{\"code\":\"300\"}}' \n" +
			")";
		tEnv.sqlUpdate(sinkDDL);

		//ROW(stringTest, ROW(booleanTest,stringTest))
		String query = "INSERT INTO rpc_test " +
			"SELECT ROW(stringTest, intTest), stringTest, intTest, arrayTest, arrayTest, mapTest, rowTest " +
			"FROM src";
		tEnv.sqlUpdate(query);
		tEnv.execute("rpc sink test");
	}

	/**
	 * This is an example of rpc sink in batch situation. This example only executable in local environment.
	 * So it doesn't support consul. And user need to change the code in ThriftRPCClient to make hostPorts which
	 * should add manually instead of use RPCDiscovery to find.
	 * @throws Exception
	 */
	public void testBatchRPCSink() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);

		String sinkDDL = "create table rpc_test with (\n" +
			"'connector.type' = 'rpc', \n" +
			"'connector.consul' = 'TestLocal', \n" +
			"'connector.thrift-service-class' = 'com.bytedance.flink.test.thrift.HelloWordService', \n" +
			"'connector.batch-class' = 'com.bytedance.flink.test.thrift.Request', \n" +
			"'connector.batch-size' = '2', \n" +
			"'connector.batch-constant-value' = " +
			"'{\"ns\":\"flinkTest\", \"Base\":{\"BaseID\":\"1\",\"innerBaseTest\":{\"Open\":true,\"Env\":\"test\"}}}', \n" +
			"'connector.thrift-method' = 'doAction2', \n" +
			"'connector.response-value' = '{\"head\":{\"code\":\"200\"}}' \n" +
			")";
		tEnv.sqlUpdate(sinkDDL);

		String query = "INSERT INTO rpc_test " +
			"SELECT ROW(stringTest, intTest), stringTest, intTest, arrayTest, arrayTest, mapTest, rowTest " +
			"FROM src";
		tEnv.sqlUpdate(query);
		tEnv.execute("rpc sink test");
	}

	/**
	 * The rpc sink example which use the counter service thrift file.
	 * @throws Exception
	 */
	public void testOnlineCounterServiceRPCSink() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		DataStream<Row> ds = execEnv.fromCollection(testData2).returns(testTypeInfo2);
		tEnv.registerDataStream("src", ds);

		String sinkDDL = "create table rpc_test with (\n" +
			"'connector.type' = 'rpc', \n" +
			"'connector.consul' = 'data.counter_service.online', \n" +
			"'connector.thrift-service-class' = 'com.bytedance.flink.test.onlineThrift.CounterQueryManager', \n" +
			"'connector.thrift-method' = 'GetCount', \n" +
			"'connector.response-value' = '{\"BaseResp\":{\"StatusMessage\":\"SUCCESS\"}}' \n" +
			")";
		tEnv.sqlUpdate(sinkDDL);
		String query = "INSERT INTO rpc_test " +
			"SELECT counterNames, itemIds, timeArray, dailyArray, keyArray, cacheExpireTime, cluster, Base " +
			"FROM src";
		tEnv.sqlUpdate(query);
		tEnv.execute("rpc sink test");
	}

	/**
	 * The rpc sink example which use the counter service thrift file.
	 * In this case, users write ddl by themselves.
	 * @throws Exception
	 */
	public void testOnlineCounterServiceRPCSinkWithDDL() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		DataStream<Row> ds = execEnv.fromCollection(testData3).returns(testTypeInfo3);
		tEnv.registerDataStream("src", ds);

		String sinkDDL = "create table rpc_test (\n" +
			"counter_names ARRAY<VARCHAR>,\n" +
			"item_ids ARRAY<BIGINT>,\n" +
			"cache_expire_time INT,\n" +
			"cluster VARCHAR,\n" +
			"Base ROW<LogID VARCHAR, Caller VARCHAR, Addr VARCHAR, client VARCHAR, trafficEnv ROW<`Open` BOOLEAN, Env VARCHAR>, extra MAP<VARCHAR, VARCHAR>>\n" +
			" ) with (\n" +
			"'connector.type' = 'rpc', \n" +
			"'connector.consul' = 'data.counter_service.online', \n" +
			"'connector.thrift-service-class' = 'com.bytedance.flink.test.onlineThrift.CounterQueryManager', \n" +
			"'connector.thrift-method' = 'GetCount', \n" +
			"'connector.response-value' = '{\"BaseResp\":{\"StatusMessage\":\"SUCCESS\"}}' \n" +
			")";
		tEnv.sqlUpdate(sinkDDL);
		String query = "INSERT INTO rpc_test " +
			"SELECT counterNames, itemIds, cacheExpireTime, cluster, Base " +
			"FROM src";
		tEnv.sqlUpdate(query);
		tEnv.execute("rpc sink test");
	}

	public static void main(String[] args) throws Exception {
		RPCTableSinkExample test = new RPCTableSinkExample();
//		test.testSingleRPCSink();
//		test.testBatchRPCSink();
		test.testOnlineCounterServiceRPCSink();
//		test.testOnlineCounterServiceRPCSinkWithDDL();
	}
}
