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
import java.util.List;

/**
 * RPC dimension example.
 */
public class RPCDimensionExample {

	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.OBJECT_ARRAY(Types.STRING), Types.OBJECT_ARRAY(Types.LONG),
			Types.OBJECT_ARRAY(Types.INT), Types.OBJECT_ARRAY(Types.INT), Types.OBJECT_ARRAY(Types.LONG),
			Types.INT, Types.STRING, Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING,
			Types.ROW(Types.BOOLEAN, Types.STRING), Types.MAP(Types.STRING, Types.STRING))},
		new String[]{"counterNames", "itemIds", "timeArray", "dailyArray", "keyArray", "cacheExpireTime", "cluster", "Base"});

	static {
		String[] counterNames = {"group_impression_count"};
		Long[] itemIds = {6492274619347632398L};
		Row base = Row.of("", "data.online_joiner.test", "", "", null, null);
		testData1.add(Row.of(counterNames, itemIds, null, null, null, 10000, "ONLINE", base));
	}

	/**
	 * The rpc dimension example which use the counter service thrift file.
	 */
	public void testOnlineCounterServiceRPCDimension() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);

		String dimensionDDL = "create table rpc_dim with (\n" +
			"'connector.type' = 'rpc', \n" +
			"'connector.consul' = 'data.counter_service.online', \n" +
			"'connector.thrift-service-class' = 'com.bytedance.flink.test.onlineThrift.CounterQueryManager', \n" +
			"'connector.thrift-method' = 'GetCount', \n" +
			"'connector.is-dimension-table' = 'true' \n" +
			"); \n";
		tEnv.sql(dimensionDDL);

		String sinkDDL = "create table sink(" +
			"	id ARRAY<BIGINT>, \n" +
			"	counterNames ARRAY<VARCHAR>, \n" +
			"	message VARCHAR \n" +
			") with ( \n" +
			"'connector.type' = 'print', \n" +
			"'connector.print-sample-ratio' = '1' \n" +
			"); \n";
		tEnv.sql(sinkDDL);

		String query = "insert into sink" +
			" select T.itemIds, T.counterNames, D.Response.BaseResp.StatusMessage " +
			" from (select src.counterNames, src.itemIds, src.cluster, src.Base, PROCTIME() as proc from src) T " +
			" left join rpc_dim FOR SYSTEM_TIME AS OF T.proc as D" +
			" ON T.counterNames = D.counter_names and T.itemIds = D.item_ids and T.cluster = D.cluster and" +
			" T.Base = D.Base";
		tEnv.sql(query);
		tEnv.execute("rpc dimension test");
	}

	public static void main(String[] args) throws Exception {
		RPCDimensionExample example = new RPCDimensionExample();
		example.testOnlineCounterServiceRPCDimension();
	}
}
