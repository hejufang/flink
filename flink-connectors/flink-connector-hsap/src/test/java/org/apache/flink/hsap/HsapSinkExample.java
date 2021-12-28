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

package org.apache.flink.hsap;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Example of doris sink.
 */
public class HsapSinkExample {

	private final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.SHORT, Types.STRING, Types.STRING, Types.INT},
		new String[]{"id", "user", "comment", "cnt"});

	private List<Row> initData() {
		List<Row> data = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			data.add(Row.of((short) i, "zhang", " hello", i));
		}
		return data;
	}

	public void testSink() {
		EnvironmentSettings streamSettings;
		List<Row> testData = initData();
		streamSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
		tEnv.createTemporaryView("src", ds);

		String sinkDDL = ""  +
			"create table hsap_test(\n" +
			"	id smallint,\n" +
			"	`user` varchar,\n" +
			"	`comment` varchar,\n" +
			"	cnt bigint\n" +
			") with (\n" +
			"	'connector' = 'hsap', \n" +
			"	'hsap-addrs' = '10.146.87.90:10001', \n" +
			"	'db-name' = 'default_cluster:flink_test_db', \n" +
			"	'table-name' = 'flink_test_table'\n" +
			")";
		tEnv.executeSql(sinkDDL);
		String query = "INSERT INTO hsap_test SELECT * FROM src";
		tEnv.executeSql(query);
	}

	public static void main(String[] args) throws Exception {
		HsapSinkExample hsapSinkExample = new HsapSinkExample();
		hsapSinkExample.testSink();
	}
}
