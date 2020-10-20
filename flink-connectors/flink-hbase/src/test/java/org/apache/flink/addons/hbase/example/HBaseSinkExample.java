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

package org.apache.flink.addons.hbase.example;

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
 * Sink example for HBase.
 */
public class HBaseSinkExample {

	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
			Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING},
		new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3", "f4c1", "f4c2", "f4c3"});

	static {
		testData1.add(Row.of("1", "10", "Hello-1", "100L", "1.01", "false", "Welt-1",
			"2019-08-18 19:00:00", "2019-08-18", "19:00:00"));
	}

	public static void main(String[] args) throws Exception {

		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		DataStream<Row> ds = env.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);

		tEnv.sqlUpdate("" +
			"create table hbase_sink(" +
			"	rowkey VARCHAR,\n" +
			"	family1 ROW<col1 VARCHAR>,\n" +
			"	family2 ROW<col1 VARCHAR, col2 VARCHAR>,\n" +
			"	family3 ROW<col1 VARCHAR, col2 VARCHAR, col3 VARCHAR>\n" +
			") with (\n" +
			"	'connector.type' = 'hbase', \n" +
			"	'connector.version' = '1.4.3', \n" +
			"	'connector.property-version' = '1', \n" +
			"	'connector.table-name' = 'testTable', \n" +
			"	'connector.zookeeper.quorum' = '10.23.72.24:2181', \n" +
			"	'connector.zookeeper.znode.parent' = '/hbase_example' \n" +
			")");

		String query = "INSERT INTO hbase_sink SELECT rowkey, ROW(f1c1), ROW(f2c1, f2c2), ROW(f3c1, f3c2, f3c3) FROM src";
		tEnv.sqlUpdate(query);

		// wait to finish
		tEnv.execute("HBase Job");
	}
}
