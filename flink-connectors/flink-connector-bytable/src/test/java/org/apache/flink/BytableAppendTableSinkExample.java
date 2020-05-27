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

package org.apache.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.bytable.BytableUpsertTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link BytableUpsertTableSink}.
 */
public class BytableAppendTableSinkExample {

	protected EnvironmentSettings streamSettings;

	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.LONG, Types.DOUBLE,
			Types.BOOLEAN, Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_DATE, Types.SQL_TIME},
		new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3", "f4c1", "f4c2", "f4c3"});

	static {
		testData1.add(Row.of(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1",
			Timestamp.valueOf("2019-08-18 19:00:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:00:00")));
		testData1.add(Row.of(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2",
			Timestamp.valueOf("2019-08-18 19:01:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:01:00")));
		testData1.add(Row.of(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3",
			Timestamp.valueOf("2019-08-18 19:02:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:02:00")));
		testData1.add(Row.of(4, 40, null, 400L, 4.04, true, "Welt-4",
			Timestamp.valueOf("2019-08-18 19:03:00"), Date.valueOf("2019-08-18"), Time.valueOf("19:03:00")));
		testData1.add(Row.of(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5",
			Timestamp.valueOf("2019-08-19 19:10:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:10:00")));
		testData1.add(Row.of(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6",
			Timestamp.valueOf("2019-08-19 19:20:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:20:00")));
		testData1.add(Row.of(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7",
			Timestamp.valueOf("2019-08-19 19:30:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:30:00")));
		testData1.add(Row.of(8, 80, null, 800L, 8.08, true, "Welt-8",
			Timestamp.valueOf("2019-08-19 19:40:00"), Date.valueOf("2019-08-19"), Time.valueOf("19:40:00")));
	}

	public void testUpsert() throws Exception {
		EnvironmentSettings.Builder streamBuilder = EnvironmentSettings.newInstance().inStreamingMode();
		this.streamSettings = streamBuilder.useBlinkPlanner().build();
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);

		String sinkDDL = "create table bytable_test(\n" +
			"rowkey INT,\n" +
			"msg ROW<col1 INT,col2 VARCHAR>,\n" +
			"cellVersion TIMESTAMP(3)\n" +
			") with (\n" +
			"'connector.type' = 'bytable', \n" +
			"'connector.master-urls' = '10.31.204.16:2001,10.31.204.17:2001,10.31.204.18:2001', \n" +
			"'connector.table' = 'flink-test', \n" +
			"'connector.cache-type' = 'OnDemand', \n" +
			"'connector.batch-size' = '1' \n" +
			")";
		tEnv.sqlUpdate(sinkDDL);

		String query = "INSERT INTO bytable_test " +
			"SELECT rowkey, ROW(f1c1,f2c1),f4c1 " +
			"FROM src";
		tEnv.sqlUpdate(query);
		tEnv.execute("bytable upsert sink test");
	}

	public static void main(String[] args) throws Exception {
		BytableAppendTableSinkExample test = new BytableAppendTableSinkExample();
		test.testUpsert();
	}
}
