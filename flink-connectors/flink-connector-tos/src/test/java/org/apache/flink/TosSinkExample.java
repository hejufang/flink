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

package org.apache.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * Example of tos sink.
 */
public class TosSinkExample {

	private enum RunningMode {
		STREAM,
		BATCH
	}

	private List<Row> testBatchData = new ArrayList<>();
	private List<Row> testStreamData = new ArrayList<>();
	private final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.STRING, Types.INT},
		new String[]{"name", "age"});

	public void initBatchData() {
		for (int i = 0; i < 1000000; i++) {
			testBatchData.add(Row.of("zhang", i));
		}
	}

	public void initStreamData() {
		for (int i = 0; i < 100; i++) {
			testStreamData.add(Row.of("zhang", i));
		}
	}

	public void testSink(RunningMode mode) throws Exception {
		EnvironmentSettings streamSettings;
		List<Row> testData;
		if (mode.equals(RunningMode.STREAM)) {
			streamSettings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			testData = testStreamData;
		} else if (mode.equals(RunningMode.BATCH)) {
			streamSettings = EnvironmentSettings
				.newInstance()
				.inBatchMode()
				.useBlinkPlanner()
				.build();
			testData = testBatchData;
		} else {
			throw new FlinkRuntimeException(String.format("mode %s is illegal", mode.name()));
		}

		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		DataStream<Row> ds = execEnv.fromCollection(testData).returns(testTypeInfo1);
		tEnv.createTemporaryView("src", ds);

		String sinkDDL = String.format("create table tos_test(\n" +
			"name VARCHAR,\n" +
			"age INT\n" +
			") with (\n" +
			"'connector' = 'tos', \n" +
			"'bucket' = 'flink-tos-sink-test', \n" +
			"'access-key' = 'NYNF7TFL0R8087101JXN', \n" +
			"'object-key' = '%s' \n" +
			")", mode.name());
		tEnv.executeSql(sinkDDL);
		String query = "INSERT INTO tos_test SELECT name, age FROM src";
		tEnv.executeSql(query);
	}

	public void testStreamSink() throws Exception {
		initStreamData();
		testSink(RunningMode.STREAM);
	}

	public void testBatchSink() throws Exception {
		initBatchData();
		testSink(RunningMode.BATCH);
	}

	public static void main(String[] args) throws Exception {
		TosSinkExample tosSinkExample = new TosSinkExample();
		tosSinkExample.testStreamSink();
		tosSinkExample.testBatchSink();
	}
}
