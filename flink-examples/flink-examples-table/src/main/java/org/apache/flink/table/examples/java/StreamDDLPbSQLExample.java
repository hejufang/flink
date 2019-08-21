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

package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Demo of pb source in DDL SQL context.
 */
public class StreamDDLPbSQLExample {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment execEnv;
		execEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		execEnv.setParallelism(1);

		String sourceDDL = "create table source ("
			+ "watermark for server_time as withoffset(server_time, 5000)"
			+ ") with ("
			+ "  'connector.type' = 'kafka',"
			+ "  'update-mode' = 'append',"
			+ "  'connector.version' = '0.10',"
			+ "  'connector.topic' = 'mario_pb_event',"
			+ "  'connector.startup-mode' = 'latest-offset',"
			+ "  'connector.cluster' = 'kafka_streaming_lf',"
			+ "  'connector.psm' = 'flink.sql.test',"
			+ "  'connector.team' = 'Data',"
			+ "  'connector.owner' = 'liuzhiyi.0424',"
			+ "  'connector.group.id' = 'flnk.sql.test.group',"
			+ "  'format.type' = 'pb',"
			+ "  'format.pb-class' = 'com.bytedance.protobuf.mario.EventProto$MarioEvents'"
			+ ")";

		String sinkDDL = "create table sink("
			+ "counts bigint,"
			+ "start_time timestamp,"
			+ "app_name varchar"
			+ ") with ("
			+ "  'connector.type' = 'filesystem',"
			+ "  'connector.property-version' = '1',"
			+ "  'connector.path' = 'test.csv',"
			+ "  'format.type' = 'csv',"
			+ "  'format.field-delimiter' = ',\t',"
			+ "  'format.fields.0.type' = 'BIGINT',"
			+ "  'format.fields.0.name' = 'counts',"
			+ "  'format.fields.1.type' = 'TIMESTAMP',"
			+ "  'format.fields.1.name' = 'start_time',"
			+ "  'format.fields.2.type' = 'VARCHAR',"
			+ "  'format.fields.2.name' = 'app_name',"
			+ "  'format.property-version' = '1'"
			+ ")";
		String query = "insert into sink " +
			"select " +
			"count(1) as counts, " +
			"TUMBLE_START(server_time, INTERVAL '1' SECOND) as start_time, " +
			"header.app_name as app_name " +
			"from source " +
			"group by " +
			"TUMBLE(server_time, INTERVAL '1' SECOND), " +
			"header.app_name";

		tEnv.sqlUpdate(sourceDDL);
		tEnv.sqlUpdate(sinkDDL);
		tEnv.sqlUpdate(query);

		tEnv.execute("testJob");
	}
}
