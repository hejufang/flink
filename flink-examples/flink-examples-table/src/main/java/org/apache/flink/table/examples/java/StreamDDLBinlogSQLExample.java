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

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Demo of binlog in SQL DDL.
 */
public class StreamDDLBinlogSQLExample {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment execEnv;
		execEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		execEnv.setParallelism(1);

		String sourceDDL = "create table source with ("
			+ "  'connector.type' = 'kafka',"
			+ "  'update-mode' = 'append',"
			+ "  'connector.version' = '0.10',"
			+ "  'connector.topic' = 'binlog_hotsoon_roomdb',"
			+ "  'connector.startup-mode' = 'latest-offset',"
			+ "  'connector.cluster' = 'kafka_miscnew_lf',"
			+ "  'connector.psm' = 'flink.sql.test',"
			+ "  'connector.team' = 'Data',"
			+ "  'connector.owner' = 'liuzhiyi.0424',"
			+ "  'connector.group.id' = 'flnk.sql.test.group',"
			+ "  'format.type' = 'pb_binlog'"
			+ ")";

		String sinkDDL = "create table sink("
			+ "entryType varchar,"
			+ "tableName varchar,"
			+ "rowType varchar,"
			+ "executeTime bigint"
			+ ") with ("
			+ "  'connector.type' = 'filesystem',"
			+ "  'connector.property-version' = '1',"
			+ "  'connector.path' = 'test.csv',"
			+ "  'format.type' = 'csv',"
			+ "  'format.field-delimiter' = ',\t',"
			+ "  'format.fields.0.type' = 'VARCHAR',"
			+ "  'format.fields.0.name' = 'entryType',"
			+ "  'format.fields.1.type' = 'VARCHAR',"
			+ "  'format.fields.1.name' = 'tableName',"
			+ "  'format.fields.2.type' = 'VARCHAR',"
			+ "  'format.fields.2.name' = 'rowType',"
			+ "  'format.fields.3.type' = 'BIGINT',"
			+ "  'format.fields.3.name' = 'executeTime',"
			+ "  'format.property-version' = '1'"
			+ ")";
		String query = "INSERT INTO sink\n" +
			"select " +
			"entryType as entryType, " +
			"header.tableName as tableName, " +
			"RowChange.eventType as rowType, " +
			"header.executeTime as executeTime \n" +
			"from source";

		tEnv.sqlUpdate(sourceDDL);
		tEnv.sqlUpdate(sinkDDL);
		tEnv.sqlUpdate(query);

		tEnv.execute("testJob");
	}
}
