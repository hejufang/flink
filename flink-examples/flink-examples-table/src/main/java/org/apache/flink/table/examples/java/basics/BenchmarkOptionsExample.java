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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.configuration.BenchmarkOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.CloseableIterator;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Tests for benchmarkOptions.
 */
public class BenchmarkOptionsExample {

	public static void testTaskInitializeThenFinishEnableStreamSQLSocket() throws Exception {
		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfiguration().set(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE, true);
		env.getConfiguration().set(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED, true);
		env.getConfiguration().set(ClusterOptions.RM_MIN_WORKER_NUM, 1);
		env.getConfiguration().set(JobManagerOptions.JOBMANAGER_BATCH_REQUEST_SLOTS_ENABLE, true);
		env.getConfiguration().set(CoreOptions.FLINK_SUBMIT_RUNNING_NOTIFY, true);
		env.getConfiguration().set(JobManagerOptions.JOBMANAGER_REQUEST_SLOT_FROM_RESOURCEMANAGER_ENABLE, true);

		env.getConfiguration().set(ExecutionConfigOptions.TABLE_EXEC_USE_OLAP_MODE, true);

		env.getConfiguration().set(TaskManagerOptions.MEMORY_POOL_MANAGER_ENABLE, true);

		env.getConfiguration().set(BenchmarkOptions.TASK_INITIALIZE_THEN_FINISH_ENABLE, true);

		StreamTableEnvironment tEnv;
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inBatchMode()
			.useBlinkPlanner()
			.build();
		tEnv = StreamTableEnvironment.create(env, settings);

		TableResult tableResult = tEnv.executeSql("select * from (values ('tom', 19), ('jim', 30), ('tony', 25)) as T(name, age) where age>2");
		try (CloseableIterator<Object> resultIterator = tableResult.getJobClient().get().getJobResultIterator(Thread.currentThread().getContextClassLoader())) {
			while (resultIterator.hasNext()) {
				throw new RuntimeException("benchmark option task-initialize-then-finish failed");
			}
		}
	}

	public static void testTaskInitializeThenFinishEnableStreamSQL() throws Exception{
		String planner = "blink";

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfiguration().set(BenchmarkOptions.TASK_INITIALIZE_THEN_FINISH_ENABLE, true);

		StreamTableEnvironment tEnv;
		if (Objects.equals(planner, "blink")) {	// use blink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useBlinkPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else if (Objects.equals(planner, "flink")) {	// use flink planner in streaming mode
			EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.useOldPlanner()
				.build();
			tEnv = StreamTableEnvironment.create(env, settings);
		} else {
			System.err.println("The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', " +
				"where planner (it is either flink or blink, and the default is blink) indicates whether the " +
				"example uses flink planner or blink planner.");
			throw new RuntimeException("benchmark option task-initialize-then-finish failed");
		}

		DataStream<StreamSQLExample.Order> orderA = env.fromCollection(Arrays.asList(
			new StreamSQLExample.Order(1L, "beer", 3),
			new StreamSQLExample.Order(1L, "diaper", 4),
			new StreamSQLExample.Order(3L, "rubber", 2)));

		DataStream<StreamSQLExample.Order> orderB = env.fromCollection(Arrays.asList(
			new StreamSQLExample.Order(2L, "pen", 3),
			new StreamSQLExample.Order(2L, "rubber", 3),
			new StreamSQLExample.Order(4L, "beer", 1)));

		// convert DataStream to Table
		Table tableA = tEnv.fromDataStream(orderA, $("user"), $("product"), $("amount"));
		// register DataStream as Table
		tEnv.createTemporaryView("OrderB", orderB, $("user"), $("product"), $("amount"));

		// union the two tables
		Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
			"SELECT * FROM OrderB WHERE amount < 2");

		DataStream resultDataStream = tEnv.toAppendStream(result, StreamSQLExample.Order.class);

		resultDataStream.filter((value) -> {
			throw new RuntimeException("benchmark option task-initialize-then-finish failed");
		});

		// after the table program is converted to DataStream program,
		// we must use `env.execute()` to submit the job.
		env.execute();
	}

	public static void main(String[] args) throws Exception{
		testTaskInitializeThenFinishEnableStreamSQLSocket();
		testTaskInitializeThenFinishEnableStreamSQL();
	}
}
