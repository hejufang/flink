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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.utils.CollectResultUtil;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test case for submit job created by {@link CollectResultUtil#collect(Table)} to socket cluster.
 */
public class CollectResultUtilsTest {
	@Test
	public void testSubmitHttpJobToSocketCluster() {
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

		StreamTableEnvironment tEnv;
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inBatchMode()
			.useBlinkPlanner()
			.build();
		tEnv = StreamTableEnvironment.create(env, settings);

		Table table = tEnv.sqlQuery("select * from (values ('tom', 19), ('jim', 30), ('tony', 25)) as T(name, age) where age>20");
		List<Row> resultList = CollectResultUtil.collect(table);
		List<String> expectList = Arrays.asList("jim,30", "tony,25");
		List<String> resultValueList = new ArrayList<>();
		for (Row row : resultList) {
			resultValueList.add(row.toString());
		}
		Collections.sort(expectList);
		Collections.sort(resultValueList);
		assertEquals(expectList, resultValueList);
	}
}
