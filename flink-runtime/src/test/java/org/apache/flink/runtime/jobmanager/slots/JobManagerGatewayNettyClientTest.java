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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.TaskExecutorSocketAddress;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.ThrownTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.netty.TaskExecutorNettyServer;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Submit job tasks from job manager gateway to netty server in task executor.
 */
public class JobManagerGatewayNettyClientTest {
	@Test
	public void testSubmitTaskList() throws Exception {
		CompletableFuture<Integer> tddsFuture = new CompletableFuture<>();
		try (TaskExecutorNettyServer taskExecutorNettyServer = new TaskExecutorNettyServer(
				() -> new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskListConsumer(tdds -> tddsFuture.complete(tdds.size()))
					.createTestingTaskExecutorGateway(),
				"localhost",
				new Configuration(),
				false)) {
			taskExecutorNettyServer.start();
			Configuration configuration = new Configuration();
			configuration.set(ClusterOptions.CLUSTER_DEPLOY_TASK_SOCKET_ENABLE, true);
			UnresolvedTaskManagerTopology unresolvedTaskManagerTopology = new UnresolvedTaskManagerTopology(
				new ThrownTaskExecutorGateway(),
				new UnresolvedTaskManagerLocation(ResourceID.generate(), "localhost", -1),
				new TaskExecutorSocketAddress(taskExecutorNettyServer.getAddress(), taskExecutorNettyServer.getPort()));
			ResolvedTaskManagerTopology resolvedTaskManagerTopology = ResolvedTaskManagerTopology
				.fromUnresolvedTaskManagerTopology(unresolvedTaskManagerTopology, false, configuration);
			TaskManagerGateway taskManagerGateway = new RpcTaskManagerGateway(
				resolvedTaskManagerTopology.getTaskExecutorGateway(),
				JobMasterId.generate(),
				true,
				"localhost",
				resolvedTaskManagerTopology.getTaskExecutorNettyClient());
			CompletableFuture<Acknowledge> submitFuture = taskManagerGateway.submitTaskList(new ArrayList<>(), Time.seconds(1));
			submitFuture.get(10, TimeUnit.SECONDS);

			int tddCount = tddsFuture.get();
			assertEquals(0, tddCount);

			resolvedTaskManagerTopology.getTaskExecutorNettyClient().close();
		}
	}
}
