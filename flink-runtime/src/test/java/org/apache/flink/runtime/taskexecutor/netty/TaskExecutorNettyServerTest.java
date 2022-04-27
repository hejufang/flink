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

package org.apache.flink.runtime.taskexecutor.netty;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.TaskExecutorSocketAddress;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test case for submit task deployment from {@link TaskExecutorNettyClient} to {@link TaskExecutorNettyServer}.
 */
public class TaskExecutorNettyServerTest {
	@Test
	public void testSubmitTaskList() throws Exception {
		CompletableFuture<Integer> submitFuture = new CompletableFuture<>();
		try (TaskExecutorNettyServer taskExecutorNettyServer = new TaskExecutorNettyServer(
				() -> new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskListConsumer(tdds -> {
							submitFuture.complete(tdds.size());
						})
					.createTestingTaskExecutorGateway(),
				"localhost",
				new Configuration())) {
			taskExecutorNettyServer.start();

			try (TaskExecutorNettyClient taskExecutorNettyClient = new TaskExecutorNettyClient(
					new TaskExecutorSocketAddress(taskExecutorNettyServer.getAddress(), taskExecutorNettyServer.getPort()),
					1,
					0,
					0,
					0)) {
				taskExecutorNettyClient.start();

				CompletableFuture<Acknowledge> future = taskExecutorNettyClient.submitTaskList("test", Collections.emptyList(), JobMasterId.generate());
				future.get(10, TimeUnit.SECONDS);

				int count = submitFuture.get(10, TimeUnit.SECONDS);
				assertEquals(0, count);
			}
		}
	}
}
