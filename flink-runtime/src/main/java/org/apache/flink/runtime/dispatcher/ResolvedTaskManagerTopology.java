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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.netty.TaskExecutorNettyClient;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container for TaskManager topology information.
 */
public class ResolvedTaskManagerTopology {
	private static final Logger LOG = LoggerFactory.getLogger(ResolvedTaskManagerTopology.class);
	private final TaskExecutorGateway taskExecutorGateway;

	private final TaskManagerLocation taskManagerLocation;

	private final TaskExecutorNettyClient taskExecutorNettyClient;

	private final long registrationTime;

	private int runningJobCount;

	public ResolvedTaskManagerTopology(
			TaskExecutorGateway taskExecutorGateway,
			TaskManagerLocation taskManagerLocation) {
		this(taskExecutorGateway, taskManagerLocation, null);
	}

	public ResolvedTaskManagerTopology(
			TaskExecutorGateway taskExecutorGateway,
			TaskManagerLocation taskManagerLocation,
			TaskExecutorNettyClient taskExecutorNettyClient) {
		this.taskExecutorGateway = taskExecutorGateway;
		this.taskManagerLocation = taskManagerLocation;
		this.taskExecutorNettyClient = taskExecutorNettyClient;
		this.runningJobCount = 0;
		this.registrationTime = System.currentTimeMillis();
	}

	public TaskExecutorGateway getTaskExecutorGateway() {
		return taskExecutorGateway;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public TaskExecutorNettyClient getTaskExecutorNettyClient() {
		return taskExecutorNettyClient;
	}

	public long getRegistrationTime() {
		return registrationTime;
	}

	public void incrementRunningJob() {
		this.runningJobCount++;
	}

	public void decrementRunningJob() {
		this.runningJobCount--;
	}

	public int getRunningJobCount() {
		return runningJobCount;
	}

	public static ResolvedTaskManagerTopology fromUnresolvedTaskManagerTopology(
			UnresolvedTaskManagerTopology unresolvedTaskManagerTopology,
			boolean useAddressAsHostname,
			Configuration configuration) throws Exception {
		final TaskManagerLocation taskManagerLocation;
		final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = unresolvedTaskManagerTopology.getUnresolvedTaskManagerLocation();
		final boolean deployTaskSocketEnable = configuration.get(ClusterOptions.CLUSTER_DEPLOY_TASK_SOCKET_ENABLE);
		try {
			taskManagerLocation = TaskManagerLocation.fromUnresolvedLocation(unresolvedTaskManagerLocation, useAddressAsHostname);
			TaskExecutorNettyClient taskExecutorNettyClient = null;
			if (deployTaskSocketEnable) {
				int channelCount = configuration.get(JobManagerOptions.JOB_DEPLOY_SOCKET_CHANNEL_COUNT);
				int connectTimeoutMills = configuration.get(JobManagerOptions.JOB_DEPLOY_SOCKET_CONNECT_TIMEOUT_MILLS);
				int lowWaterMark = configuration.get(JobManagerOptions.JOB_DEPLOY_SOCKET_LOW_WATER_MARK);
				int highWaterMark = configuration.get(JobManagerOptions.JOB_DEPLOY_SOCKET_HIGH_WATER_MARK);
				taskExecutorNettyClient = new TaskExecutorNettyClient(
					unresolvedTaskManagerTopology.getSocketAddress(),
					channelCount,
					connectTimeoutMills,
					lowWaterMark,
					highWaterMark);
				taskExecutorNettyClient.start();
			}
			return new ResolvedTaskManagerTopology(
					unresolvedTaskManagerTopology.getTaskExecutorGateway(),
					taskManagerLocation,
					taskExecutorNettyClient);
		} catch (Throwable throwable) {
			final String errMsg = String.format(
					"Could not accept TaskManager registration. TaskManager address %s cannot be resolved. %s",
					unresolvedTaskManagerLocation.getExternalAddress(),
					throwable.getMessage());
			LOG.error(errMsg);
			throw throwable;
		}
	}
}
