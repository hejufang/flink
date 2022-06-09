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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.deployment.JobTaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.JobVertexDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTracker;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.exceptions.TaskSubmissionException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * {@link TaskExecutor} extension for testing purposes.
 */
class TestingTaskExecutor extends TaskExecutor {
	private final CompletableFuture<Void> startFuture = new CompletableFuture<>();
	private Consumer<Tuple4<JobTable.Connection, TaskDeploymentDescriptor, JobInformation, TaskInformation>> submitTaskInternalConsumer = null;
	private Consumer<Tuple5<JobTable.Connection, JobInformation, TaskInformation, JobVertexDeploymentDescriptor, JobTaskDeploymentDescriptor>> submitJobTaskInternalConsumer = null;

	public TestingTaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			@Nullable String metricQueryServiceAddress,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler,
			TaskExecutorPartitionTracker partitionTracker,
			BackPressureSampleService backPressureSampleService) {
		super(
			rpcService,
			taskManagerConfiguration,
			haServices,
			taskExecutorServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			partitionTracker,
			backPressureSampleService);
	}

	public TestingTaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			@Nullable String metricQueryServiceAddress,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler,
			TaskExecutorPartitionTracker partitionTracker,
			BackPressureSampleService backPressureSampleService,
			Consumer<Tuple4<JobTable.Connection, TaskDeploymentDescriptor, JobInformation, TaskInformation>> submitTaskInternalConsumer) {
		super(
			rpcService,
			taskManagerConfiguration,
			haServices,
			taskExecutorServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			partitionTracker,
			backPressureSampleService);
		this.submitTaskInternalConsumer = submitTaskInternalConsumer;
	}

	public TestingTaskExecutor(
			RpcService rpcService,
			TaskManagerConfiguration taskManagerConfiguration,
			HighAvailabilityServices haServices,
			TaskManagerServices taskExecutorServices,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			HeartbeatServices heartbeatServices,
			TaskManagerMetricGroup taskManagerMetricGroup,
			@Nullable String metricQueryServiceAddress,
			BlobCacheService blobCacheService,
			FatalErrorHandler fatalErrorHandler,
			TaskExecutorPartitionTracker partitionTracker,
			BackPressureSampleService backPressureSampleService,
			Consumer<Tuple4<JobTable.Connection, TaskDeploymentDescriptor, JobInformation, TaskInformation>> submitTaskInternalConsumer,
			Consumer<Tuple5<JobTable.Connection, JobInformation, TaskInformation, JobVertexDeploymentDescriptor, JobTaskDeploymentDescriptor>> submitJobTaskInternalConsumer) {
		super(
			rpcService,
			taskManagerConfiguration,
			haServices,
			taskExecutorServices,
			externalResourceInfoProvider,
			heartbeatServices,
			taskManagerMetricGroup,
			metricQueryServiceAddress,
			blobCacheService,
			fatalErrorHandler,
			partitionTracker,
			backPressureSampleService);
		this.submitTaskInternalConsumer = submitTaskInternalConsumer;
		this.submitJobTaskInternalConsumer = submitJobTaskInternalConsumer;
	}

	@Override
	public void onStart() throws Exception {
		try {
			super.onStart();
		} catch (Exception e) {
			startFuture.completeExceptionally(e);
			throw e;
		}

		startFuture.complete(null);
	}

	@Override
	protected void submitTaskInternal(
			JobTable.Connection jobManagerConnection,
			TaskDeploymentDescriptor tdd,
			JobInformation jobInformation,
			TaskInformation taskInformation) throws TaskSubmissionException {
		if (submitTaskInternalConsumer == null) {
			super.submitTaskInternal(jobManagerConnection, tdd, jobInformation, taskInformation);
		} else {
			submitTaskInternalConsumer.accept(Tuple4.of(jobManagerConnection, tdd, jobInformation, taskInformation));
		}
	}

	@Override
	protected void submitTaskInternal(
			JobTable.Connection jobManagerConnection,
			JobInformation jobInformation,
			TaskInformation taskInformation,
			JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor,
			JobTaskDeploymentDescriptor jobTaskDeploymentDescriptor) throws TaskSubmissionException {
		if (submitJobTaskInternalConsumer == null) {
			super.submitTaskInternal(
				jobManagerConnection,
				jobInformation,
				taskInformation,
				jobVertexDeploymentDescriptor,
				jobTaskDeploymentDescriptor);
		} else {
			submitJobTaskInternalConsumer.accept(Tuple5.of(jobManagerConnection,
				jobInformation,
				taskInformation,
				jobVertexDeploymentDescriptor,
				jobTaskDeploymentDescriptor));
		}
	}

	void waitUntilStarted() {
		startFuture.join();
	}

	MainThreadExecutable getMainThreadExecutableForTesting() {
		return this.rpcServer;
	}
}
