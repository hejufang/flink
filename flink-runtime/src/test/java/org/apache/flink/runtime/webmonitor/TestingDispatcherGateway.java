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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.EstablishedTaskExecutorConnection;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.TriFunction;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Testing implementation of the {@link DispatcherGateway}.
 */
public final class TestingDispatcherGateway extends TestingRestfulGateway implements DispatcherGateway {

	static final Function<JobGraph, CompletableFuture<Acknowledge>> DEFAULT_SUBMIT_FUNCTION = jobGraph -> CompletableFuture.completedFuture(Acknowledge.get());
	static final Supplier<CompletableFuture<Collection<JobID>>> DEFAULT_LIST_FUNCTION = () -> CompletableFuture.completedFuture(Collections.emptyList());
	static final int DEFAULT_BLOB_SERVER_PORT = 1234;
	static final DispatcherId DEFAULT_FENCING_TOKEN = DispatcherId.generate();
	static final Function<JobID, CompletableFuture<ArchivedExecutionGraph>> DEFAULT_REQUEST_ARCHIVED_JOB_FUNCTION = jobID -> CompletableFuture.completedFuture(null);
	static final Function<ApplicationStatus, CompletableFuture<Acknowledge>> DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION = status -> CompletableFuture.completedFuture(Acknowledge.get());
	static final Consumer<ResourceManagerId> DEFAULT_DISCONNECT_RESOURCE_MANAGER_CONSUMER = ignore -> {};
	static final Consumer<ResourceID> DEFAULT_DISCONNECT_TASK_EXECUTOR_CONSUMER = ignore -> {};
	static final Consumer<ResourceID> DEFAULT_RESOURCE_MANAGER_HEARTBEAT_CONSUMER = ignore -> {};
	static final Function<Collection<UnresolvedTaskManagerTopology>, CompletableFuture<Acknowledge>> DEFAULT_OFFER_TASKMANAGERS_FUNCTION = taskManagerTopologies -> CompletableFuture.completedFuture(Acknowledge.get());
	static final Consumer<ResourceID> DEFAULT_TASK_EXECUTOR_HEARTBEAT_CONSUMER = ignore -> {};
	static final Consumer<JobID> DEFAULT_REPORT_ACCUMULATOR_CONSUMER = ignore -> {};
	static final BiFunction<String, ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> DEFAULT_REGISTER_TASKMANAGER_FUNCTION = (address, resourceID) -> CompletableFuture.completedFuture(null);
	static final BiFunction<JobID, ResourceID, CompletableFuture<Acknowledge>> DEFAULT_DISCONNECT_TASKMANAGER_FUNCTION = (jobid, resourceID) -> CompletableFuture.completedFuture(null);

	private Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction;
	private BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer;
	private Supplier<CompletableFuture<Collection<JobID>>> listFunction;
	private int blobServerPort;
	private DispatcherId fencingToken;
	private Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestArchivedJobFunction;
	private Function<ApplicationStatus, CompletableFuture<Acknowledge>> clusterShutdownWithStatusFunction;
	private Consumer<ResourceManagerId> disconnectResourceManagerConsumer;
	private Consumer<ResourceID> disconnectTaskExecutorConsumer;
	private Consumer<ResourceID> resourceManagerHeartbeatConsumer;
	private Function<Collection<UnresolvedTaskManagerTopology>, CompletableFuture<Acknowledge>> offerTaskManagersFunction;
	private Consumer<ResourceID> taskExecutorHeartbeatConsumer;
	private Consumer<JobID> reportAccumulatorConsumer;
	private BiFunction<String, ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> registerTaskExecutorFunction;
	private BiFunction<JobID, ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction;

	public TestingDispatcherGateway() {
		super();
		submitFunction = DEFAULT_SUBMIT_FUNCTION;
		listFunction = DEFAULT_LIST_FUNCTION;
		blobServerPort = DEFAULT_BLOB_SERVER_PORT;
		fencingToken = DEFAULT_FENCING_TOKEN;
		requestArchivedJobFunction = DEFAULT_REQUEST_ARCHIVED_JOB_FUNCTION;
		clusterShutdownWithStatusFunction = DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION;
		disconnectResourceManagerConsumer = DEFAULT_DISCONNECT_RESOURCE_MANAGER_CONSUMER;
		disconnectTaskExecutorConsumer = DEFAULT_DISCONNECT_TASK_EXECUTOR_CONSUMER;
		registerTaskExecutorFunction = DEFAULT_REGISTER_TASKMANAGER_FUNCTION;
		resourceManagerHeartbeatConsumer = DEFAULT_RESOURCE_MANAGER_HEARTBEAT_CONSUMER;
		taskExecutorHeartbeatConsumer = DEFAULT_TASK_EXECUTOR_HEARTBEAT_CONSUMER;
		reportAccumulatorConsumer = DEFAULT_REPORT_ACCUMULATOR_CONSUMER;
		offerTaskManagersFunction = DEFAULT_OFFER_TASKMANAGERS_FUNCTION;
		disconnectTaskManagerFunction = DEFAULT_DISCONNECT_TASKMANAGER_FUNCTION;
	}

	public TestingDispatcherGateway(
			String address,
			String hostname,
			Function<JobID, CompletableFuture<Acknowledge>> cancelJobFunction,
			Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction,
			Function<JobID, CompletableFuture<JobResult>> requestJobResultFunction,
			Function<JobID, CompletableFuture<JobStatus>> requestJobStatusFunction,
			Supplier<CompletableFuture<MultipleJobsDetails>> requestMultipleJobDetailsSupplier,
			Supplier<CompletableFuture<ClusterOverview>> requestClusterOverviewSupplier,
			Supplier<CompletableFuture<Collection<String>>> requestMetricQueryServiceAddressesSupplier,
			Supplier<CompletableFuture<Collection<Tuple2<ResourceID, String>>>> requestTaskManagerMetricQueryServiceGatewaysSupplier,
			Supplier<CompletableFuture<ThreadDumpInfo>> requestThreadDumpSupplier,
			BiFunction<JobID, JobVertexID, CompletableFuture<OperatorBackPressureStatsResponse>> requestOperatorBackPressureStatsFunction,
			BiFunction<JobID, String, CompletableFuture<String>> triggerSavepointFunction,
			BiFunction<JobID, String, CompletableFuture<String>> stopWithSavepointFunction,
			Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction,
			BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer,
			Supplier<CompletableFuture<Collection<JobID>>> listFunction,
			int blobServerPort,
			DispatcherId fencingToken,
			Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestArchivedJobFunction,
			Supplier<CompletableFuture<Acknowledge>> clusterShutdownSupplier,
			Function<ApplicationStatus, CompletableFuture<Acknowledge>> clusterShutdownWithStatusFunction,
			TriFunction<JobID, OperatorID, SerializedValue<CoordinationRequest>, CompletableFuture<CoordinationResponse>> deliverCoordinationRequestToCoordinatorFunction,
			Consumer<ResourceManagerId> disconnectResourceManagerConsumer,
			Consumer<ResourceID> disconnectTaskExecutorConsumer,
			Consumer<ResourceID> resourceManagerHeartbeatConsumer,
			Function<Collection<UnresolvedTaskManagerTopology>, CompletableFuture<Acknowledge>> offerTaskManagersFunction,
			BiFunction<String, ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> registerTaskExecutorFunction,
			Consumer<ResourceID> taskExecutorHeartbeatConsumer,
			Consumer<JobID> reportAccumulatorConsumer,
			BiFunction<JobID, ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction
	) {
		super(
			address,
			hostname,
			cancelJobFunction,
			requestJobFunction,
			requestJobResultFunction,
			requestJobStatusFunction,
			requestMultipleJobDetailsSupplier,
			requestClusterOverviewSupplier,
			requestMetricQueryServiceAddressesSupplier,
			requestTaskManagerMetricQueryServiceGatewaysSupplier,
			requestThreadDumpSupplier,
			requestOperatorBackPressureStatsFunction,
			triggerSavepointFunction,
			stopWithSavepointFunction,
			clusterShutdownSupplier,
			deliverCoordinationRequestToCoordinatorFunction);
		this.submitFunction = submitFunction;
		this.submitConsumer = submitConsumer;
		this.listFunction = listFunction;
		this.blobServerPort = blobServerPort;
		this.fencingToken = fencingToken;
		this.requestArchivedJobFunction = requestArchivedJobFunction;
		this.clusterShutdownWithStatusFunction = clusterShutdownWithStatusFunction;
		this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
		this.resourceManagerHeartbeatConsumer = resourceManagerHeartbeatConsumer;
		this.offerTaskManagersFunction = offerTaskManagersFunction;
		this.disconnectTaskExecutorConsumer = disconnectTaskExecutorConsumer;
		this.registerTaskExecutorFunction = registerTaskExecutorFunction;
		this.taskExecutorHeartbeatConsumer = taskExecutorHeartbeatConsumer;
		this.reportAccumulatorConsumer = reportAccumulatorConsumer;
		this.disconnectTaskManagerFunction = disconnectTaskManagerFunction;
	}

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		return submitFunction.apply(jobGraph);
	}

	@Override
	public void submitJob(JobGraph jobGraph, ChannelHandlerContext ctx, Time timeout) {
		submitConsumer.accept(jobGraph, ctx);
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		return listFunction.get();
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return CompletableFuture.completedFuture(blobServerPort);
	}

	@Override
	public DispatcherId getFencingToken() {
		return fencingToken;
	}

	public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, @RpcTimeout Time timeout) {
		return requestArchivedJobFunction.apply(jobId);
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster(ApplicationStatus applicationStatus) {
		return clusterShutdownWithStatusFunction.apply(applicationStatus);
	}

	@Override
	public void disconnectResourceManager(final ResourceManagerId resourceManagerId, final Exception cause) {
		disconnectResourceManagerConsumer.accept(resourceManagerId);
	}

	@Override
	public void disconnectTaskExecutor(ResourceID resourceID, Exception cause) {
		disconnectTaskExecutorConsumer.accept(resourceID);
	}

	@Override
	public CompletableFuture<Acknowledge> offerTaskManagers(Collection<UnresolvedTaskManagerTopology> taskManagerTopologies, Time timeout) {
		return offerTaskManagersFunction.apply(taskManagerTopologies);
	}

	@Override
	public CompletableFuture<EstablishedTaskExecutorConnection> registerTaskManager(String taskManagerRpcAddress, ResourceID taskManagerId, Time timeout) {
		return registerTaskExecutorFunction.apply(taskManagerRpcAddress, taskManagerId);
	}

	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(JobID jobId, TaskExecutionState taskExecutionState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(JobID jobId, JobVertexID vertexID, ExecutionAttemptID executionAttempt) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(JobID jobId, IntermediateDataSetID intermediateResultId, ResultPartitionID partitionId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(JobID jobId, ResultPartitionID partitionID, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(JobID jobId, ResourceID resourceID, Exception cause) {
		return disconnectTaskManagerFunction.apply(jobId, resourceID);
	}

	@Override
	public CompletableFuture<String> triggerDetachSavepoint(JobID jobId, String savepointId, boolean blockSource, long savepointTimeout, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<List<String>> dumpPendingSavepoints(JobID jobId) {
		return null;
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(JobID jobId, String aggregateName, Object aggregand, byte[] serializedAggregationFunction) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(JobID jobId, ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID resourceID) {
		resourceManagerHeartbeatConsumer.accept(resourceID);
	}

	@Override
	public void heartbeatFromTaskExecutor(ResourceID resourceID, Void payload) {
		taskExecutorHeartbeatConsumer.accept(resourceID);
	}

	/**
	 * Builder for the {@link TestingDispatcherGateway}.
	 */
	public static final class Builder extends TestingRestfulGateway.AbstractBuilder<Builder> {

		private Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction;
		private BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer;
		private Supplier<CompletableFuture<Collection<JobID>>> listFunction;
		private int blobServerPort;
		private DispatcherId fencingToken;
		private Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestArchivedJobFunction;
		private Function<ApplicationStatus, CompletableFuture<Acknowledge>> clusterShutdownWithStatusFunction = DEFAULT_SHUTDOWN_WITH_STATUS_FUNCTION;
		private Consumer<ResourceManagerId> disconnectResourceManagerConsumer = DEFAULT_DISCONNECT_RESOURCE_MANAGER_CONSUMER;
		private Consumer<ResourceID> resourceManagerHeartbeatConsumer = DEFAULT_RESOURCE_MANAGER_HEARTBEAT_CONSUMER;
		private Function<Collection<UnresolvedTaskManagerTopology>, CompletableFuture<Acknowledge>> offerTaskManagersFunction = DEFAULT_OFFER_TASKMANAGERS_FUNCTION;
		private Consumer<ResourceID> disconnectTaskExecutorConsumer = DEFAULT_DISCONNECT_TASK_EXECUTOR_CONSUMER;
		private BiFunction<String, ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> registerTaskExecutorFunction = DEFAULT_REGISTER_TASKMANAGER_FUNCTION;
		private Consumer<ResourceID> taskExecutorHeartbeatConsumer = DEFAULT_TASK_EXECUTOR_HEARTBEAT_CONSUMER;
		private Consumer<JobID> reportAccumulatorConsumer = DEFAULT_REPORT_ACCUMULATOR_CONSUMER;
		private BiFunction<JobID, ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction = DEFAULT_DISCONNECT_TASKMANAGER_FUNCTION;

		public Builder setSubmitFunction(Function<JobGraph, CompletableFuture<Acknowledge>> submitFunction) {
			this.submitFunction = submitFunction;
			return this;
		}

		public Builder setSubmitConsumer(BiConsumer<JobGraph, ChannelHandlerContext> submitConsumer) {
			this.submitConsumer = submitConsumer;
			return this;
		}

		public Builder setListFunction(Supplier<CompletableFuture<Collection<JobID>>> listFunction) {
			this.listFunction = listFunction;
			return this;
		}

		public Builder setRequestArchivedJobFunction(Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
			requestArchivedJobFunction = requestJobFunction;
			return this;
		}

		public Builder setClusterShutdownFunction(Function<ApplicationStatus, CompletableFuture<Acknowledge>> clusterShutdownFunction) {
			this.clusterShutdownWithStatusFunction = clusterShutdownFunction;
			return this;
		}

		@Override
		public Builder setRequestJobFunction(Function<JobID, CompletableFuture<ArchivedExecutionGraph>> requestJobFunction) {
			// signature clash
			throw new UnsupportedOperationException("Use setRequestArchivedJobFunction() instead.");
		}

		public Builder setDisconnectResourceManagerConsumer(Consumer<ResourceManagerId> disconnectResourceManagerConsumer) {
			this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
			return this;
		}

		public Builder setResourceManagerHeartbeatConsumer(Consumer<ResourceID> resourceManagerHeartbeatConsumer) {
			this.resourceManagerHeartbeatConsumer = resourceManagerHeartbeatConsumer;
			return this;
		}

		public Builder setOfferTaskManagersFunction(
				Function<Collection<UnresolvedTaskManagerTopology>, CompletableFuture<Acknowledge>> offerTaskManagersFunction) {
			this.offerTaskManagersFunction = offerTaskManagersFunction;
			return this;
		}

		public Builder setRegisterTaskExecutorFunction(BiFunction<String, ResourceID, CompletableFuture<EstablishedTaskExecutorConnection>> registerTaskExecutorFunction) {
			this.registerTaskExecutorFunction = registerTaskExecutorFunction;
			return this;
		}

		public Builder setTaskExecutorHeartbeatConsumer(Consumer<ResourceID> taskExecutorHeartbeatConsumer) {
			this.taskExecutorHeartbeatConsumer = taskExecutorHeartbeatConsumer;
			return this;
		}

		public Builder setDisconnectTaskExecutorConsumer(Consumer<ResourceID> disconnectTaskExecutorConsumer) {
			this.disconnectTaskExecutorConsumer = disconnectTaskExecutorConsumer;
			return this;
		}

		public Builder setDisconnectTaskManagerFunction(BiFunction<JobID, ResourceID, CompletableFuture<Acknowledge>> disconnectTaskManagerFunction) {
			this.disconnectTaskManagerFunction = disconnectTaskManagerFunction;
			return this;
		}

		@Override
		protected Builder self() {
			return this;
		}

		public Builder setBlobServerPort(int blobServerPort) {
			this.blobServerPort = blobServerPort;
			return this;
		}

		public Builder setFencingToken(DispatcherId fencingToken) {
			this.fencingToken = fencingToken;
			return this;
		}

		public Builder setReportAccumulatorConsumer(Consumer<JobID> reportAccumulatorConsumer) {
			this.reportAccumulatorConsumer = reportAccumulatorConsumer;
			return this;
		}

		public TestingDispatcherGateway build() {
			return new TestingDispatcherGateway(
				address,
				hostname,
				cancelJobFunction,
				requestJobFunction,
				requestJobResultFunction,
				requestJobStatusFunction,
				requestMultipleJobDetailsSupplier,
				requestClusterOverviewSupplier,
				requestMetricQueryServiceGatewaysSupplier,
				requestTaskManagerMetricQueryServiceGatewaysSupplier,
				requestThreadDumpSupplier,
				requestOperatorBackPressureStatsFunction,
				triggerSavepointFunction,
				stopWithSavepointFunction,
				submitFunction,
				submitConsumer,
				listFunction,
				blobServerPort,
				fencingToken,
				requestArchivedJobFunction,
				clusterShutdownSupplier,
				clusterShutdownWithStatusFunction,
				deliverCoordinationRequestToCoordinatorFunction,
				disconnectResourceManagerConsumer,
				disconnectTaskExecutorConsumer,
				resourceManagerHeartbeatConsumer,
				offerTaskManagersFunction,
				registerTaskExecutorFunction,
				taskExecutorHeartbeatConsumer,
				reportAccumulatorConsumer,
				disconnectTaskManagerFunction);
		}
	}
}
