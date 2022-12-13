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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalhandler.ExternalRequestHandleReport;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerOfferSlots;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Dispatcher forward taskExecutor request to jobMaster.
 */
public class JobMasterDispatcherProxyGateway implements JobMasterGateway {

	private JobID jobId;

	private JobMasterId jobMasterId;

	private DispatcherGateway dispatcherGateway;

	public JobMasterDispatcherProxyGateway(JobID jobId, DispatcherGateway  dispatcherGateway) {
		this.jobId = jobId;
		this.dispatcherGateway = dispatcherGateway;
		this.jobMasterId = JobMasterId.generate();
	}

	//----------------------------------------------------------------------------------------------
	// jobMasterGateway methods
	//----------------------------------------------------------------------------------------------
	@Override
	public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> reportExternalRequestResult(ExternalRequestHandleReport externalRequestHandleReport) {
		return null;
	}

	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		return dispatcherGateway.updateTaskExecutionState(jobId, taskExecutionState);
	}

	@Override
	public CompletableFuture<Acknowledge> batchUpdateTaskExecutionState(BatchTaskExecutionState batchTaskExecutionState) {
		return dispatcherGateway.batchUpdateTaskExecutionState(batchTaskExecutionState);
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) {
		return dispatcherGateway.requestNextInputSplit(jobId, vertexID, executionAttempt);
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID partitionId) {
		return dispatcherGateway.requestPartitionState(jobId, intermediateResultId, partitionId);
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(ResultPartitionID partitionID, Time timeout) {
		return dispatcherGateway.scheduleOrUpdateConsumers(jobId, partitionID, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause) {
		return dispatcherGateway.disconnectTaskManager(jobId, resourceID, cause);
	}

	@Override
	public void disconnectResourceManager(ResourceManagerId resourceManagerId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(ResourceID taskManagerId, Collection<SlotOffer> slots, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> offerOptimizeSlots(Collection<TaskManagerOfferSlots> slots, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void failSlot(ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(String taskManagerRpcAddress, UnresolvedTaskManagerLocation unresolvedTaskManagerLocation, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromTaskManager(ResourceID resourceID, AccumulatorReport accumulatorReport) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID resourceID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String targetDirectory, boolean cancelJob, long savepointTimeout, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<String> triggerDetachSavepoint(String savepointId, boolean blockSource, long savepointTimeout, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<List<String>> dumpPendingSavepoints() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(@Nullable String targetDirectory, boolean advanceToEndOfEventTime, long savepointTimeout, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(JobVertexID jobVertexId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(String aggregateName, Object aggregand, byte[] serializedAggregationFunction) {
		return dispatcherGateway.updateGlobalAggregate(jobId, aggregateName, aggregand, serializedAggregationFunction);
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(OperatorID operatorId, SerializedValue<CoordinationRequest> serializedRequest, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event) {
		return dispatcherGateway.sendOperatorEventToCoordinator(jobId, task, operatorID, event);
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(JobID jobId, String registrationName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId, InetSocketAddress kvStateServerAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobMasterId getFencingToken() {
		return jobMasterId;
	}

	@Override
	public String getAddress() {
		return dispatcherGateway.getAddress();
	}

	@Override
	public String getHostname() {
		return dispatcherGateway.getHostname();
	}
}
