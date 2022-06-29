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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceRequestSlot;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Simple {@link TaskExecutorGateway} implementation with throwing exception in all methods.
 */
public class ThrownTaskExecutorGateway implements TaskExecutorGateway {

	public ThrownTaskExecutorGateway() {}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(
			SlotID slotId,
			JobID jobId,
			AllocationID allocationId,
			ResourceProfile resourceProfile,
			String targetAddress,
			ResourceManagerId resourceManagerId,
			Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> requestJobSlotList(
		JobID jobId,
		String targetAddress,
		ResourceManagerId resourceManagerId,
		List<ResourceRequestSlot> resourceSlotList,
		@RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(ExecutionAttemptID executionAttemptId, int requestId, @RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void releaseOrPromotePartitions(JobID jobId, Set<ResultPartitionID> partitionToRelease, Set<ResultPartitionID> partitionsToPromote) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> releaseClusterPartitions(Collection<IntermediateDataSetID> dataSetsToRelease, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> abortCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromJobManager(ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromDispatcher(ResourceID resourceID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void disconnectResourceManager(Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void disconnectDispatcher(ResourceID dispatcherId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUploadByType(FileType fileType, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<PreviewDataResponse> requestTaskManagerPreviewData(JobID jobId,  JobVertexID jobVertexId, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUploadByName(String fileName, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Boolean> canBeReleased() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToTask(
			ExecutionAttemptID task,
			OperatorID operator,
			SerializedValue<OperatorEvent> evt) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ThreadDumpInfo> requestThreadDump(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerDispatcher(DispatcherRegistrationRequest dispatcherRegistrationRequest, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getAddress() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getHostname() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Collection<LogInfo>> requestLogList(Time timeout) {
		throw new UnsupportedOperationException();
	}
}
