/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License")); you may not use this file except in compliance
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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.BatchTaskExecutionState;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * Dispatcher which proxy jobMaster requests.
 */
public abstract class JobMaterProxyDispatcher extends Dispatcher {

	public JobMaterProxyDispatcher(RpcService rpcService, DispatcherId fencingToken, DispatcherBootstrap dispatcherBootstrap, DispatcherServices dispatcherServices) throws Exception {
		super(rpcService, fencingToken, dispatcherBootstrap, dispatcherServices);
	}

	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(final JobID jobId, final TaskExecutionState taskExecutionState) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway)
			-> jobMasterGateway.updateTaskExecutionState(taskExecutionState));
	}

	@Override
	public CompletableFuture<Acknowledge> batchUpdateTaskExecutionState(final BatchTaskExecutionState batchTaskExecutionState) {
		return getJobMasterGatewayFuture(batchTaskExecutionState.getJobId()).thenCompose((JobMasterGateway jobMasterGateway)
			-> jobMasterGateway.batchUpdateTaskExecutionState(batchTaskExecutionState));
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
		final JobID jobId,
		final JobVertexID vertexID,
		final ExecutionAttemptID executionAttempt) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestNextInputSplit(vertexID, executionAttempt));
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(
		final JobID jobId,
		final IntermediateDataSetID intermediateResultId,
		final ResultPartitionID partitionId) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestPartitionState(intermediateResultId, partitionId));
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
		final JobID jobId,
		final ResultPartitionID partitionID,
		final Time timeout) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.scheduleOrUpdateConsumers(partitionID, timeout));
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(final JobID jobId, ResourceID resourceID, Exception cause) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.disconnectTaskManager(resourceID, cause));
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(final JobID jobId, String aggregateName, Object aggregand, byte[] serializedAggregationFunction) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.updateGlobalAggregate(aggregateName, aggregand, serializedAggregationFunction));
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(final JobID jobId, ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event) {
		return getJobMasterGatewayFuture(jobId).thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.sendOperatorEventToCoordinator(task, operatorID, event));
	}
}
