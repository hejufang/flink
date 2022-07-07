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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.BatchTaskExecutionState;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for the Dispatcher component.
 */
public interface DispatcherGateway extends FencedRpcGateway<DispatcherId>, RestfulGateway {

	/**
	 * Submit a job to the dispatcher.
	 *
	 * @param jobGraph JobGraph to submit
	 * @param timeout RPC timeout
	 * @return A future acknowledge if the submission succeeded
	 */
	CompletableFuture<Acknowledge> submitJob(
		JobGraph jobGraph,
		@RpcTimeout Time timeout);

	/**
	 * Submit a job to the dispatcher with channel context.
	 *
	 * @param jobGraph JobGraph to submit
	 * @param ctx channel context the job be submitted
	 * @param timeout RPC timeout
	 */
	void submitJob(
		JobGraph jobGraph,
		ChannelHandlerContext ctx,
		@RpcTimeout Time timeout);

	/**
	 * List the current set of submitted jobs.
	 *
	 * @param timeout RPC timeout
	 * @return A future collection of currently submitted jobs
	 */
	CompletableFuture<Collection<JobID>> listJobs(
		@RpcTimeout Time timeout);

	/**
	 * Returns the port of the blob server.
	 *
	 * @param timeout of the operation
	 * @return A future integer of the blob server port
	 */
	CompletableFuture<Integer> getBlobServerPort(@RpcTimeout Time timeout);

	/**
	 * Requests the {@link ArchivedExecutionGraph} for the given jobId. If there is no such graph, then
	 * the future is completed with a {@link FlinkJobNotFoundException}.
	 *
	 * <p>Note: We enforce that the returned future contains a {@link ArchivedExecutionGraph} unlike
	 * the super interface.
	 *
	 * @param jobId identifying the job whose AccessExecutionGraph is requested
	 * @param timeout for the asynchronous operation
	 * @return Future containing the AccessExecutionGraph for the given jobId, otherwise {@link FlinkJobNotFoundException}
	 */
	@Override
	CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, @RpcTimeout Time timeout);

	default CompletableFuture<Acknowledge> shutDownCluster(ApplicationStatus applicationStatus) {
		return shutDownCluster();
	}

	/**
	 * Disconnects the resource manager from the dispatcher because of the given cause.
	 *
	 * @param resourceManagerId identifying the resource manager leader id
	 * @param cause of the disconnect
	 */
	void disconnectResourceManager(final ResourceManagerId resourceManagerId, final Exception cause);

	/**
	 * Disconnects the task executor from the dispatcher because of the given cause.
	 * @param resourceID identifying the task executor id
	 * @param cause of the disconnect
	 */
	void disconnectTaskExecutor(final ResourceID resourceID, final Exception cause);

	/**
	 * Offer TaskManagers topology to the dispatcher.
	 *
	 * @param taskManagerTopologies provided by ResourceManager
	 * @param timeout RPC timeout
	 * @return A future acknowledge if the receiving succeeded
	 */
	CompletableFuture<Acknowledge> offerTaskManagers(
		Collection<UnresolvedTaskManagerTopology> taskManagerTopologies,
		@RpcTimeout Time timeout);

	/**
	 * Sends heartbeat request from the resource manager.
	 *
	 * @param resourceID unique id of the resource manager.
	 */
	void heartbeatFromResourceManager(final ResourceID resourceID);

	/**
	 * Sends heartbeat request from taskExecutor.
	 *
	 * @param resourceID unique id of the task executor.
	 * @param payload of heartbeat.
	 */
	void heartbeatFromTaskExecutor(final ResourceID resourceID, Void payload);

	//----------------------------------------------------------------------------------------------
	// Proxy jobMaster API
	//----------------------------------------------------------------------------------------------
	/**
	 * Registers the task manager at the job manager.
	 *
	 * @param taskManagerRpcAddress the rpc address of the task manager
	 * @param timeout               for the rpc call
	 * @return Future registration response indicating whether the registration was successful or not
	 */
	CompletableFuture<EstablishedTaskExecutorConnection> registerTaskManager(
		final String taskManagerRpcAddress,
		final ResourceID taskManagerId,
		@RpcTimeout final Time timeout);

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	CompletableFuture<Acknowledge> updateTaskExecutionState(
		final JobID jobId,
		final TaskExecutionState taskExecutionState);

	/**
	 * Batch update task execution states.
	 *
	 * @param batchTaskExecutionState batch task execution state
	 * @return Future flag of the batch execution state update result
	 */
	CompletableFuture<Acknowledge> batchUpdateTaskExecutionState(final BatchTaskExecutionState batchTaskExecutionState);

	/**
	 * Requests the next input split for the {@link ExecutionJobVertex}.
	 * The next input split is sent back to the sender as a
	 * {@link SerializedInputSplit} message.
	 *
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 */
	CompletableFuture<SerializedInputSplit> requestNextInputSplit(
		final JobID jobId,
		final JobVertexID vertexID,
		final ExecutionAttemptID executionAttempt);

	/**
	 * Requests the current state of the partition. The state of a
	 * partition is currently bound to the state of the producing execution.
	 *
	 * @param intermediateResultId The execution attempt ID of the task requesting the partition state.
	 * @param partitionId          The partition ID of the partition to request the state of.
	 * @return The future of the partition state
	 */
	CompletableFuture<ExecutionState> requestPartitionState(
		final JobID jobId,
		final IntermediateDataSetID intermediateResultId,
		final ResultPartitionID partitionId);

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 *
	 * <p>There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 *
	 * <p>The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param partitionID The partition which has already produced data
	 * @param timeout     before the rpc call fails
	 * @return Future acknowledge of the schedule or update operation
	 */
	CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
		final JobID jobId,
		final ResultPartitionID partitionID,
		@RpcTimeout final Time timeout);

	/**
	 * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
	 * {@link JobMaster}.
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 * @param cause      for the disconnection of the TaskManager
	 * @return Future acknowledge once the JobMaster has been disconnected from the TaskManager
	 */
	CompletableFuture<Acknowledge> disconnectTaskManager(final JobID jobId, ResourceID resourceID, Exception cause);

	/**
	 * Update the aggregate and return the new value.
	 *
	 * @param aggregateName                 The name of the aggregate to update
	 * @param aggregand                     The value to add to the aggregate
	 * @param serializedAggregationFunction The function to apply to the current aggregate and aggregand to
	 *                                      obtain the new aggregate value, this should be of type {@link AggregateFunction}
	 * @return The updated aggregate
	 */
	CompletableFuture<Object> updateGlobalAggregate(final JobID jobId, String aggregateName, Object aggregand, byte[] serializedAggregationFunction);

	CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(final JobID jobId, ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event);
}
