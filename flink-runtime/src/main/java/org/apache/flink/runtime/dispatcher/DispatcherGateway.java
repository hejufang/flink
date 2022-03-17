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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.socket.TaskJobResultGateway;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for the Dispatcher component.
 */
public interface DispatcherGateway extends FencedRpcGateway<DispatcherId>, RestfulGateway, TaskJobResultGateway {

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
	 * Offer TaskManagers topology to the dispatcher.
	 *
	 * @param taskManagerTopologies provided by ResourceManager
	 * @param timeout RPC timeout
	 * @return A future acknowledge if the receiving succeeded
	 */
	CompletableFuture<Acknowledge> offerTaskManagers(
		Collection<TaskManagerTopology> taskManagerTopologies,
		@RpcTimeout Time timeout);

	/**
	 * Sends heartbeat request from the resource manager.
	 *
	 * @param resourceID unique id of the resource manager
	 */
	void heartbeatFromResourceManager(final ResourceID resourceID);

	/**
	 * Report usage of TaskManagers from JobMaster.
	 *
	 * @param jobID identifying the job to update resource bookkeeping
	 * @param usedTaskManagers current used TaskManagers by the job
	 * @param timeout RPC timeout
	 * @return
	 */
	CompletableFuture<Acknowledge> reportTaskManagerUsage(
		JobID jobID,
		Collection<ResourceID> usedTaskManagers,
		@RpcTimeout Time timeout);
}
