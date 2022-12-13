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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.JobSlotRequestList;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ClusterPartitionManager;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link ResourceManager}'s RPC gateway interface.
 */
public interface ResourceManagerGateway extends FencedRpcGateway<ResourceManagerId>, ClusterPartitionManager {

	/**
	 * Register a {@link Dispatcher} at the resource manager.
	 *
	 * @param dispatcherId The fencing token for the Dispatcher leader
	 * @param dispatcherResourceId The resource ID of the Dispatcher that registers
	 * @param dispatcherAddress The address of the Dispatcher that registers
	 * @param timeout Timeout for the future to complete
	 * @return Future registration response
	 */
	CompletableFuture<RegistrationResponse> registerDispatcher(
		DispatcherId dispatcherId,
		ResourceID dispatcherResourceId,
		String dispatcherAddress,
		@RpcTimeout Time timeout);

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterId The fencing token for the JobMaster leader
	 * @param jobMasterResourceId The resource ID of the JobMaster that registers
	 * @param jobMasterAddress The address of the JobMaster that registers
	 * @param jobId The Job ID of the JobMaster that registers
	 * @param timeout Timeout for the future to complete
	 * @return Future registration response
	 */
	CompletableFuture<RegistrationResponse> registerJobManager(
		JobMasterId jobMasterId,
		ResourceID jobMasterResourceId,
		String jobMasterAddress,
		JobID jobId,
		JobInfo jobInfo,
		@RpcTimeout Time timeout);

	/**
	 * Requests a slot from the resource manager.
	 *
	 * @param jobMasterId id of the JobMaster
	 * @param slotRequest The slot to request
	 * @return The confirmation that the slot gets allocated
	 */
	CompletableFuture<Acknowledge> requestSlot(
		JobMasterId jobMasterId,
		SlotRequest slotRequest,
		@RpcTimeout Time timeout);

	CompletableFuture<Acknowledge> requestJobSlots(
		JobMasterId jobMasterId,
		JobSlotRequestList jobSlotRequestList,
		@RpcTimeout Time timeout);

	/**
	 * Request several task managers from the dispatcher.
	 * @param numNewTaskManagerRequests
	 * @param timeout
	 * @return The confirmation that request received
	 */
	CompletableFuture<Acknowledge> requestTaskManager(
		int numNewTaskManagerRequests,
		@RpcTimeout Time timeout);

	/**
	 * Cancel the slot allocation requests from the resource manager.
	 *
	 * @param allocationID The slot to request
	 */
	void cancelSlotRequest(AllocationID allocationID);

	/**
	 * Register a {@link TaskExecutor} at the resource manager.
	 *
	 * @param taskExecutorRegistration the task executor registration.
	 * @param timeout The timeout for the response.
	 *
	 * @return The future to the response by the ResourceManager.
	 */
	CompletableFuture<RegistrationResponse> registerTaskExecutor(
		TaskExecutorRegistration taskExecutorRegistration,
		@RpcTimeout Time timeout);

	/**
	 * Sends the given {@link SlotReport} to the ResourceManager.
	 *
	 * @param taskManagerRegistrationId id identifying the sending TaskManager
	 * @param slotReport which is sent to the ResourceManager
	 * @param timeout for the operation
	 * @return Future which is completed with {@link Acknowledge} once the slot report has been received.
	 */
	CompletableFuture<Acknowledge> sendSlotReport(
		ResourceID taskManagerResourceId,
		InstanceID taskManagerRegistrationId,
		SlotReport slotReport,
		@RpcTimeout Time timeout);

	/**
	 * Sent by the TaskExecutor to notify the ResourceManager that a slot has become available.
	 *
	 * @param instanceId TaskExecutor's instance id
	 * @param slotID The SlotID of the freed slot
	 * @param oldAllocationId to which the slot has been allocated
	 */
	void notifySlotAvailable(
		InstanceID instanceId,
		SlotID slotID,
		AllocationID oldAllocationId);

	/**
	 * Sent by JobMaster to notify the ResourceManager that a slot has become available.
	 *
	 * @param allocateSlotIds The SlotID and AllocationID of the freed slot.
	 */
	void notifyAllocateSlotsAvailable(Collection<AllocateSlotID> allocateSlotIds);

	/**
	 * Deregister Flink from the underlying resource management system.
	 *
	 * @param finalStatus final status with which to deregister the Flink application
	 * @param diagnostics additional information for the resource management system, can be {@code null}
	 */
	CompletableFuture<Acknowledge> deregisterApplication(final ApplicationStatus finalStatus, @Nullable final String diagnostics);

	/**
	 * Gets the currently registered number of TaskManagers.
	 *
	 * @return The future to the number of registered TaskManagers.
	 */
	CompletableFuture<Integer> getNumberOfRegisteredTaskManagers();

	/**
	 * Sends the heartbeat to resource manager from task manager.
	 *
	 * @param heartbeatOrigin unique id of the task manager
	 * @param heartbeatPayload payload from the originating TaskManager
	 */
	void heartbeatFromTaskManager(final ResourceID heartbeatOrigin, final TaskExecutorHeartbeatPayload heartbeatPayload);

	/**
	 * Sends the heartbeat to resource manager from job manager.
	 *
	 * @param heartbeatOrigin unique id of the job manager
	 */
	void heartbeatFromJobManager(final ResourceID heartbeatOrigin);

	/**
	 * Sends the heartbeat to resource manager from dispatcher.
	 *
	 * @param heartbeatOrigin unique id of the dispatcher
	 */
	void heartbeatFromDispatcher(final ResourceID heartbeatOrigin);

	/**
	 * Disconnects a Dispatcher specified by the given resourceID from the {@link ResourceManager}.
	 *
	 * @param dispatcherId identifying the Dispatcher to disconnect
	 * @param cause for the disconnection of the Dispatcher
	 */
	void disconnectDispatcher(ResourceID dispatcherId, Exception cause);

	/**
	 * Disconnects a TaskManager specified by the given resourceID from the {@link ResourceManager}.
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 * @param cause for the disconnection of the TaskManager
	 */
	void disconnectTaskManager(ResourceID resourceID, Exception cause);

	/**
	 * JobManager disconnects TaskManager specified by the given resourceID from the {@link ResourceManager}.
	 *
	 * @param resourceID identifying the TaskManager to release
	 * @param exitCode the exit code of release
	 * @param cause for the release of the TaskManager
	 */
	void releaseTaskManager(ResourceID resourceID, Integer exitCode, Exception cause);

	/**
	 * Disconnects a JobManager specified by the given resourceID from the {@link ResourceManager}.
	 *
	 * @param jobId JobID for which the JobManager was the leader
	 * @param jobStatus status of the job at the time of disconnection
	 * @param cause for the disconnection of the JobManager
	 */
	void disconnectJobManager(JobID jobId, JobStatus jobStatus, Exception cause);

	/**
	 * Requests information about the registered {@link TaskExecutor}.
	 *
	 * @param timeout of the request
	 * @return Future collection of TaskManager information
	 */
	CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(@RpcTimeout Time timeout);

	/**
	 * Requests information about the given {@link TaskExecutor}.
	 *
	 * @param taskManagerId identifying the TaskExecutor for which to return information
	 * @param timeout of the request
	 * @return Future TaskManager information
	 */
	CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID taskManagerId, @RpcTimeout Time timeout);

	/**
	 * Requests the resource overview. The resource overview provides information about the
	 * connected TaskManagers, the total number of slots and the number of available slots.
	 *
	 * @param timeout of the request
	 * @return Future containing the resource overview
	 */
	CompletableFuture<ResourceOverview> requestResourceOverview(@RpcTimeout Time timeout);

	/**
	 * Requests the JMWebShell url from Yarn.
	 *
	 * @param timeout of the request
	 * @return Future containing the resource overview
	 */
	default CompletableFuture<String> requestJMWebShell(@RpcTimeout Time timeout) {
		CompletableFuture<String> jmWebShell = new CompletableFuture<>();
		jmWebShell.completeExceptionally(new Exception("no JobManager WebShell"));
		return jmWebShell;
	}

	/**
	 * Requests the JobManagerLog url.
	 *
	 * @param timeout of the request
	 * @return Future containing the resource overview
	 */
	default CompletableFuture<String> requestJobManagerLogUrl(@RpcTimeout Time timeout) {
		CompletableFuture<String> jmLog = new CompletableFuture<>();
		jmLog.completeExceptionally(new Exception("no JobManager Log"));
		return jmLog;
	}

	/**
	 * Requests the paths for the TaskManager's {@link MetricQueryService} to query.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of resource ids and the corresponding metric query service path
	 */
	CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(@RpcTimeout Time timeout);

	/**
	 * Request the file upload from the given {@link TaskExecutor} to the cluster's {@link BlobServer}. The
	 * corresponding {@link TransientBlobKey} is returned.
	 *
	 * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
	 * @param fileType type of the file to upload
	 * @param timeout for the asynchronous operation
	 * @return Future which is completed with the {@link TransientBlobKey} after uploading the file to the
	 * {@link BlobServer}.
	 */
	CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(ResourceID taskManagerId, FileType fileType, @RpcTimeout Time timeout);

	/**
	 * Request preview data from task in taskManager.
	 * @param taskManagerId identifying the {@link TaskExecutor} to get preview data.
	 * @param jobId the job of this taskManager, in session mode will have multiple jobs.
	 * @param jobVertexId the jobVertexId of job, in one taskManager will have multiple preview subTask.
	 * @param timeout for the asynchronous operation
	 * @return previewData {@link PreviewDataResponse}
	 */
	CompletableFuture<PreviewDataResponse> requestTaskManagerPreviewData(ResourceID taskManagerId, JobID jobId, JobVertexID jobVertexId, @RpcTimeout Time timeout);

	/**
	 * Request the file upload from the given {@link TaskExecutor} to the cluster's {@link BlobServer}. The
	 * corresponding {@link TransientBlobKey} is returned.
	 *
	 * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
	 * @param fileName name of the file to upload
	 * @param timeout for the asynchronous operation
	 * @return Future which is completed with the {@link TransientBlobKey} after uploading the file to the
	 * {@link BlobServer}.
	 */
	CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByName(ResourceID taskManagerId, String fileName, @RpcTimeout Time timeout);

	/**
	 * Request log list from the given {@link TaskExecutor}.
	 * @param taskManagerId identifying the {@link TaskExecutor} to get log list from
	 * @param timeout for the asynchronous operation
	 * @return Future which is completed with the historical log list
	 */
	CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(ResourceID taskManagerId, @RpcTimeout Time timeout);

	/**
	 * Requests the thread dump from the given {@link TaskExecutor}.
	 *
	 * @param taskManagerId taskManagerId identifying the {@link TaskExecutor} to get the thread dump from
	 * @param timeout timeout of the asynchronous operation
	 * @return Future containing the thread dump information
	 */
	CompletableFuture<ThreadDumpInfo> requestThreadDump(ResourceID taskManagerId, @RpcTimeout Time timeout);

	default CompletableFuture<SmartResourcesStats> requestSmartResourcesStats(@RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	default void clearBlacklist(
			JobID jobID,
			JobMasterId jobMasterId,
			@RpcTimeout Time timeout) { }

	default void onTaskFailure(
			JobID jobID,
			JobMasterId jobMasterId,
			BlacklistUtil.FailureType failureType,
			String hostname,
			ResourceID taskManagerId,
			Throwable cause,
			long timestamp,
			@RpcTimeout Time timeout) { }

	default void addIgnoreExceptionClass(
			JobID jobID,
			JobMasterId jobMasterId,
			Class<? extends Throwable> exceptionClass) { }
}
