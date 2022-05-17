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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.JobSlotRequestList;
import org.apache.flink.runtime.blacklist.BlacklistActions;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.reporter.BlacklistReporter;
import org.apache.flink.runtime.blacklist.tracker.BlacklistTracker;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherRegistrationSuccess;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.MaximumFailedTaskManagerExceedingException;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.registration.DispatcherRegistration;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.NoSlotWorkerManagerImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceActions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskmanager.TaskManagerAddressLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.LoggerHelper;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #registerJobManager(JobMasterId, ResourceID, String, JobID, JobInfo, Time)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(JobMasterId, SlotRequest, Time)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
		extends FencedRpcEndpoint<ResourceManagerId>
		implements ResourceManagerGateway, LeaderContender {

	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	/** Unique id of the resource manager. */
	private final ResourceID resourceId;

	/** All currently registered JobMasterGateways scoped by JobID. */
	private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

	/** All currently registered JobMasterGateways scoped by ResourceID. */
	private final Map<ResourceID, JobManagerRegistration> jmResourceIdRegistrations;

	/** Service to retrieve the job leader ids. */
	private final JobLeaderIdService jobLeaderIdService;

	/** All currently registered TaskExecutors with there framework specific worker information. */
	private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

	/** Ongoing registration of TaskExecutors per resource ID. */
	private final Map<ResourceID, CompletableFuture<TaskExecutorGateway>> taskExecutorGatewayFutures;

	private final Map<ResourceID, CompletableFuture<DispatcherGateway>> dispatcherGatewayFutures;

	private final Map<ResourceID, DispatcherRegistration> dispatcherRegistrations;

	private final Map<ResourceID, UnresolvedTaskManagerTopology> taskExecutorTopology;

	/** High availability services for leader retrieval and election. */
	private final HighAvailabilityServices highAvailabilityServices;

	private final HeartbeatServices heartbeatServices;

	private final Counter heartbeatTimeoutWithJM = new SimpleCounter();

	private final Counter heartbeatTimeoutWithTM = new SimpleCounter();

	private final Time rpcTimeout;

	/** Fatal error handler. */
	private final FatalErrorHandler fatalErrorHandler;

	/** The slot manager maintains the available slots. */
	private final SlotManager slotManager;

	private final ResourceManagerPartitionTracker clusterPartitionTracker;

	protected final BlacklistTracker blacklistTracker;

	protected final BlacklistReporter blacklistReporter;

	private final ClusterInformation clusterInformation;

	protected final ResourceManagerMetricGroup resourceManagerMetricGroup;

	private final FailureRater failureRater;

	private final boolean jobLogDetailDisable;

	private final boolean requestSlotFromResourceManagerDirectEnable;

	private final boolean requestWorkerDirectlyEnable;

	private final int maxTasksPerWorker;

	private final int minWorkersPerJob;

	private Boolean jmResourceAllocationEnabled;

	/** The service to elect a ResourceManager leader. */
	private LeaderElectionService leaderElectionService;

	/** The heartbeat manager with task managers. */
	private HeartbeatManager<TaskExecutorHeartbeatPayload, Void> taskManagerHeartbeatManager;

	/** The heartbeat manager with job managers. */
	private HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

	/** The heartbeat manager with dispatchers. */
	private HeartbeatManager<Void, Void> dispatcherHeartbeatManager;

	/**
	 * Represents asynchronous state clearing work.
	 *
	 * @see #clearStateAsync()
	 * @see #clearStateInternal()
	 */
	private CompletableFuture<Void> clearStateFuture = CompletableFuture.completedFuture(null);

	private boolean waitingFatal;
	private String fatalMessage;

	public ResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			Time rpcTimeout,
			FailureRater failureRater) {
		this(rpcService,
				resourceId,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				clusterPartitionTrackerFactory,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup,
				rpcTimeout,
				failureRater,
				new Configuration());
	}

	public ResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			Time rpcTimeout,
			FailureRater failureRater,
			Configuration flinkConfig) {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(RESOURCE_MANAGER_NAME), null);

		this.resourceId = checkNotNull(resourceId);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.heartbeatServices = checkNotNull(heartbeatServices);
		this.slotManager = checkNotNull(slotManager);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.clusterInformation = checkNotNull(clusterInformation);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.resourceManagerMetricGroup = checkNotNull(resourceManagerMetricGroup);
		this.failureRater = checkNotNull(failureRater);
		this.waitingFatal = false;

		this.jobManagerRegistrations = new HashMap<>(4);
		this.jmResourceIdRegistrations = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		this.taskExecutorGatewayFutures = new HashMap<>(8);
		this.dispatcherGatewayFutures = new HashMap<>(4);
		this.dispatcherRegistrations = new HashMap<>(4);
		this.taskExecutorTopology = new HashMap<>(8);

		this.rpcTimeout = rpcTimeout;
		this.jmResourceAllocationEnabled = flinkConfig.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED);

		this.jobManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.dispatcherHeartbeatManager = NoOpHeartbeatManager.getInstance();

		this.clusterPartitionTracker = checkNotNull(clusterPartitionTrackerFactory).get(
			(taskExecutorResourceId, dataSetIds) -> taskExecutors.get(taskExecutorResourceId).getTaskExecutorGateway()
				.releaseClusterPartitions(dataSetIds, rpcTimeout)
				.exceptionally(throwable -> {
					log.debug("Request for release of cluster partitions belonging to data sets {} was not successful.", dataSetIds, throwable);
					throw new CompletionException(throwable);
				})
		);

		this.blacklistTracker = BlacklistUtil.createBlacklistTracker(flinkConfig, resourceManagerMetricGroup);
		this.blacklistReporter = BlacklistUtil.createLocalBlacklistReporter(flinkConfig, blacklistTracker);
		this.jobLogDetailDisable = flinkConfig.getBoolean(CoreOptions.FLINK_JOB_LOG_DETAIL_DISABLE);
		this.requestSlotFromResourceManagerDirectEnable = flinkConfig.getBoolean(JobManagerOptions.JOBMANAGER_REQUEST_SLOT_FROM_RESOURCEMANAGER_ENABLE);
		this.requestWorkerDirectlyEnable = flinkConfig.getBoolean(JobManagerOptions.JOBMANAGER_REQUEST_WORKER_DIRECTLY_ENABLE);
		this.maxTasksPerWorker = flinkConfig.getInteger(JobManagerOptions.JOBMANAGER_MAX_TASKS_PER_JOB);
		this.minWorkersPerJob = flinkConfig.getInteger(JobManagerOptions.JOBMANAGER_MIN_WORKERS_PER_JOB);
	}

	// ------------------------------------------------------------------------
	//  RPC lifecycle methods
	// ------------------------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		try {
			startResourceManagerServices();
		} catch (Exception e) {
			final ResourceManagerException exception = new ResourceManagerException(String.format("Could not start the ResourceManager %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}
	}

	private void startResourceManagerServices() throws Exception {
		try {
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();

			initialize();

			leaderElectionService.start(this);
			jobLeaderIdService.start(new JobLeaderIdActionsImpl());

			registerTaskExecutorMetrics();
			registerJobManagerMetrics();
		} catch (Exception e) {
			handleStartResourceManagerServicesException(e);
		}
	}

	private void handleStartResourceManagerServicesException(Exception e) throws Exception {
		try {
			stopResourceManagerServices();
		} catch (Exception inner) {
			e.addSuppressed(inner);
		}

		throw e;
	}

	@Override
	public CompletableFuture<Void> onStop() {
		try {
			stopResourceManagerServices();
		} catch (Exception exception) {
			return FutureUtils.completedExceptionally(
				new FlinkException("Could not properly shut down the ResourceManager.", exception));
		}

		return CompletableFuture.completedFuture(null);
	}

	private void stopResourceManagerServices() throws Exception {
		Exception exception = null;

		stopHeartbeatServices();

		try {
			slotManager.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			blacklistTracker.close();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			jobLeaderIdService.stop();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		resourceManagerMetricGroup.close();

		clearStateInternal();

		ExceptionUtils.tryRethrowException(exception);
	}

	// ------------------------------------------------------------------------
	//  RPC methods
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(
			final JobMasterId jobMasterId,
			final ResourceID jobManagerResourceId,
			final String jobManagerAddress,
			final JobID jobId,
			final JobInfo jobInfo,
			final Time timeout) {

		checkNotNull(jobMasterId);
		checkNotNull(jobManagerResourceId);
		checkNotNull(jobManagerAddress);
		checkNotNull(jobId);

		if (!jobLeaderIdService.containsJob(jobId)) {
			try {
				jobLeaderIdService.addJob(jobId);
			} catch (Exception e) {
				ResourceManagerException exception = new ResourceManagerException("Could not add the job " +
					jobId + " to the job id leader service.", e);

					onFatalError(exception);

				log.error("Could not add job {} to job leader id service.", jobId, e);
				return FutureUtils.completedExceptionally(exception);
			}
		}

		log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

		CompletableFuture<JobMasterId> jobMasterIdFuture;

		try {
			jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
		} catch (Exception e) {
			// we cannot check the job leader id so let's fail
			// TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
			ResourceManagerException exception = new ResourceManagerException("Cannot obtain the " +
				"job leader id future to verify the correct job leader.", e);

				onFatalError(exception);

			log.debug("Could not obtain the job leader id future to verify the correct job leader.");
			return FutureUtils.completedExceptionally(exception);
		}

		CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

		CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
			jobMasterIdFuture,
			(JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
				if (Objects.equals(leadingJobMasterId, jobMasterId)) {
					return registerJobMasterInternal(
						jobMasterGateway,
						jobId,
						jobInfo,
						jobManagerAddress,
						jobManagerResourceId);
				} else {
					final String declineMessage = String.format(
						"The leading JobMaster id %s did not match the received JobMaster id %s. " +
						"This indicates that a JobMaster leader change has happened.",
						leadingJobMasterId,
						jobMasterId);
					log.debug(declineMessage);
					return new RegistrationResponse.Decline(declineMessage);
				}
			},
			getMainThreadExecutor());

		// handle exceptions which might have occurred in one of the futures inputs of combine
		return registrationResponseFuture.handleAsync(
			(RegistrationResponse registrationResponse, Throwable throwable) -> {
				if (throwable != null) {
					if (log.isDebugEnabled()) {
						log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
					} else {
						log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
					}

					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					return registrationResponse;
				}
			},
			getRpcService().getExecutor());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskExecutor(
			final TaskExecutorRegistration taskExecutorRegistration,
			final Time timeout) {

		CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture = getRpcService().connect(taskExecutorRegistration.getTaskExecutorAddress(), TaskExecutorGateway.class);
		taskExecutorGatewayFutures.put(taskExecutorRegistration.getResourceId(), taskExecutorGatewayFuture);

		return taskExecutorGatewayFuture.handleAsync(
			(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
				final ResourceID resourceId = taskExecutorRegistration.getResourceId();
				if (taskExecutorGatewayFuture == taskExecutorGatewayFutures.get(resourceId)) {
					taskExecutorGatewayFutures.remove(resourceId);
					if (throwable != null) {
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						return registerTaskExecutorInternal(taskExecutorGateway, taskExecutorRegistration);
					}
				} else {
					log.debug("Ignoring outdated TaskExecutorGateway connection for {}.", resourceId);
					return new RegistrationResponse.Decline("Decline outdated task executor registration.");
				}
			},
			getMainThreadExecutor());
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerDispatcher(
			final DispatcherId dispatcherId,
			final ResourceID dispatcherResourceId,
			final String dispatcherAddress,
			final Time timeout) {
		CompletableFuture<DispatcherGateway> dispatcherGatewayFuture = getRpcService().connect(dispatcherAddress, dispatcherId, DispatcherGateway.class);
		dispatcherGatewayFutures.put(dispatcherResourceId, dispatcherGatewayFuture);

		return dispatcherGatewayFuture.handleAsync(
			(DispatcherGateway dispatcherGateway, Throwable throwable) -> {
				if (dispatcherGatewayFuture == dispatcherGatewayFutures.get(dispatcherResourceId)) {
					dispatcherGatewayFutures.remove(dispatcherResourceId);
					if (throwable != null) {
						return new RegistrationResponse.Decline(throwable.getMessage());
					} else {
						return registerDispatcherInternal(dispatcherGateway, dispatcherResourceId, dispatcherAddress);
					}
				} else {
					log.debug("Ignoring outdated DispatcherGateway connection for {}.", dispatcherId);
					return new RegistrationResponse.Decline("Decline outdated dispatcher registration.");
				}
			},
			getMainThreadExecutor());
	}

		@Override
	public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
		final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);

		if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
			TaskManagerAddressLocation taskManagerAddressLocation = workerTypeWorkerRegistration.getTaskManagerAddressLocation();
			if (slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport, taskManagerAddressLocation)) {
				onTaskManagerRegistration(workerTypeWorkerRegistration);
			}
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException(String.format("Unknown TaskManager registration id %s.", taskManagerRegistrationId)));
		}
	}

	protected void onTaskManagerRegistration(WorkerRegistration<WorkerType> workerTypeWorkerRegistration) {
		// noop
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, final TaskExecutorHeartbeatPayload heartbeatPayload) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, heartbeatPayload);
	}

	@Override
	public void heartbeatFromJobManager(final ResourceID resourceID) {
		jobManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
	}

	@Override
	public void heartbeatFromDispatcher(final ResourceID resourceID) {
		dispatcherHeartbeatManager.receiveHeartbeat(resourceID, null);
	}

	@Override
	public void disconnectTaskManager(final ResourceID resourceId, final Exception cause) {
		log.error("Task manager {} is disconnecting with resource manager because of {}.", LoggerHelper.secMark("resourceID", resourceId), LoggerHelper.secMark("errMsg", cause.getMessage()));
		closeTaskManagerConnection(resourceId, cause, WorkerExitCode.EXIT_BY_TASK_MANAGER);
	}

	@Override
	public void releaseTaskManager(final ResourceID resourceId, final Exception cause) {
		closeTaskManagerConnection(resourceId, cause, WorkerExitCode.EXIT_BY_JOB_MANAGER);
	}

	@Override
	public void disconnectJobManager(final JobID jobId, JobStatus jobStatus, final Exception cause) {
		if (jobStatus.isGloballyTerminalState()) {
			removeJob(jobId);
		} else {
			closeJobManagerConnection(jobId, cause);
		}
	}

	@Override
	public void disconnectDispatcher(final ResourceID dispatcherId, final Exception cause) {
		closeDispatcherConnection(dispatcherId, cause);
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(
			JobMasterId jobMasterId,
			SlotRequest slotRequest,
			final Time timeout) {

		JobID jobId = slotRequest.getJobId();
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

		if (null != jobManagerRegistration) {
			if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {
				if (jobLogDetailDisable) {
					log.debug("Request slot with profile {} for job {} with allocation id {}.",
						slotRequest.getResourceProfile(),
						slotRequest.getJobId(),
						slotRequest.getAllocationId());
				} else {
					log.info("Request slot with profile {} for job {} with allocation id {}.",
						slotRequest.getResourceProfile(),
						slotRequest.getJobId(),
						slotRequest.getAllocationId());
				}

				try {
					slotManager.registerSlotRequest(slotRequest);
				} catch (ResourceManagerException e) {
					return FutureUtils.completedExceptionally(e);
				}

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				return FutureUtils.completedExceptionally(new ResourceManagerException("The job leader's id " +
					jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));
			}

		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
		}
	}

	public CompletableFuture<Acknowledge> requestJobSlots(
			JobMasterId jobMasterId,
			JobSlotRequestList jobSlotRequestList,
			@RpcTimeout Time timeout) {

		JobID jobId = jobSlotRequestList.getJobId();
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

		if (null != jobManagerRegistration) {
			if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {
				log.info("Request slot list({}) for job {}.",
					jobSlotRequestList.getSlotRequests().size(),
					jobId);

				try {
					if (requestSlotFromResourceManagerDirectEnable) {
						if (requestWorkerDirectlyEnable) {
							slotManager.registerJobSlotRequests(
								jobManagerRegistration.getJobManagerGateway(),
								jobSlotRequestList,
								computeJobWorkerCount(
									jobSlotRequestList.getSlotRequests().size(),
									jobSlotRequestList.getTaskCount(),
									maxTasksPerWorker,
									minWorkersPerJob,
									taskExecutors.size()));
						} else {
							slotManager.registerJobSlotRequests(
								jobManagerRegistration.getJobManagerGateway(),
								jobSlotRequestList);
						}
					} else {
						slotManager.registerJobSlotRequests(jobSlotRequestList);
					}
				} catch (ResourceManagerException e) {
					return FutureUtils.completedExceptionally(e);
				}

				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				return FutureUtils.completedExceptionally(new ResourceManagerException("The job leader's id " +
					jobManagerRegistration.getJobMasterId() + " does not match the received id " + jobMasterId + '.'));
			}

		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException("Could not find registered job manager for job " + jobId + '.'));
		}
	}

	@Override
	public CompletableFuture<Acknowledge> requestTaskManager(int numNewTaskManagerRequests, @RpcTimeout Time timeout) {
		checkArgument(slotManager instanceof NoSlotWorkerManagerImpl);

		((NoSlotWorkerManagerImpl) slotManager).requestNewTaskManagers(numNewTaskManagerRequests);

		if (log.isDebugEnabled()) {
			log.debug("Notify Dispatchers [{}] for latest TM topology",
				dispatcherRegistrations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		dispatcherRegistrations.values().forEach(
			dispatcherRegistration -> dispatcherRegistration
				.getDispatcherGateway()
				.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout)
		);

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void cancelSlotRequest(AllocationID allocationID) {
		// As the slot allocations are async, it can not avoid all redundant slots, but should best effort.
		slotManager.unregisterSlotRequest(allocationID);
	}

	@Override
	public void notifySlotAvailable(
			final InstanceID instanceID,
			final SlotID slotId,
			final AllocationID allocationId) {

		final ResourceID resourceId = slotId.getResourceID();
		WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

		if (registration != null) {
			InstanceID registrationId = registration.getInstanceID();

			if (Objects.equals(registrationId, instanceID)) {
				slotManager.freeSlot(slotId, allocationId);
			} else {
				log.debug("Invalid registration id for slot available message. This indicates an" +
					" outdated request.");
			}
		} else {
			log.debug("Could not find registration for resource id {}. Discarding the slot available" +
				"message {}.", resourceId, slotId);
		}
	}

	@Override
	public void notifyAllocateSlotsAvailable(Collection<AllocateSlotID> allocateSlotIds) {
		for (AllocateSlotID allocateSlotId : allocateSlotIds) {
			SlotID slotId = allocateSlotId.getSlotId();
			AllocationID allocationId = allocateSlotId.getAllocationId();
			final ResourceID resourceId = slotId.getResourceID();
			WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

			if (registration != null) {
				if (!requestWorkerDirectlyEnable) {
					slotManager.freeSlot(slotId, allocationId);
				}
			} else {
				log.debug("Could not find registration for resource id {}. Discarding the slot available" +
					"message {}.", resourceId, slotId);
			}
		}
	}

	/**
	 * Cleanup application and shut down cluster.
	 *
	 * @param finalStatus of the Flink application
	 * @param diagnostics diagnostics message for the Flink application or {@code null}
	 */
	@Override
	public CompletableFuture<Acknowledge> deregisterApplication(
			final ApplicationStatus finalStatus,
			@Nullable final String diagnostics) {
		ApplicationStatus applicationStatus = finalStatus;
		String errMsg = diagnostics;

		if (finalStatus != ApplicationStatus.FAILED && waitingFatal) {
			applicationStatus = ApplicationStatus.FAILED;
			errMsg = fatalMessage;
			log.info("ResourceManager is waiting fatal, transform appStatus from {} to {}", finalStatus, applicationStatus);
		} else {
			log.info("Shut down cluster because application is in {}, diagnostics {}.", applicationStatus, errMsg);
		}

		try {
			internalDeregisterApplication(applicationStatus, errMsg);
		} catch (ResourceManagerException e) {
			log.warn("Could not properly shutdown the application.", e);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
		return CompletableFuture.completedFuture(taskExecutors.size());
	}

	@Override
	public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {

		final ArrayList<TaskManagerInfo> taskManagerInfos = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> taskExecutorEntry : taskExecutors.entrySet()) {
			final ResourceID resourceId = taskExecutorEntry.getKey();
			final WorkerRegistration<WorkerType> taskExecutor = taskExecutorEntry.getValue();

			String host = taskExecutor.getTaskExecutorGateway().getHostname();
			String webShell = getTaskManagerWebShell(resourceId, host);
			String tmLog = getTaskManagerLogUrl(resourceId, host);
			log.debug("webShell = {}, tmLog = {}", webShell, tmLog);

			taskManagerInfos.add(
				new TaskManagerInfo(
					resourceId,
					taskExecutor.getTaskExecutorGateway().getAddress(),
					taskExecutor.getDataPort(),
					taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
					slotManager.getNumberRegisteredSlotsOf(taskExecutor.getInstanceID()),
					slotManager.getNumberFreeSlotsOf(taskExecutor.getInstanceID()),
					slotManager.getRegisteredResourceOf(taskExecutor.getInstanceID()),
					slotManager.getFreeResourceOf(taskExecutor.getInstanceID()),
					taskExecutor.getHardwareDescription(),
					taskExecutor.getMemoryConfiguration(),
					webShell,
					tmLog));
		}

		return CompletableFuture.completedFuture(taskManagerInfos);
	}

	public String getTaskManagerWebShell(ResourceID resourceId, String host) {
		return "";
	}

	public String getTaskManagerLogUrl(ResourceID resourceId, String host) {
		return "";
	}

	@Override
	public CompletableFuture<TaskManagerInfo> requestTaskManagerInfo(ResourceID resourceId, Time timeout) {

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(resourceId);

		if (taskExecutor == null) {
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(resourceId));
		} else {
			final InstanceID instanceId = taskExecutor.getInstanceID();
			String host = taskExecutor.getTaskExecutorGateway().getHostname();
			String webShell = getTaskManagerWebShell(resourceId, host);
			String tmLog = getTaskManagerLogUrl(resourceId, host);
			log.debug("webShell = {}, tmLog = {}", webShell, tmLog);

			final TaskManagerInfo taskManagerInfo = new TaskManagerInfo(
				resourceId,
				taskExecutor.getTaskExecutorGateway().getAddress(),
				taskExecutor.getDataPort(),
				taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
				slotManager.getNumberRegisteredSlotsOf(instanceId),
				slotManager.getNumberFreeSlotsOf(instanceId),
				slotManager.getRegisteredResourceOf(instanceId),
				slotManager.getFreeResourceOf(instanceId),
				taskExecutor.getHardwareDescription(),
				taskExecutor.getMemoryConfiguration(),
				webShell,
				tmLog);

			return CompletableFuture.completedFuture(taskManagerInfo);
		}
	}

	@Override
	public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
		final int numberSlots = slotManager.getNumberRegisteredSlots();
		final int numberFreeSlots = slotManager.getNumberFreeSlots();
		final ResourceProfile totalResource = slotManager.getRegisteredResource();
		final ResourceProfile freeResource = slotManager.getFreeResource();

		return CompletableFuture.completedFuture(
			new ResourceOverview(
				taskExecutors.size(),
				numberSlots,
				numberFreeSlots,
				totalResource,
				freeResource,
				waitingFatal,
				fatalMessage));
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
		final ArrayList<CompletableFuture<Optional<Tuple2<ResourceID, String>>>> metricQueryServiceAddressFutures = new ArrayList<>(taskExecutors.size());

		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry : taskExecutors.entrySet()) {
			final ResourceID tmResourceId = workerRegistrationEntry.getKey();
			final WorkerRegistration<WorkerType> workerRegistration = workerRegistrationEntry.getValue();
			final TaskExecutorGateway taskExecutorGateway = workerRegistration.getTaskExecutorGateway();

			final CompletableFuture<Optional<Tuple2<ResourceID, String>>> metricQueryServiceAddressFuture = taskExecutorGateway
				.requestMetricQueryServiceAddress(timeout)
				.thenApply(o -> o.toOptional().map(address -> Tuple2.of(tmResourceId, address)));

			metricQueryServiceAddressFutures.add(metricQueryServiceAddressFuture);
		}

		return FutureUtils.combineAll(metricQueryServiceAddressFutures).thenApply(
			collection -> collection
				.stream()
				.filter(Optional::isPresent)
				.map(Optional::get)
				.collect(Collectors.toList()));
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(ResourceID taskManagerId, FileType fileType, Time timeout) {
		log.debug("Request {} file upload from TaskExecutor {}.", fileType, taskManagerId);

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

		if (taskExecutor == null) {
			log.debug("Request upload of file {} from unregistered TaskExecutor {}.", fileType, taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestFileUploadByType(fileType, timeout);
		}
	}

	@Override
	public CompletableFuture<PreviewDataResponse> requestTaskManagerPreviewData(ResourceID taskManagerId, JobID jobId, JobVertexID jobVertexId, Time timeout) {
		log.debug("Request preview data of job {} , jobVertexId {} , from TaskExecutor {}", jobId, jobVertexId, taskManagerId);
		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);
		if (taskExecutor == null) {
			log.warn("Request preview data of job {} from unregistered TaskExecutor {}.", jobId, taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestTaskManagerPreviewData(jobId, jobVertexId, timeout);
		}
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByName(ResourceID taskManagerId, String fileName, Time timeout) {
		log.debug("Request upload of file {} from TaskExecutor {}.", fileName, taskManagerId);

		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

		if (taskExecutor == null) {
			log.debug("Request upload of file {} from unregistered TaskExecutor {}.", fileName, taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestFileUploadByName(fileName, timeout);
		}
	}

	@Override
	public CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(ResourceID taskManagerId, Time timeout) {
		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);
		if (taskExecutor == null) {
			log.debug("Requested log list from unregistered TaskExecutor {}.", taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestLogList(timeout);
		}
	}

	@Override
	public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
		return clusterPartitionTracker.releaseClusterPartitions(dataSetId);
	}

	@Override
	public CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets() {
		return CompletableFuture.completedFuture(clusterPartitionTracker.listDataSets());
	}

	@Override
	public CompletableFuture<ThreadDumpInfo> requestThreadDump(ResourceID taskManagerId, Time timeout) {
		final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

		if (taskExecutor == null) {
			log.debug("Requested thread dump from unregistered TaskExecutor {}.", taskManagerId);
			return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(taskManagerId));
		} else {
			return taskExecutor.getTaskExecutorGateway().requestThreadDump(timeout);
		}

	}

	@Override
	public void clearBlacklist(JobID jobID, JobMasterId jobMasterId, Time timeout) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobID);
		if (jobManagerRegistration != null) {
			if (jobManagerRegistration.getJobMasterId().equals(jobMasterId)) {
				blacklistTracker.clearAll();
			} else {
				log.warn("The job leader's id {} does not match the received id {}.",
						jobManagerRegistration.getJobMasterId(), jobMasterId);
			}
		} else {
			log.warn("Could not find registered job manager for job {}.", jobID);
		}
	}

	@Override
	public void onTaskFailure(
			JobID jobID,
			JobMasterId jobMasterId,
			BlacklistUtil.FailureType failureType,
			String hostname,
			ResourceID taskManagerId,
			Throwable cause,
			long timestamp,
			Time timeout) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobID);
		if (jobManagerRegistration != null) {
			if (jobManagerRegistration.getJobMasterId().equals(jobMasterId)) {
				final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

				if (taskExecutor == null) {
					log.debug("Blacklist will not update, because taskExecutor {} not exists.", taskManagerId);
				} else {
					blacklistTracker.onFailure(
							failureType,
							taskExecutor.getTaskExecutorGateway().getHostname(),
							taskManagerId,
							cause,
							timestamp);
				}
			} else {
				log.warn("The job leader's id {} does not match the received id {}.",
						jobManagerRegistration.getJobMasterId(), jobMasterId);
			}
		} else {
			log.warn("Could not find registered job manager for job {}.", jobID);
		}
	}

	@Override
	public void addIgnoreExceptionClass(JobID jobID, JobMasterId jobMasterId, Class<? extends Throwable> exceptionClass) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobID);
		if (jobManagerRegistration != null) {
			if (jobManagerRegistration.getJobMasterId().equals(jobMasterId)) {
				blacklistTracker.addIgnoreExceptionClass(exceptionClass);
			} else {
				log.warn("The job leader's id {} does not match the received id {}.",
						jobManagerRegistration.getJobMasterId(), jobMasterId);
			}
		} else {
			log.warn("Could not find registered job manager for job {}.", jobID);
		}
	}

	public Map<ResourceID, WorkerRegistration<WorkerType>> getTaskExecutors() {
		return taskExecutors;
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	/**
	 * Registers a new JobMaster.
	 *
	 * @param jobMasterGateway to communicate with the registering JobMaster
	 * @param jobId of the job for which the JobMaster is responsible
	 * @param jobManagerAddress address of the JobMaster
	 * @param jobManagerResourceId ResourceID of the JobMaster
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerJobMasterInternal(
		final JobMasterGateway jobMasterGateway,
		JobID jobId,
		JobInfo jobInfo,
		String jobManagerAddress,
		ResourceID jobManagerResourceId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
				// same registration
				log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
			} else {
				// tell old job manager that he is no longer the job leader
				closeJobManagerConnection(
					oldJobManagerRegistration.getJobID(),
					new Exception("New job leader for job " + jobId + " found."));

				JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
					jobId,
					jobManagerResourceId,
					jobMasterGateway,
					jobInfo);
				jobManagerRegistrations.put(jobId, jobManagerRegistration);
				jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
			}
		} else {
			// new registration for the job
			JobManagerRegistration jobManagerRegistration = new JobManagerRegistration(
				jobId,
				jobManagerResourceId,
				jobMasterGateway,
				jobInfo);
			jobManagerRegistrations.put(jobId, jobManagerRegistration);
			jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
			slotManager.initializeJobResources(jobId, jobManagerRegistration.getJobInfo());
			failureRater.onRequiredSlotNumChanged(jobInfo.getMinSlotsNum());
		}

		log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

		jobManagerHeartbeatManager.monitorTarget(jobManagerResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
				// the ResourceManager will always send heartbeat requests to the JobManager
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				jobMasterGateway.heartbeatFromResourceManager(resourceID);
			}
		});

		return new JobMasterRegistrationSuccess(
			getFencingToken(),
			resourceId);
	}

	/**
	 * Registers a new TaskExecutor.
	 *
	 * @param taskExecutorRegistration task executor registration parameters
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerTaskExecutorInternal(
			TaskExecutorGateway taskExecutorGateway,
			TaskExecutorRegistration taskExecutorRegistration) {
		ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();
		WorkerRegistration<WorkerType> oldRegistration = taskExecutors.remove(taskExecutorResourceId);
		if (oldRegistration != null) {
			// TODO :: suggest old taskExecutor to stop itself
			log.debug("Replacing old registration of TaskExecutor {}.", taskExecutorResourceId);

			// remove old task manager registration from slot manager
			slotManager.unregisterTaskManager(
				oldRegistration.getInstanceID(),
				new ResourceManagerException(String.format("TaskExecutor %s re-connected to the ResourceManager.", taskExecutorResourceId)));
		}

		final WorkerType newWorker = workerStarted(taskExecutorResourceId);

		String taskExecutorAddress = taskExecutorRegistration.getTaskExecutorAddress();
		if (newWorker == null) {
			log.warn("Discard registration from TaskExecutor {} at ({}) because the framework did " +
				"not recognize it", taskExecutorResourceId, taskExecutorAddress);
			return new RegistrationResponse.Decline("unrecognized TaskExecutor");
		} else {
			UnresolvedTaskManagerLocation unresolvedTaskManagerLocation = new UnresolvedTaskManagerLocation(
					taskExecutorResourceId,
					taskExecutorRegistration.getExternalAddress(),
					taskExecutorRegistration.getDataPort());
			TaskManagerAddressLocation taskManagerAddressLocation = new TaskManagerAddressLocation(
					taskExecutorRegistration.getTaskExecutorAddress(),
					unresolvedTaskManagerLocation);
			WorkerRegistration<WorkerType> registration = new WorkerRegistration<>(
				taskExecutorGateway,
				newWorker,
				taskExecutorRegistration.getDataPort(),
				taskExecutorRegistration.getHardwareDescription(),
				taskExecutorRegistration.getMemoryConfiguration(),
				taskManagerAddressLocation);

			log.info("Registering TaskManager with ResourceID {} ({}) at ResourceManager", taskExecutorResourceId, taskExecutorAddress);
			taskExecutors.put(taskExecutorResourceId, registration);

			taskManagerHeartbeatManager.monitorTarget(taskExecutorResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					// the ResourceManager will always send heartbeat requests to the
					// TaskManager
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					taskExecutorGateway.heartbeatFromResourceManager(resourceID);
				}
			});

			if (jmResourceAllocationEnabled) {
				UnresolvedTaskManagerTopology oldUnresolvedTaskManagerTopology = taskExecutorTopology.get(taskExecutorResourceId);
				if (oldUnresolvedTaskManagerTopology == null) {
					taskExecutorTopology.put(
						taskExecutorResourceId,
						new UnresolvedTaskManagerTopology(
							taskExecutorGateway,
							unresolvedTaskManagerLocation,
							taskExecutorRegistration.getSocketAddress()));

					if (log.isDebugEnabled()) {
						log.debug("Notify Dispatchers [{}] for latest TM topology because {} added",
							dispatcherRegistrations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")),
							taskExecutorResourceId);
					}

					dispatcherRegistrations.values().forEach(
						dispatcherRegistration -> dispatcherRegistration
							.getDispatcherGateway()
							.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout)
					);
				}
			}

			return new TaskExecutorRegistrationSuccess(
				registration.getInstanceID(),
				resourceId,
				clusterInformation);
		}
	}

	/**
	 * Register a new dispatcher.
	 *
	 * @param dispatcherGateway to communicate with the registering Dispatcher
	 * @param dispatcherAddress address of the Dispatcher
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerDispatcherInternal(
			DispatcherGateway dispatcherGateway,
			ResourceID dispatcherResourceId,
			String dispatcherAddress) {
		if (dispatcherRegistrations.containsKey(dispatcherResourceId)) {
			DispatcherRegistration oldDispatcherRegistration = dispatcherRegistrations.get(dispatcherResourceId);

			if (Objects.equals(oldDispatcherRegistration.getDispatcherId(), dispatcherGateway.getFencingToken())) {
				// same registration
				log.debug("Dispatcher {}@{} was already registered.", dispatcherGateway.getFencingToken(), dispatcherAddress);
			} else {
				// tell old dispatcher that he is no longer the leader
				closeDispatcherConnection(
					dispatcherResourceId,
					new Exception("New dispatcher " + dispatcherResourceId + " found."));

				DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
					dispatcherGateway.getFencingToken(),
					dispatcherResourceId,
					dispatcherGateway);
				dispatcherRegistrations.put(dispatcherResourceId, dispatcherRegistration);
			}
		} else {
			DispatcherRegistration dispatcherRegistration = new DispatcherRegistration(
				dispatcherGateway.getFencingToken(),
				dispatcherResourceId,
				dispatcherGateway);
			dispatcherRegistrations.put(dispatcherResourceId, dispatcherRegistration);
		}
		log.info("Registered dispatcher {}@{} at ResourceManager.", dispatcherGateway.getFencingToken(), dispatcherAddress);

		if (log.isDebugEnabled()) {
			log.debug("Notify Dispatcher {} for latest TM topology", dispatcherResourceId);
		}

		dispatcherGateway.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout);

		dispatcherHeartbeatManager.monitorTarget(dispatcherResourceId, new HeartbeatTarget<Void>() {
			@Override
			public void receiveHeartbeat(ResourceID resourceID, Void payload) {
				// the ResourceManager will always send heartbeat requests to the Dispatcher
			}

			@Override
			public void requestHeartbeat(ResourceID resourceID, Void payload) {
				dispatcherGateway.heartbeatFromResourceManager(resourceID);
			}
		});

		return new DispatcherRegistrationSuccess(
			getFencingToken(),
			resourceId);
	}

	private void registerJobManagerMetrics(){
		resourceManagerMetricGroup.counter(
			MetricNames.NUM_RM_HEARTBEAT_TIMOUT_FROM_JM,
			heartbeatTimeoutWithJM);
	}

	private void registerTaskExecutorMetrics() {
		resourceManagerMetricGroup.gauge(
			MetricNames.NUM_REGISTERED_TASK_MANAGERS,
			() -> (long) taskExecutors.size());
		resourceManagerMetricGroup.meter(
			MetricNames.WORKER_FAILURE_RATE,
			failureRater);
		resourceManagerMetricGroup.counter(
			MetricNames.NUM_RM_HEARTBEAT_TIMOUT_FROM_TM,
			heartbeatTimeoutWithTM);
	}

	private void clearStateInternal() {
		jobManagerRegistrations.clear();
		jmResourceIdRegistrations.clear();
		taskExecutors.clear();
		dispatcherRegistrations.clear();

		try {
			jobLeaderIdService.clear();
		} catch (Exception e) {
			onFatalError(new ResourceManagerException("Could not properly clear the job leader id service.", e));
		}
		clearStateFuture = clearStateAsync();
	}

	protected boolean tryStartNewWorker(WorkerResourceSpec workerResourceSpec) {
		if (failureRater.exceedsFailureRate()) {
			log.warn("tryStartNewWorker failed because failure rate exceeds limit. current failure rate is {}",
				failureRater.getCurrentFailureRate());
			return false;
		}
		return startNewWorker(workerResourceSpec);
	}

	/**
	 * Record failure number of worker in ResourceManagers. If maximum failure rate is detected,
	 * then cancel all pending requests.
	 */
	@VisibleForTesting
	protected void recordWorkerFailure() {
		failureRater.markFailure(SystemClock.getInstance());
		if (failureRater.exceedsFailureRate()) {
			cancelAllPendingSlotRequests(new MaximumFailedTaskManagerExceedingException(
				new RuntimeException(String.format("Maximum number of failed workers %f"
					+ " is detected in Resource Manager", failureRater.getCurrentFailureRate()))));

		}
	}

	protected void recordWorkerFailure(String hostname, ResourceID resourceID, Throwable cause) {
		recordWorkerFailure();
		try {
			blacklistReporter.onFailure(hostname, resourceID, cause, System.currentTimeMillis());
		} catch (Exception e) {
			log.warn("Report failure to blacklist error.", e);
		}
	}

	private void cancelAllPendingSlotRequests(Exception cause) {
		slotManager.cancelAllPendingSlotRequests(cause);
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * job manager has failed.
	 *
	 * @param jobId identifying the job whose leader shall be disconnected.
	 * @param cause The exception which cause the JobManager failed.
	 */
	protected void closeJobManagerConnection(JobID jobId, Exception cause) {
		JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);

		if (jobManagerRegistration != null) {
			final ResourceID jobManagerResourceId = jobManagerRegistration.getJobManagerResourceID();
			final JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();
			final JobMasterId jobMasterId = jobManagerRegistration.getJobMasterId();

			log.info("Disconnect job manager {}@{} for job {} from the resource manager.",
				jobMasterId,
				jobMasterGateway.getAddress(),
				jobId);

			jobManagerHeartbeatManager.unmonitorTarget(jobManagerResourceId);
			failureRater.onRequiredSlotNumChanged(-jobManagerRegistration.getJobInfo().getMinSlotsNum());
			jmResourceIdRegistrations.remove(jobManagerResourceId);
			slotManager.unregisterJobMaster(jobManagerRegistration.getJobInfo());

			// tell the job manager about the disconnect
			jobMasterGateway.disconnectResourceManager(getFencingToken(), cause);
		} else {
			log.debug("There was no registered job manager for job {}.", jobId);
		}
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * dispatcher has failed.
	 *
	 * @param dispatcherId identifying the dispatcher leader shall be disconnected.
	 * @param cause The exception which cause the JobManager failed.
	 */
	protected void closeDispatcherConnection(ResourceID dispatcherId, Exception cause) {
		DispatcherRegistration dispatcherRegistration = dispatcherRegistrations.remove(dispatcherId);

		if (dispatcherRegistration != null) {
			final DispatcherGateway dispatcherGateway = dispatcherRegistration.getDispatcherGateway();
			log.info("Disconnect dispatcher {}@{}", dispatcherId, dispatcherGateway.getAddress());

			dispatcherHeartbeatManager.unmonitorTarget(dispatcherRegistration.getDispatcherResourceID());

			// tell the dispatcher about the disconnect
			dispatcherGateway.disconnectResourceManager(getFencingToken(), cause);
		} else {
			log.debug("There was no registered dispatcher {}.", dispatcherId);
		}
	}

	protected int getMinNumberOfTaskManagerForPodGroup(){
		if (jobManagerRegistrations.size() != 1) {
			log.error("Multiple job master found! This method might not be invoked in application mode");
			// return 0 as default minimum number of task manager pod in pod group annotation
			return 0;
		}
		return slotManager.numTaskManagersNeedRequest();
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * task executor has failed.
	 *
	 * @param resourceID Id of the TaskManager that has failed.
	 * @param cause The exception which cause the TaskManager failed.
	 */
	protected void closeTaskManagerConnection(final ResourceID resourceID, final Exception cause, final int exitCode) {
		taskManagerHeartbeatManager.unmonitorTarget(resourceID);

		WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

		if (workerRegistration != null) {
			log.warn("Closing TaskExecutor connection {} with exit code {} because: {}", LoggerHelper.secMark("resourceID", resourceID), LoggerHelper.secMark("exitCode", exitCode), LoggerHelper.secMark("errMsg", cause.getMessage()));

			// TODO :: suggest failed task executor to stop itself
			slotManager.unregisterTaskManager(workerRegistration.getInstanceID(), cause);
			clusterPartitionTracker.processTaskExecutorShutdown(resourceID);

			if (jmResourceAllocationEnabled) {
				taskExecutorTopology.remove(resourceID);

				if (log.isDebugEnabled()) {
					log.debug("Notify Dispatchers [{}] for latest TM topology because {} closed",
						dispatcherRegistrations.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")),
						resourceID);
				}

				dispatcherRegistrations.values().forEach(
					dispatcherRegistration -> dispatcherRegistration
						.getDispatcherGateway()
						.offerTaskManagers(taskExecutorTopology.values(), rpcTimeout)
				);
			}

			workerRegistration.getTaskExecutorGateway().disconnectResourceManager(cause);
		} else {
			log.debug(
				"No open TaskExecutor connection {}. Ignoring close TaskExecutor connection. Closing exit code was: {} reason was: {}",
				resourceID,
				exitCode,
				cause.getMessage());
		}
	}

	protected void removeJob(JobID jobId) {
		try {
			jobLeaderIdService.removeJob(jobId);
		} catch (Exception e) {
			log.warn("Could not properly remove the job {} from the job leader id service.", jobId, e);
		}

		if (jobManagerRegistrations.containsKey(jobId)) {
			closeJobManagerConnection(jobId, new Exception("Job " + jobId + "was removed"));
		}
	}

	protected void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {
		if (jobManagerRegistrations.containsKey(jobId)) {
			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

			if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {
				closeJobManagerConnection(jobId, new Exception("Job leader lost leadership."));
			} else {
				log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);
			}
		} else {
			log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobMasterId, jobId);
		}
	}

	protected void releaseResource(InstanceID instanceId, Exception cause, int exitCode) {
		WorkerType worker = null;

		// TODO: Improve performance by having an index on the instanceId
		for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> entry : taskExecutors.entrySet()) {
			if (entry.getValue().getInstanceID().equals(instanceId)) {
				worker = entry.getValue().getWorker();
				break;
			}
		}

		if (worker != null) {
			if (stopWorker(worker, exitCode)) {
				closeTaskManagerConnection(worker.getResourceID(), cause, exitCode);
			} else {
				log.debug("Worker {} could not be stopped.", worker.getResourceID());
			}
		} else {
			// unregister in order to clean up potential left over state
			slotManager.unregisterTaskManager(instanceId, cause);
		}
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 *
	 * @param t The exception describing the fatal error
	 */
	@Override
	public void onFatalError(Throwable t) {
		try {
			log.error("Fatal error occurred in ResourceManager.", t);
		} catch (Throwable ignored) {}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	protected void onFatalError(String fatalMessage, int waitTime) {
		this.fatalMessage = fatalMessage;
		this.waitingFatal = true;
		try {
			deregisterApplication(ApplicationStatus.FAILED, fatalMessage).get(waitTime, TimeUnit.MILLISECONDS);
		} catch (Exception ignored) {}

		onFatalError(new Exception(fatalMessage));
	}

	// ------------------------------------------------------------------------
	//  Leader Contender
	// ------------------------------------------------------------------------

	/**
	 * Callback method when current resourceManager is granted leadership.
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {
		final CompletableFuture<Boolean> acceptLeadershipFuture = clearStateFuture
			.thenComposeAsync((ignored) -> tryAcceptLeadership(newLeaderSessionID), getUnfencedMainThreadExecutor());

		final CompletableFuture<Void> confirmationFuture = acceptLeadershipFuture.thenAcceptAsync(
			(acceptLeadership) -> {
				if (acceptLeadership) {
					// confirming the leader session ID might be blocking,
					leaderElectionService.confirmLeadership(newLeaderSessionID, getAddress());
				}
			},
			getRpcService().getExecutor());

		confirmationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					onFatalError(ExceptionUtils.stripCompletionException(throwable));
				}
			});
	}

	private CompletableFuture<Boolean> tryAcceptLeadership(final UUID newLeaderSessionID) {
		if (leaderElectionService.hasLeadership(newLeaderSessionID)) {
			final ResourceManagerId newResourceManagerId = ResourceManagerId.fromUuid(newLeaderSessionID);

			log.info("ResourceManager {} was granted leadership with fencing token {}", getAddress(), newResourceManagerId);

			// clear the state if we've been the leader before
			if (getFencingToken() != null) {
				clearStateInternal();
			}

			setFencingToken(newResourceManagerId);

			startServicesOnLeadership();

			return prepareLeadershipAsync().thenApply(ignored -> true);
		} else {
			return CompletableFuture.completedFuture(false);
		}
	}

	protected void startServicesOnLeadership() {
		startHeartbeatServices();

		blacklistTracker.start(getMainThreadExecutor(), new BlacklistActionsImpl());
		slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());
	}

	/**
	 * Callback method when current resourceManager loses leadership.
	 */
	@Override
	public void revokeLeadership() {
		runAsyncWithoutFencing(
			() -> {
				log.info("ResourceManager {} was revoked leadership. Clearing fencing token.", getAddress());

				clearStateInternal();

				setFencingToken(null);

				slotManager.suspend();

				blacklistTracker.clearAll();

				stopHeartbeatServices();

			});
	}

	private void startHeartbeatServices() {
		taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);

		jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new JobManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);

		if (jmResourceAllocationEnabled) {
			dispatcherHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
				resourceId,
				new DispatcherHeartbeatListener(),
				getMainThreadExecutor(),
				log);
		}
	}

	private void stopHeartbeatServices() {
			taskManagerHeartbeatManager.stop();
			jobManagerHeartbeatManager.stop();
			if (jmResourceAllocationEnabled) {
				dispatcherHeartbeatManager.stop();
			}
	}

	/**
	 * Handles error occurring in the leader election service.
	 *
	 * @param exception Exception being thrown in the leader election service
	 */
	@Override
	public void handleError(final Exception exception) {
		onFatalError(new ResourceManagerException("Received an error from the LeaderElectionService.", exception));
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws ResourceManagerException which occurs during initialization and causes the resource manager to fail.
	 */
	protected abstract void initialize() throws ResourceManagerException;

	/**
	 * This method can be overridden to add a (non-blocking) initialization routine to the
	 * ResourceManager that will be called when leadership is granted but before leadership is
	 * confirmed.
	 *
	 * @return Returns a {@code CompletableFuture} that completes when the computation is finished.
	 */
	protected CompletableFuture<Void> prepareLeadershipAsync() {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * This method can be overridden to add a (non-blocking) state clearing routine to the
	 * ResourceManager that will be called when leadership is revoked.
	 *
	 * @return Returns a {@code CompletableFuture} that completes when the state clearing routine
	 * is finished.
	 */
	protected CompletableFuture<Void> clearStateAsync() {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * The framework specific code to deregister the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * <p>This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics A diagnostics message or {@code null}.
	 * @throws ResourceManagerException if the application could not be shut down.
	 */
	protected abstract void internalDeregisterApplication(
		ApplicationStatus finalStatus,
		@Nullable String optionalDiagnostics) throws ResourceManagerException;

	/**
	 * Allocates a resource using the worker resource specification.
	 *
	 * @param workerResourceSpec workerResourceSpec specifies the size of the to be allocated resource
	 * @return whether the resource can be allocated
	 */
	@VisibleForTesting
	public abstract boolean startNewWorker(WorkerResourceSpec workerResourceSpec);

	public boolean startNewWorkers(WorkerResourceSpec workerResourceSpec, int resourceNumber) {
		return false;
	}

	/**
	 * Callback when a worker was started.
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerStarted(ResourceID resourceID);

	/**
	 * Stops the given worker.
	 *
	 * @param worker The worker.
	 * @param exitCode The Container exitCode.
	 * @return True if the worker was stopped, otherwise false
	 */
	public boolean stopWorker(WorkerType worker, int exitCode) {
		return stopWorker(worker);
	}

	public abstract boolean stopWorker(WorkerType worker);

	/**
	 * Stop worker with ResourceID.
	 * @return True if the worker was stopped or not exist, otherwise false
	 */
	public abstract boolean stopWorker(ResourceID resourceID, int exitCode);

	public abstract boolean stopWorkerAndStartNewIfRequired(ResourceID resourceID, int exitCode);

	public void removePendingContainerRequest(WorkerResourceSpec workerResourceSpec, int expectedNum) { }

	/**
	 * Set {@link SlotManager} whether to fail unfulfillable slot requests.
	 * @param failUnfulfillableRequest whether to fail unfulfillable requests
	 */
	protected void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		slotManager.setFailUnfulfillableRequest(failUnfulfillableRequest);
	}

	public void onBlacklistUpdated() {
		log.info("Received onBlacklistUpdated, but nothing can do.");
	}

	// ------------------------------------------------------------------------
	//  Static utility classes
	// ------------------------------------------------------------------------

	private class ResourceActionsImpl implements ResourceActions {

		@Override
		public void releaseResource(InstanceID instanceId, Exception cause, int exitCode) {
			validateRunsInMainThread();

			ResourceManager.this.releaseResource(instanceId, cause, exitCode);
		}

		@Override
		public boolean allocateResource(WorkerResourceSpec workerResourceSpec) {
			validateRunsInMainThread();
			return tryStartNewWorker(workerResourceSpec);
		}

		@Override
		public boolean allocateResources(WorkerResourceSpec workerResourceSpec, int resourceNumber) {
			validateRunsInMainThread();
			return startNewWorkers(workerResourceSpec, resourceNumber);
		}

		@Override
		public void notifyAllocationFailure(JobID jobId, AllocationID allocationId, Exception cause) {
			validateRunsInMainThread();

			JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
			if (jobManagerRegistration != null) {
				jobManagerRegistration.getJobManagerGateway().notifyAllocationFailure(allocationId, cause);
			}
		}
	}

	private class BlacklistActionsImpl implements BlacklistActions {
		@Override
		public void notifyBlacklistUpdated() {
			validateRunsInMainThread();
			onBlacklistUpdated();
		}
	}

	private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

		@Override
		public void jobLeaderLostLeadership(final JobID jobId, final JobMasterId oldJobMasterId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
				}
			});
		}

		@Override
		public void notifyJobTimeout(final JobID jobId, final UUID timeoutId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {
						removeJob(jobId);
					}
				}
			});
		}

		@Override
		public void handleError(Throwable error) {
			onFatalError(error);
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<TaskExecutorHeartbeatPayload, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			validateRunsInMainThread();
			heartbeatTimeoutWithTM.inc();
			log.info("The heartbeat of TaskManager with id {} timed out.", resourceID);

			closeTaskManagerConnection(
				resourceID,
				new TimeoutException("The heartbeat of TaskManager with id " + resourceID + "  timed out."),
				WorkerExitCode.HEARTBEAT_TIMEOUT);
		}

		@Override
		public void reportPayload(final ResourceID resourceID, final TaskExecutorHeartbeatPayload payload) {
			validateRunsInMainThread();
			final WorkerRegistration<WorkerType> workerRegistration = taskExecutors.get(resourceID);

			if (workerRegistration == null) {
				log.debug("Received slot report from TaskManager {} which is no longer registered.", resourceID);
			} else {
				InstanceID instanceId = workerRegistration.getInstanceID();

				slotManager.reportSlotStatus(instanceId, payload.getSlotReport());
				clusterPartitionTracker.processTaskExecutorClusterPartitionReport(resourceID, payload.getClusterPartitionReport());
			}
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}

	private class JobManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			validateRunsInMainThread();
			log.info("The heartbeat of JobManager with id {} timed out.", resourceID);
			heartbeatTimeoutWithJM.inc();
			if (jmResourceIdRegistrations.containsKey(resourceID)) {
				JobManagerRegistration jobManagerRegistration = jmResourceIdRegistrations.get(resourceID);

				if (jobManagerRegistration != null) {
					closeJobManagerConnection(
						jobManagerRegistration.getJobID(),
						new TimeoutException("The heartbeat of JobManager with id " + resourceID + " timed out."));
				}
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}

	private class DispatcherHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceID) {
			validateRunsInMainThread();
			log.info("The heartbeat of Dispatcher with id {} timed out.", resourceID);
			if (dispatcherRegistrations.containsKey(resourceID)) {
				DispatcherRegistration dispatcherRegistration = dispatcherRegistrations.get(resourceID);

				if (dispatcherRegistration != null) {
					closeDispatcherConnection(
						resourceID,
						new TimeoutException("The heartbeat of Dispatcher with id " + resourceID + " timed out."));
				}
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since there is no payload
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}

	// ------------------------------------------------------------------------
	//  Resource Management
	// ------------------------------------------------------------------------

	protected int getNumberRequiredTaskManagers() {
		return getRequiredResources().values().stream().reduce(0, Integer::sum);
	}

	protected Map<WorkerResourceSpec, Integer> getRequiredResources() {
		return slotManager.getRequiredResources();
	}

	protected WorkerResourceSpec getDefaultWorkerResourceSpec() {
		return slotManager.getDefaultWorkerResourceSpec();
	}

	/**
	 * Compute the least worker count for job.
	 * @param requestSlotCount job request slot count
	 * @param taskCount job task count
	 * @param maxTasksPerWorker max tasks per worker
	 * @param minWorkersPerJob min workers per job
	 * @param totalWorkerCount total worker count
	 * @return the least worker count for job
	 */
	@VisibleForTesting
	public static int computeJobWorkerCount(
			int requestSlotCount,
			int taskCount,
			int maxTasksPerWorker,
			int minWorkersPerJob,
			int totalWorkerCount) {
		return Math.min(
					Math.min(
						Math.max(taskCount / maxTasksPerWorker, minWorkersPerJob),
						requestSlotCount),
					totalWorkerCount);
	}

	// ------------------------------------------------------------------------
	//  Testing methods
	// ------------------------------------------------------------------------

	@VisibleForTesting
	protected void setJmResourceAllocationEnabled(boolean jmResourceAllocationEnabled) {
		this.jmResourceAllocationEnabled = jmResourceAllocationEnabled;
	}
}

