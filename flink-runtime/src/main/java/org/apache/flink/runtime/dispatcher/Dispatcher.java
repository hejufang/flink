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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BenchmarkOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.event.WarehouseJobStartEventMessageRecorder;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.SimpleHistogram;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.history.ApplicationModeClusterInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.ResourceManagerAddress;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.ClusterResourceOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceOverview;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.socket.result.JobChannelManager;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.taskexecutor.DispatcherRegistration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends PermanentlyFencedRpcEndpoint<DispatcherId> implements DispatcherGateway {

	public static final String DISPATCHER_NAME = "dispatcher";
	private static final String EVENT_METRIC_NAME = "jobmanagerEvent";

	private final Configuration configuration;

	private ResourceID resourceId;

	private final JobGraphWriter jobGraphWriter;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final JobManagerSharedServices jobManagerSharedServices;
	private final HeartbeatServices heartbeatServices;
	private final BlobServer blobServer;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, CompletableFuture<JobManagerRunner>> jobManagerRunnerFutures;

	private final Map<JobID, CompletableFuture<Acknowledge>> pendingJobFutures;

	private final DispatcherBootstrap dispatcherBootstrap;

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final JobManagerRunnerFactory jobManagerRunnerFactory;

	private final JobManagerMetricGroup jobManagerMetricGroup;

	private final HistoryServerArchivist historyServerArchivist;

	@Nullable
	private final String metricServiceQueryAddress;

	private final Map<JobID, CompletableFuture<Void>> jobManagerTerminationFutures;

	private final Map<JobID, CompletableFuture<Acknowledge>> archiveExecutionGraphFutures;

	protected final CompletableFuture<ApplicationStatus> shutDownFuture;

	// just effective in application mode
	protected boolean applicationModeClusterInfoArchiveEnable;

	private final Counter submittedJobCounter = new SimpleCounter();
	private final Counter failedJobCounter = new SimpleCounter();
	private final Counter canceledJobCounter = new SimpleCounter();
	private final Counter finishedJobCounter = new SimpleCounter();
	private final Counter slowJobsCounter = new SimpleCounter();
	private final Counter rejectedJobCounter = new SimpleCounter();

	// Only record the duration from ExecutionGraph Create to job Finished.
	private final Histogram jobDurationHistogram = new SimpleHistogram(SimpleHistogram.buildSlidingTimeWindowReservoirHistogram());

	// Record the failed job's latency.
	private final Histogram failedJobDurationHistogram = new SimpleHistogram(SimpleHistogram.buildSlidingTimeWindowReservoirHistogram());

	private final Histogram jobToScheduleLatencyHistogram = new SimpleHistogram(SimpleHistogram.buildSlidingTimeWindowReservoirHistogram());

	// --------- ResourceManager --------
	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private DispatcherToResourceManagerConnection resourceManagerConnection;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	private final Boolean jmResourceAllocationEnabled;

	private final Map<ResourceID, ResolvedTaskManagerTopology> registeredTaskManagers;

	private int minRequiredTaskManager;

	private final int maxRegisteredTaskManager;

	private final int maxRunningTasks;

	private int totalRunningTasks;

	private final Queue<JobGraph> pendingJobs;

	private final Queue<Tuple2<JobGraph, ChannelHandlerContext>> pendingJobsWithContext;

	private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	private final boolean useAddressAsHostname;

	private final JobResultClientManager jobResultClientManager;

	private final boolean useSocketEnable;

	private final boolean jobReuseDispatcherEnable;

	private final boolean dispatcherFetchResultThreadPoolEnabled;

	private final ExecutorService fetchResultExecutor;

	private final WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder;

	private final boolean recordSlowQuery;

	private final long slowQueryLatencyThreshold;

	private final int maxRunningJobs;

	private final AtomicInteger totalRunningJobs = new AtomicInteger(0);

	public Dispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			DispatcherServices dispatcherServices) throws Exception {
		super(rpcService, AkkaRpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
		checkNotNull(dispatcherServices);

		this.configuration = dispatcherServices.getConfiguration();
		this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
		this.resourceManagerGatewayRetriever = dispatcherServices.getResourceManagerGatewayRetriever();
		this.heartbeatServices = dispatcherServices.getHeartbeatServices();
		this.blobServer = dispatcherServices.getBlobServer();
		this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
		this.jobGraphWriter = dispatcherServices.getJobGraphWriter();
		this.jobManagerMetricGroup = dispatcherServices.getJobManagerMetricGroup();
		this.metricServiceQueryAddress = dispatcherServices.getMetricQueryServiceAddress();
		this.jobResultClientManager = dispatcherServices.getJobResultClientManager();

		this.jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(
			configuration,
			blobServer,
			fatalErrorHandler,
			jobResultClientManager);

		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunnerFutures = new HashMap<>(16);

		this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

		this.archivedExecutionGraphStore = dispatcherServices.getArchivedExecutionGraphStore();

		this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();

		this.jobManagerTerminationFutures = new HashMap<>(2);

		this.pendingJobFutures = new HashMap<>(16);

		this.archiveExecutionGraphFutures = new HashMap<>(1);

		this.applicationModeClusterInfoArchiveEnable = this.configuration.getBoolean(JobManagerOptions.APPLICATION_MODE_CLUSTER_INFO_ARCHIVE_ENABLE);

		this.shutDownFuture = new CompletableFuture<>();

		this.dispatcherBootstrap = checkNotNull(dispatcherBootstrap);

		this.resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		this.resourceManagerAddress = null;

		this.resourceManagerConnection = null;

		this.establishedResourceManagerConnection = null;

		this.jmResourceAllocationEnabled = configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED);

		this.minRequiredTaskManager = configuration.getInteger(ClusterOptions.RM_MIN_WORKER_NUM);

		this.maxRegisteredTaskManager = configuration.getInteger(ClusterOptions.RM_MAX_WORKER_NUM);
		checkArgument(minRequiredTaskManager <= maxRegisteredTaskManager,
			"minimal required workers should be smaller or equal to maximum required workers.");

		this.maxRunningTasks = configuration.getInteger(JobManagerOptions.JOBMANAGER_MAX_RUNNING_TASKS_PER_TASKMANAGER);

		this.registeredTaskManagers = new HashMap<>();

		this.totalRunningTasks = 0;

		this.pendingJobs = new ArrayDeque<>();

		this.pendingJobsWithContext = new ArrayDeque<>();

		this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

		this.resourceId = ResourceID.generate();

		this.useAddressAsHostname = configuration.getBoolean(CoreOptions.USE_ADDRESS_AS_HOSTNAME_ENABLE);

		this.useSocketEnable = configuration.get(ClusterOptions.CLUSTER_SOCKET_ENDPOINT_ENABLE);
		checkArgument(
			!useSocketEnable || configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED),
			"Socket enable flag can be turned on when `cluster.jm-resource-allocation.enabled` is true");
		this.jobReuseDispatcherEnable = configuration.get(ClusterOptions.JOB_REUSE_DISPATCHER_IN_TASKEXECUTOR_ENABLE);
		checkArgument(
			!useSocketEnable || configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED),
			"Job reuse dispatcher in task executor flag can be turned on when `cluster.jm-resource-allocation.enabled` is true");

		this.dispatcherFetchResultThreadPoolEnabled =
			configuration.getBoolean(JobManagerOptions.DISPATCHER_FETCH_RESULT_THREAD_POOL_ENABLED);

		this.fetchResultExecutor =
			this.dispatcherFetchResultThreadPoolEnabled ? Executors.newFixedThreadPool(
				configuration.getInteger(JobManagerOptions.DISPATCHER_FETCH_RESULT_THREADS_NUM),
				new ExecutorThreadFactory("dispatcher-fetch-result")) : null;
		this.warehouseJobStartEventMessageRecorder = new WarehouseJobStartEventMessageRecorder(true);

		this.recordSlowQuery = this.configuration.getBoolean(JobManagerOptions.JOBMANAGER_RECORD_SLOW_QUERY_ENABLED);
		this.slowQueryLatencyThreshold = this.configuration.get(JobManagerOptions.JOBMANAGER_SLOW_QUERY_LATENCY_THRESHOLD).toMillis();
		this.maxRunningJobs = configuration.getInteger(JobManagerOptions.JOBMANAGER_MAX_RUNNING_JOBS);
	}

	//------------------------------------------------------
	// Getters
	//------------------------------------------------------

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	private DispatcherId getDispatcherId() {
		return getFencingToken();
	}

	//------------------------------------------------------
	// Setters
	//------------------------------------------------------

	@VisibleForTesting
	protected void setResourceId(ResourceID resourceId) {
		this.resourceId = resourceId;
	}

	@VisibleForTesting
	protected void setMinRequiredTaskManager(int minRequiredTaskManager) {
		this.minRequiredTaskManager = minRequiredTaskManager;
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		try {
			startDispatcherServices();
		} catch (Exception e) {
			final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}

		dispatcherBootstrap.initialize(this, this.getRpcService().getScheduledExecutor());
	}

	private void startDispatcherServices() throws Exception {
		try {
			if (jmResourceAllocationEnabled) {
				resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
					resourceId,
					new ResourceManagerHeartbeatListener(),
					getMainThreadExecutor(),
					log);

				// start by connecting to the ResourceManager
				resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
			}
			registerDispatcherMetrics(jobManagerMetricGroup);
		} catch (Exception e) {
			handleStartDispatcherServicesException(e);
		}
	}

	void runRecoveredJob(final JobGraph recoveredJob) {
		checkNotNull(recoveredJob);
		FutureUtils.assertNoException(runJob(recoveredJob)
			.handle(handleRecoveredJobStartError(recoveredJob.getJobID())));
	}

	private BiFunction<Void, Throwable, Void> handleRecoveredJobStartError(JobID jobId) {
		return (ignored, throwable) -> {
			if (throwable != null) {
				onFatalError(new DispatcherException(String.format("Could not start recovered job %s.", jobId), throwable));
			}

			return null;
		};
	}

	private void handleStartDispatcherServicesException(Exception e) throws Exception {
		try {
			stopDispatcherServices();
		} catch (Exception exception) {
			e.addSuppressed(exception);
		}

		throw e;
	}

	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping dispatcher {}.", getAddress());

		// now just waiting the finish of archiving before stopping the cluster.
		final CompletableFuture<Void> allArchiveExecutionGraphAndGetResultFuture = archiveExecutionGraphAndGetResultFuture();

		final CompletableFuture<Void> allJobManagerRunnersTerminationFuture = terminateJobManagerRunnersAndGetTerminationFuture();

		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);

		terminationFutures.add(allArchiveExecutionGraphAndGetResultFuture);
		terminationFutures.add(allJobManagerRunnersTerminationFuture);

		if (jmResourceAllocationEnabled) {
			try {
				resourceManagerLeaderRetriever.stop();
			} catch (Exception e) {
				log.warn("Failed to stop resource manager leader retriever when stopping.", e);
			}

			resourceManagerHeartbeatManager.stop();
		}

		if (jobResultClientManager != null) {
			jobResultClientManager.close();
		}

		return FutureUtils.runAfterwards(
			FutureUtils.completeAll(terminationFutures),
			() -> {
				dispatcherBootstrap.stop();

				stopDispatcherServices();

				log.info("Stopped dispatcher {}.", getAddress());
			});
	}

	private void stopDispatcherServices() throws Exception {
		Exception exception = null;
		try {
			jobManagerSharedServices.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		jobManagerMetricGroup.close();

		ExceptionUtils.tryRethrowException(exception);
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		log.info("Received JobGraph submission {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		try {
			if (isDuplicateJob(jobGraph.getJobID())) {
				return FutureUtils.completedExceptionally(
					new DuplicateJobSubmissionException(jobGraph.getJobID()));
			} else {
				if (isClusterOverloaded(jobGraph.getJobID())) {
					return FutureUtils.completedExceptionally(
						new JobSubmissionException(jobGraph.getJobID(),
							String.format("Too many job submit requests to cluster. Maximum capacity: %d", maxRunningJobs)));
				} else {
					if (jmResourceAllocationEnabled) {
						if (canSubmitJob()) {
							return internalSubmitJob(jobGraph);
						} else {
							pendingJobs.add(jobGraph);
							pendingJobFutures.put(jobGraph.getJobID(), new CompletableFuture<>());
							log.info("Available taskmanagers: {} is less then {} or running tasks: {} is greater then {}." +
									"Put job {} ({}) to pending queue.",
								registeredTaskManagers.size(),
								minRequiredTaskManager,
								totalRunningTasks,
								maxRunningTasks,
								jobGraph.getJobID(),
								jobGraph.getName());

							if (registeredTaskManagers.size() < maxRegisteredTaskManager && establishedResourceManagerConnection != null) {
								int extraTaskManagers = (int) Math.ceil((double) minRequiredTaskManager * 0.1);
								establishedResourceManagerConnection
									.getResourceManagerGateway()
									.requestTaskManager(extraTaskManagers, timeout);
							}

							return CompletableFuture.completedFuture(Acknowledge.get());
						}
					} else {
						if (isPartialResourceConfigured(jobGraph)) {
							return FutureUtils.completedExceptionally(
								new JobSubmissionException(jobGraph.getJobID(), "Currently jobs is not supported if parts of the vertices have " +
									"resources configured. The limitation will be removed in future versions."));
						} else {
							return internalSubmitJob(jobGraph);
						}
					}
				}
			}
		} catch (FlinkException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public void submitJob(JobGraph jobGraph, ChannelHandlerContext ctx, Time timeout) {
		try {
			if (isDuplicateJob(jobGraph.getJobID())) {
				writeFinishJobToContext(
					jobGraph.getJobID(),
					ctx,
					JobStatus.FAILED,
					new DuplicateJobSubmissionException(jobGraph.getJobID()));
			} else if (isPartialResourceConfigured(jobGraph)) {
				writeFinishJobToContext(
					jobGraph.getJobID(),
					ctx,
					JobStatus.FAILED,
					new JobSubmissionException(jobGraph.getJobID(), "Currently jobs is not supported if parts of the vertices have " +
						"resources configured. The limitation will be removed in future versions."));
			} else {
				if (isClusterOverloaded(jobGraph.getJobID())) {
					writeFinishJobToContext(
						jobGraph.getJobID(),
						ctx,
						JobStatus.FAILED,
						new JobSubmissionException(jobGraph.getJobID(),
							String.format("Too many job submit requests to cluster. Maximum capacity: %d", maxRunningJobs)));
				} else {
					if (jmResourceAllocationEnabled) {
						if (canSubmitJob()) {
							internalSubmitJob(jobGraph, ctx);
						} else {
							pendingJobsWithContext.add(Tuple2.of(jobGraph, ctx));
							pendingJobFutures.put(jobGraph.getJobID(), new CompletableFuture<>());
							log.info("Available taskmanagers: {} is less then {} or running tasks: {} is greater then {}." +
									"Put job {} ({}) to pending queue.",
								registeredTaskManagers.size(),
								minRequiredTaskManager,
								totalRunningTasks,
								maxRunningTasks,
								jobGraph.getJobID(),
								jobGraph.getName());

							if (registeredTaskManagers.size() < maxRegisteredTaskManager && establishedResourceManagerConnection != null) {
								int extraTaskManagers = (int) Math.ceil((double) minRequiredTaskManager * 0.1);
								establishedResourceManagerConnection
									.getResourceManagerGateway()
									.requestTaskManager(extraTaskManagers, timeout);
							}
						}
					} else {
						internalSubmitJob(jobGraph, ctx);
					}
				}
			}
		} catch (FlinkException e) {
			writeFinishJobToContext(jobGraph.getJobID(), ctx, JobStatus.FAILED, e);
		}
	}

	private boolean isClusterOverloaded(JobID jobID) {
		int runningJobs = totalRunningJobs.get();
		if (maxRunningJobs > 0 && runningJobs >= maxRunningJobs) {
			log.info("[reject query] jobId: {}, current running jobs: {}, capacity: {}",
				jobID,
				runningJobs,
				maxRunningJobs);
			rejectedJobCounter.inc();

			return true;
		} else {
			return false;
		}
	}

	private boolean canSubmitJob() {
		return registeredTaskManagers.size() >= minRequiredTaskManager
			&& totalRunningTasks <= maxRunningTasks * registeredTaskManagers.size();
	}

	/**
	 * Checks whether the given job has already been submitted or executed.
	 *
	 * @param jobId identifying the submitted job
	 * @return true if the job has already been submitted (is running) or has been executed
	 * @throws FlinkException if the job scheduling status cannot be retrieved
	 */
	private boolean isDuplicateJob(JobID jobId) throws FlinkException {
		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			throw new FlinkException(String.format("Failed to retrieve job scheduling status for job %s.", jobId), e);
		}

		return jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || jobManagerRunnerFutures.containsKey(jobId);
	}

	private boolean isPartialResourceConfigured(JobGraph jobGraph) {
		boolean hasVerticesWithUnknownResource = false;
		boolean hasVerticesWithConfiguredResource = false;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
				hasVerticesWithUnknownResource = true;
			} else {
				hasVerticesWithConfiguredResource = true;
			}

			if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
				return true;
			}
		}

		return false;
	}

	private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		log.info("Submitting job {} ({}).", jobGraph.getJobID(), jobGraph.getName());
		submittedJobCounter.inc();

		final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
			.thenApply(ignored -> Acknowledge.get());

		return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {
			if (throwable != null) {
				cleanUpJobData(jobGraph.getJobID(), true);

				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
				throw new CompletionException(
					new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
			} else {
				return acknowledge;
			}
		}, getRpcService().getExecutor());
	}

	private void internalSubmitJob(JobGraph jobGraph, ChannelHandlerContext chx) {
		log.info("Submitting job {} ({}) by socket.", jobGraph.getJobID(), jobGraph.getName());
		submittedJobCounter.inc();

		final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminationJobManager(jobGraph.getJobID(), jobGraph, chx, this::persistAndRunJobWithContext)
			.thenApply(ignored -> Acknowledge.get());

		persistAndRunFuture.handleAsync((acknowledge, throwable) -> {
			if (throwable != null) {
				cleanUpJobData(jobGraph.getJobID(), true);

				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
				throw new CompletionException(
					new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
			} else {
				return acknowledge;
			}
		}, getRpcService().getExecutor());
	}

	private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
		jobGraphWriter.putJobGraph(jobGraph);

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> persistAndRunJobWithContext(JobGraph jobGraph, ChannelHandlerContext chx) throws Exception {
		jobGraphWriter.putJobGraph(jobGraph);

		final CompletableFuture<Void> runJobFuture = runJob(jobGraph, chx);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> runJob(JobGraph jobGraph) {

		if (configuration.get(BenchmarkOptions.JOB_RECEIVE_THEN_FINISH_ENABLE)) {
			archiveExecutionGraph(ArchivedExecutionGraph.buildEmptyArchivedExecutionGraph(jobGraph.getJobID(), jobGraph.getName()));
			return CompletableFuture.completedFuture(null);
		}

		Preconditions.checkState(!jobManagerRunnerFutures.containsKey(jobGraph.getJobID()));
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);
		totalRunningJobs.incrementAndGet();

		if (jmResourceAllocationEnabled) {
			CompletableFuture<Acknowledge> pendingJobFuture = pendingJobFutures.get(jobGraph.getJobID());
			if (pendingJobFuture != null) {
				pendingJobFutures.remove(jobGraph.getJobID());
				pendingJobFuture.complete(Acknowledge.get());
			}
		}

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
						totalRunningJobs.decrementAndGet();
					}
					if (jmResourceAllocationEnabled) {
						totalRunningTasks -= jobGraph.calcMinRequiredSlotsNum();

						while (canSubmitJob() && !pendingJobs.isEmpty()) {
							JobGraph pendingJob = pendingJobs.poll();
							internalSubmitJob(pendingJob);
						}
					}
				},
				getMainThreadExecutor());
	}

	private CompletableFuture<Void> runJob(JobGraph jobGraph, ChannelHandlerContext chx) {

		if (configuration.get(BenchmarkOptions.JOB_RECEIVE_THEN_FINISH_ENABLE)) {
			archiveExecutionGraph(ArchivedExecutionGraph.buildEmptyArchivedExecutionGraph(jobGraph.getJobID(), jobGraph.getName()));
			JobChannelManager jobChannelManager = jobResultClientManager.getJobChannelManager(jobGraph.getJobID());
			jobChannelManager.completeAllTask();
			jobChannelManager.finishJob();
			return CompletableFuture.completedFuture(null);
		}

		Preconditions.checkState(!jobManagerRunnerFutures.containsKey(jobGraph.getJobID()));
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph, chx);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);
		totalRunningJobs.incrementAndGet();

		if (jmResourceAllocationEnabled) {
			CompletableFuture<Acknowledge> pendingJobFuture = pendingJobFutures.get(jobGraph.getJobID());
			if (pendingJobFuture != null) {
				pendingJobFutures.remove(jobGraph.getJobID());
				pendingJobFuture.complete(Acknowledge.get());
			}
		}

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
						totalRunningJobs.decrementAndGet();
					}
					if (jmResourceAllocationEnabled) {
						totalRunningTasks -= jobGraph.calcMinRequiredSlotsNum();

						while (canSubmitJob() && !pendingJobsWithContext.isEmpty()) {
							Tuple2<JobGraph, ChannelHandlerContext> pendingJobAndContext = pendingJobsWithContext.poll();
							internalSubmitJob(pendingJobAndContext.f0, pendingJobAndContext.f1);
						}
					}
				},
				getMainThreadExecutor());
	}

	private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			() -> {
				try {
					totalRunningTasks += jobGraph.calcMinRequiredSlotsNum();

					return jobManagerRunnerFactory.createJobManagerRunner(
						jobGraph,
						configuration,
						rpcService,
						highAvailabilityServices,
						heartbeatServices,
						jobManagerSharedServices,
						new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
						fatalErrorHandler,
						registeredTaskManagers);
				} catch (Exception e) {
					throw new CompletionException(new JobExecutionException(jobGraph.getJobID(), "Could not instantiate JobManager.", e));
				}
			},
			rpcService.getExecutor());
	}

	private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph, ChannelHandlerContext chx) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			() -> {
				try {
					totalRunningTasks += jobGraph.calcMinRequiredSlotsNum();

					return jobManagerRunnerFactory.createJobManagerRunner(
						jobGraph,
						configuration,
						rpcService,
						highAvailabilityServices,
						heartbeatServices,
						jobManagerSharedServices,
						new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
						fatalErrorHandler,
						registeredTaskManagers,
						chx);
				} catch (Exception e) {
					throw new CompletionException(new JobExecutionException(jobGraph.getJobID(), "Could not instantiate JobManager.", e));
				}
			},
			rpcService.getExecutor());
	}

	private void writeFinishJobToContext(JobID jobId, ChannelHandlerContext channelContext, JobStatus jobStatus, Throwable throwable) {
		if (jobStatus.isGloballyTerminalState()) {
			if (channelContext != null) {
				JobChannelManager jobChannelManager = jobResultClientManager.getJobChannelManager(jobId);
				if (jobChannelManager != null) {
					if (jobStatus == JobStatus.FINISHED) {
						jobChannelManager.finishJob();
					} else {
						jobChannelManager.failJob(throwable == null ? new Exception("Job failed with status " + jobStatus) : throwable);
					}
				} else {
					log.warn("Get job channel manager null for job {}", jobId);
				}
			}
		}
	}

	private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
		final JobID jobId = jobManagerRunner.getJobID();

		FutureUtils.assertNoException(
			jobManagerRunner.getResultFuture().handleAsync(
				(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
					// check if we are still the active JobManagerRunner by checking the identity
					final JobManagerRunner currentJobManagerRunner = Optional.ofNullable(jobManagerRunnerFutures.get(jobId))
						.map(future -> future.getNow(null))
						.orElse(null);
					//noinspection ObjectEquality
					if (jobManagerRunner == currentJobManagerRunner) {
						if (archivedExecutionGraph != null) {
							jobReachedGloballyTerminalState(jobManagerRunner.getChannelContext(), throwable, archivedExecutionGraph);
						} else {
							final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

							if (strippedThrowable instanceof JobNotFinishedException) {
								jobNotFinished(jobId);
							} else {
								writeFinishJobToContext(
									jobManagerRunner.getJobID(),
									jobManagerRunner.getChannelContext(),
									JobStatus.FAILED,
									throwable);
								jobMasterFailed(jobId, strippedThrowable);
							}
						}
					} else {
						writeFinishJobToContext(
							jobManagerRunner.getJobID(),
							jobManagerRunner.getChannelContext(),
							JobStatus.FAILED,
							new Exception("There is a newer JobManagerRunner for the job " + jobId));
						log.debug("There is a newer JobManagerRunner for the job {}.", jobId);
					}

					return null;
				}, getMainThreadExecutor()));

		jobManagerRunner.start();

		return jobManagerRunner;
	}

	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		return CompletableFuture.completedFuture(
			Collections.unmodifiableSet(new HashSet<>(jobManagerRunnerFutures.keySet())));
	}

	@Override
	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath, Time timeout) {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

		return CompletableFuture.supplyAsync(
			() -> {
				log.info("Disposing savepoint {}.", savepointPath);

				try {
					Checkpoints.disposeSavepoint(savepointPath, configuration, classLoader, log);
				} catch (IOException | FlinkException e) {
					throw new CompletionException(new FlinkException(String.format("Could not dispose savepoint %s.", savepointPath), e));
				}

				return Acknowledge.get();
			},
			jobManagerSharedServices.getScheduledExecutorService());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.cancel(timeout));
	}

	@Override
	public CompletableFuture<String> requestJMWebShell(Time timeout) {
		return runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestJMWebShell(timeout));
	}

	@Override
	public CompletableFuture<String> requestJobManagerLogUrl(Time timeout) {
		return runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestJobManagerLogUrl(timeout));
	}

	@Override
	public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
		CompletableFuture<ResourceOverview> taskManagerOverviewFuture = runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestResourceOverview(timeout));

		final List<CompletableFuture<Optional<JobStatus>>> optionalJobInformation = queryJobMastersForInformation(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobStatus(timeout));

		CompletableFuture<Collection<Optional<JobStatus>>> allOptionalJobsFuture = FutureUtils.combineAll(optionalJobInformation);

		CompletableFuture<Collection<JobStatus>> allJobsFuture = allOptionalJobsFuture.thenApply(this::flattenOptionalCollection);

		final JobsOverview completedJobsOverview = archivedExecutionGraphStore.getStoredJobsOverview();

		return allJobsFuture.thenCombine(
			taskManagerOverviewFuture,
			(Collection<JobStatus> runningJobsStatus, ResourceOverview resourceOverview) -> {
				final JobsOverview allJobsOverview = JobsOverview.create(runningJobsStatus).combine(completedJobsOverview);
				return new ClusterOverview(resourceOverview, allJobsOverview);
			});
	}

	@Override
	public CompletableFuture<ClusterResourceOverview> requestClusterResourceOverview(Time timeout) {
		CompletableFuture<ResourceOverview> resourceOverview = runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestResourceOverview(timeout));
		return resourceOverview.thenApply(ClusterResourceOverview::new);
	}

	@Override
	public CompletableFuture<SmartResourcesStats> requestSmartResourcesStats(Time timeout) {
		return runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestSmartResourcesStats(timeout));
	}

	@Override
	public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
		List<CompletableFuture<Optional<JobDetails>>> individualOptionalJobDetails = queryJobMastersForInformation(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobDetails(timeout));

		CompletableFuture<Collection<Optional<JobDetails>>> optionalCombinedJobDetails = FutureUtils.combineAll(
			individualOptionalJobDetails);

		CompletableFuture<Collection<JobDetails>> combinedJobDetails = optionalCombinedJobDetails.thenApply(this::flattenOptionalCollection);

		final Collection<JobDetails> completedJobDetails = archivedExecutionGraphStore.getAvailableJobDetails();

		return combinedJobDetails.thenApply(
			(Collection<JobDetails> runningJobDetails) -> {
				final Collection<JobDetails> allJobDetails = new ArrayList<>(completedJobDetails.size() + runningJobDetails.size());

				allJobDetails.addAll(runningJobDetails);
				allJobDetails.addAll(completedJobDetails);

				return new MultipleJobsDetails(allJobDetails);
			});
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(JobID jobId, Time timeout) {
		return internalRequestJobStatus(jobId, timeout);
	}

	public CompletableFuture<JobStatus> internalRequestJobStatus(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		final CompletableFuture<JobStatus> jobStatusFuture = jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJobStatus(timeout));

		return jobStatusFuture.exceptionally(
			(Throwable throwable) -> {
				final JobDetails jobDetails = archivedExecutionGraphStore.getAvailableJobDetails(jobId);

				// check whether it is a completed job
				if (jobDetails == null) {
					throw new CompletionException(ExceptionUtils.stripCompletionException(throwable));
				} else {
					return jobDetails.getStatus();
				}
			});
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
			final JobID jobId,
			final JobVertexID jobVertexId) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestOperatorBackPressureStats(jobVertexId));
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(JobID jobId, Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		final CompletableFuture<ArchivedExecutionGraph> archivedExecutionGraphFuture = jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) -> jobMasterGateway.requestJob(timeout));

		return archivedExecutionGraphFuture.exceptionally(
			(Throwable throwable) -> {
				final ArchivedExecutionGraph serializableExecutionGraph = archivedExecutionGraphStore.get(jobId);

				// check whether it is a completed job
				if (serializableExecutionGraph == null) {
					throw new CompletionException(ExceptionUtils.stripCompletionException(throwable));
				} else {
					return serializableExecutionGraph;
				}
			});
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		if (jmResourceAllocationEnabled) {
			final CompletableFuture<Acknowledge> pendingJobFuture = pendingJobFutures.get(jobId);
			if (pendingJobFuture == null) {
				return internalRequestJobResult(jobId, timeout);
			} else {
				log.debug("Request Job: {} result, but is pending for submitting", jobId);
				return pendingJobFuture.thenCompose(ack -> {
					pendingJobFutures.remove(jobId);
					return internalRequestJobResult(jobId, timeout);
				});
			}
		} else {
			return internalRequestJobResult(jobId, timeout);
		}
	}

	public CompletableFuture<JobResult> internalRequestJobResult(JobID jobId, Time timeout) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			final ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphStore.get(jobId);

			if (archivedExecutionGraph == null) {
				// create wait JobMater in queue
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			} else {
				return CompletableFuture.completedFuture(JobResult.createFrom(archivedExecutionGraph));
			}
		} else {
			return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getResultFuture).thenApply(JobResult::createFrom);
		}
	}

	@Override
	public CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(Time timeout) {
		if (metricServiceQueryAddress != null) {
			return CompletableFuture.completedFuture(Collections.singleton(metricServiceQueryAddress));
		} else {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
		return runResourceManagerCommand(resourceManagerGateway -> resourceManagerGateway.requestTaskManagerMetricQueryServiceAddresses(timeout));
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return CompletableFuture.completedFuture(blobServer.getPort());
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			final JobID jobId,
			final String targetDirectory,
			final boolean cancelJob,
			final long savepointTimeout,
			final Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
			(JobMasterGateway jobMasterGateway) ->
				jobMasterGateway.triggerSavepoint(targetDirectory, cancelJob, savepointTimeout, timeout));
	}

	@Override
	public CompletableFuture<String> triggerDetachSavepoint(
		final JobID jobId,
		final String savepointId,
		final boolean blockSource,
		final long savepointTimeout,
		final Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose((JobMasterGateway jobMasterGateway) ->
			jobMasterGateway.triggerDetachSavepoint(savepointId, blockSource, savepointTimeout, timeout));
	}

	@Override
	public CompletableFuture<List<String>> dumpPendingSavepoints(JobID jobId) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(JobMasterGateway::dumpPendingSavepoints);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
			final JobID jobId,
			final String targetDirectory,
			final boolean advanceToEndOfEventTime,
			final long savepointTimeout,
			final Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		return jobMasterGatewayFuture.thenCompose(
				(JobMasterGateway jobMasterGateway) ->
						jobMasterGateway.stopWithSavepoint(targetDirectory, advanceToEndOfEventTime, savepointTimeout, timeout));
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster() {
		return shutDownCluster(ApplicationStatus.SUCCEEDED);
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster(final ApplicationStatus applicationStatus) {
		log.info("Shut down cluster for applicationStatus: {}.", applicationStatus);
		CompletableFuture<Acknowledge> archiveApplicationStatusFuture = CompletableFuture.completedFuture(Acknowledge.get());
		if (applicationModeClusterInfoArchiveEnable) {
			log.info("Starting to archive the applicationModeClusterInfo.");
			ApplicationModeClusterInfo applicationModeClusterInfo = new ApplicationModeClusterInfo();
			applicationModeClusterInfo.setApplicationStatus(applicationStatus);
			archiveApplicationStatusFuture = historyServerArchivist.archiveApplicationStatus(applicationModeClusterInfo);
		}
		archiveApplicationStatusFuture.whenComplete(
			(Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
					log.warn("Could not archive applicationModeClusterInfo to the history server.", throwable);
				}
				shutDownFuture.complete(applicationStatus);
			});
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void disconnectResourceManager(
			final ResourceManagerId resourceManagerId,
			final Exception cause) {
		if (isConnectingToResourceManager(resourceManagerId)) {
			reconnectToResourceManager(cause);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> offerTaskManagers(
			Collection<UnresolvedTaskManagerTopology> taskManagerTopologies,
			Time timeout) {

		if (log.isDebugEnabled()) {
			log.debug("Registered TM: [{}]",
				registeredTaskManagers.keySet().stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		Map<ResourceID, UnresolvedTaskManagerTopology> updatedRegisteredTM = new HashMap<>();
		for (UnresolvedTaskManagerTopology unresolvedTaskManagerTopology : taskManagerTopologies) {
			updatedRegisteredTM.put(
				unresolvedTaskManagerTopology.getUnresolvedTaskManagerLocation().getResourceID(),
					unresolvedTaskManagerTopology);
		}

		Set<ResourceID> added = new HashSet<>(updatedRegisteredTM.keySet());
		Set<ResourceID> removed = new HashSet<>(registeredTaskManagers.keySet());

		added.removeAll(registeredTaskManagers.keySet());
		removed.removeAll(updatedRegisteredTM.keySet());

		Map<ResourceID, ResolvedTaskManagerTopology> addedTaskManagerTopology = new HashMap<>();

		for (ResourceID resourceID: added) {
			final ResolvedTaskManagerTopology resolvedTaskManagerTopology;
			try {
				resolvedTaskManagerTopology = ResolvedTaskManagerTopology.fromUnresolvedTaskManagerTopology(
						updatedRegisteredTM.get(resourceID),
						useAddressAsHostname,
						configuration);
				registeredTaskManagers.put(resourceID, resolvedTaskManagerTopology);
				addedTaskManagerTopology.put(resourceID, resolvedTaskManagerTopology);

				if (useSocketEnable || jobReuseDispatcherEnable) {
					ClusterInformation clusterInformation = jobResultClientManager.getClusterInformation();
					resolvedTaskManagerTopology
						.getTaskExecutorGateway()
						.registerDispatcher(
							DispatcherRegistration.from(resourceId, clusterInformation, useSocketEnable, getAddress(), jobReuseDispatcherEnable));
				}
			} catch (Exception e) {
				log.error("Resource {} can not added to registeredTaskManagers, ignore it.", resourceID, e);
			}
		}

		for (ResourceID resourceID: removed) {
			ResolvedTaskManagerTopology resolvedTaskManagerTopology = registeredTaskManagers.remove(resourceID);
			if (resolvedTaskManagerTopology != null && resolvedTaskManagerTopology.getTaskExecutorNettyClient() != null) {
				resolvedTaskManagerTopology.getTaskExecutorNettyClient().close();
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("Added TM: [{}], Removed TM: [{}]",
				added.stream().map(Objects::toString).collect(Collectors.joining(",")),
				removed.stream().map(Objects::toString).collect(Collectors.joining(",")));
		}

		// update taskmanager change to JobMaster
		for (JobID jobID : jobManagerRunnerFutures.keySet()) {
			getJobMasterGatewayFuture(jobID).whenComplete(
				(jobMasterGateway, throwable) -> {
					if (throwable != null) {
						log.error("Get JobMasterGateway for job {} failed and can't notify TM change, because {}",
							jobID,
							throwable);
					} else {
						jobMasterGateway.notifyWorkerAdded(addedTaskManagerTopology);
						// this may cause Job never failed if miss some JobMasterGateway.
						// Maybe we need a sync loop.
						jobMasterGateway.notifyWorkerRemoved(removed);
					}
				}
			);
		}

		while (canSubmitJob() && (!pendingJobs.isEmpty() || !pendingJobsWithContext.isEmpty())) {
			if (!pendingJobsWithContext.isEmpty()) {
				Tuple2<JobGraph, ChannelHandlerContext> jobGraphAndContext = pendingJobsWithContext.poll();
				internalSubmitJob(jobGraphAndContext.f0, jobGraphAndContext.f1);
			}

			if (canSubmitJob() && !pendingJobs.isEmpty()) {
				JobGraph jobGraph = pendingJobs.poll();
				internalSubmitJob(jobGraph);
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
		return resourceManagerAddress != null
			&& resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	@Override
	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			JobID jobId,
			OperatorID operatorId,
			SerializedValue<CoordinationRequest> serializedRequest,
			Time timeout) {
		final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

		if (dispatcherFetchResultThreadPoolEnabled) {
			return jobMasterGatewayFuture.thenComposeAsync(
				(JobMasterGateway jobMasterGateway) ->
					jobMasterGateway.deliverCoordinationRequestToCoordinator(operatorId, serializedRequest, timeout),
				fetchResultExecutor);
		} else {
			return jobMasterGatewayFuture.thenCompose(
				(JobMasterGateway jobMasterGateway) ->
					jobMasterGateway.deliverCoordinationRequestToCoordinator(operatorId, serializedRequest, timeout));
		}
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJobAndRegisterTerminationFuture(JobID jobId, boolean cleanupHA) {
		final CompletableFuture<Void> cleanupFuture = removeJob(jobId, cleanupHA);

		registerJobManagerRunnerTerminationFuture(jobId, cleanupFuture);
	}

	private void registerJobManagerRunnerTerminationFuture(JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
		Preconditions.checkState(!jobManagerTerminationFutures.containsKey(jobId));

		jobManagerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

		// clean up the pending termination future
		jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> {
				final CompletableFuture<Void> terminationFuture = jobManagerTerminationFutures.remove(jobId);

				//noinspection ObjectEquality
				if (terminationFuture != null && terminationFuture != jobManagerRunnerTerminationFuture) {
					jobManagerTerminationFutures.put(jobId, terminationFuture);
				}
			},
			getMainThreadExecutor());
	}

	private CompletableFuture<Void> removeJob(JobID jobId, boolean cleanupHA) {
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.remove(jobId);
		totalRunningJobs.decrementAndGet();

		final CompletableFuture<Void> jobManagerRunnerTerminationFuture;
		if (jobManagerRunnerFuture != null) {
			jobManagerRunnerTerminationFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::closeAsync);
		} else {
			jobManagerRunnerTerminationFuture = CompletableFuture.completedFuture(null);
		}

		return jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> cleanUpJobData(jobId, cleanupHA),
			getRpcService().getExecutor());
	}

	private void cleanUpJobData(JobID jobId, boolean cleanupHA) {
		jobManagerMetricGroup.removeJob(jobId);

		boolean cleanupHABlobs = false;
		if (cleanupHA) {
			try {
				jobGraphWriter.removeJobGraph(jobId);

				// only clean up the HA blobs if we could remove the job from HA storage
				cleanupHABlobs = true;
			} catch (Exception e) {
				log.warn("Could not properly remove job {} from submitted job graph store.", jobId, e);
			}

			try {
				runningJobsRegistry.clearJob(jobId);
			} catch (IOException e) {
				log.warn("Could not properly remove job {} from the running jobs registry.", jobId, e);
			}
		} else {
			try {
				jobGraphWriter.releaseJobGraph(jobId);
			} catch (Exception e) {
				log.warn("Could not properly release job {} from submitted job graph store.", jobId, e);
			}
		}

		blobServer.cleanupJob(jobId, cleanupHABlobs);
	}

	/**
	 * Terminate all currently running {@link JobManagerRunnerImpl}.
	 */
	private void terminateJobManagerRunners() {
		log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

		final HashSet<JobID> jobsToRemove = new HashSet<>(jobManagerRunnerFutures.keySet());

		for (JobID jobId : jobsToRemove) {
			removeJobAndRegisterTerminationFuture(jobId, false);
		}
	}

	private CompletableFuture<Void> terminateJobManagerRunnersAndGetTerminationFuture() {
		terminateJobManagerRunners();
		final Collection<CompletableFuture<Void>> values = jobManagerTerminationFutures.values();
		return FutureUtils.completeAll(values);
	}

	private CompletableFuture<Void> archiveExecutionGraphAndGetResultFuture() {
		final Collection<CompletableFuture<Acknowledge>> values = archiveExecutionGraphFutures.values();
		return FutureUtils.completeAll(values);
	}

	public void onFatalError(Throwable throwable) {
		fatalErrorHandler.onFatalError(throwable);
	}

	protected void jobReachedGloballyTerminalState(ArchivedExecutionGraph archivedExecutionGraph) {
		Preconditions.checkArgument(
			archivedExecutionGraph.getState().isGloballyTerminalState(),
			"Job %s is in state %s which is not globally terminal.",
			archivedExecutionGraph.getJobID(),
			archivedExecutionGraph.getState());
		switch (archivedExecutionGraph.getState()) {
			case FINISHED:
				finishedJobCounter.inc();
				long jobDuration = archivedExecutionGraph.getStatusTimestamp(JobStatus.FINISHED) - archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED);
				jobDurationHistogram.update(jobDuration);
				String formattedJobName = archivedExecutionGraph.getJobName().replace("\n", "\\n").replace("\r", "\\r");
				if (this.recordSlowQuery && jobDuration > this.slowQueryLatencyThreshold) {
					slowJobsCounter.inc();
					log.info("[slow query] jobId: {}, query: {}, latency: {}.",
							archivedExecutionGraph.getJobID(), formattedJobName, jobDuration);
				} else {
					log.info("[query] jobId: {}, query: {}, latency: {}.",
							archivedExecutionGraph.getJobID(), formattedJobName, jobDuration);
				}

				jobToScheduleLatencyHistogram.update(
					archivedExecutionGraph.getScheduledTimestamp() - archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED));
				break;
			case FAILED:
				failedJobCounter.inc();
				failedJobDurationHistogram.update(
						archivedExecutionGraph.getStatusTimestamp(JobStatus.FAILED) - archivedExecutionGraph.getStatusTimestamp(JobStatus.CREATED));
				break;
			case CANCELED:
				canceledJobCounter.inc();
				break;
		}

		log.info("Job {} reached globally terminal state {}.", archivedExecutionGraph.getJobID(), archivedExecutionGraph.getState());

		archiveExecutionGraph(archivedExecutionGraph);

		final JobID jobId = archivedExecutionGraph.getJobID();

		removeJobAndRegisterTerminationFuture(jobId, true);
	}

	protected void jobReachedGloballyTerminalState(
			ChannelHandlerContext channelContext,
			Throwable throwable,
			ArchivedExecutionGraph archivedExecutionGraph) {
		writeFinishJobToContext(archivedExecutionGraph.getJobID(), channelContext, archivedExecutionGraph.getState(), throwable);
		this.jobReachedGloballyTerminalState(archivedExecutionGraph);
	}

	private void archiveExecutionGraph(ArchivedExecutionGraph archivedExecutionGraph) {
		try {
			archivedExecutionGraphStore.put(archivedExecutionGraph);
		} catch (IOException e) {
			log.info(
				"Could not store completed job {}({}).",
				archivedExecutionGraph.getJobName(),
				archivedExecutionGraph.getJobID(),
				e);
		}

		final CompletableFuture<Acknowledge> executionGraphFuture = historyServerArchivist.archiveExecutionGraph(archivedExecutionGraph);

		this.archiveExecutionGraphFutures.put(archivedExecutionGraph.getJobID(), executionGraphFuture);

		executionGraphFuture.whenComplete(
			(Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
					log.info(
						"Could not archive completed job {}({}) to the history server.",
						archivedExecutionGraph.getJobName(),
						archivedExecutionGraph.getJobID(),
						throwable);
				} else {
					log.debug("archive completed job {}({}) to the history server.",
						archivedExecutionGraph.getJobName(),
						archivedExecutionGraph.getJobID());
				}
			});
	}

	protected void jobNotFinished(JobID jobId) {
		log.info("Job {} was not finished by JobManager.", jobId);

		removeJobAndRegisterTerminationFuture(jobId, false);
	}

	private void jobMasterFailed(JobID jobId, Throwable cause) {
		// we fail fatally in case of a JobMaster failure in order to restart the
		// dispatcher to recover the jobs again. This only works in HA mode, though
		onFatalError(new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
	}

	private CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture(JobID jobId) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
		if (jmResourceAllocationEnabled) {
			CompletableFuture<Acknowledge> pendingJobFuture = pendingJobFutures.get(jobId);
			if (pendingJobFuture != null) {
				log.debug("Get JobMaster for Job: {}, but is pending for submitting", jobId);
				jobManagerRunnerFuture = pendingJobFuture.thenCompose(ack -> {
					pendingJobFutures.remove(jobId);
					return jobManagerRunnerFutures.get(jobId);
				});
			} else {
				jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);
			}
		} else {
			jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);
		}

		if (jobManagerRunnerFuture == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			final CompletableFuture<JobMasterGateway> leaderGatewayFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
			return leaderGatewayFuture.thenApplyAsync(
				(JobMasterGateway jobMasterGateway) -> {
					// check whether the retrieved JobMasterGateway belongs still to a running JobMaster
					if (jobManagerRunnerFutures.containsKey(jobId)) {
						return jobMasterGateway;
					} else {
						throw new CompletionException(new FlinkJobNotFoundException(jobId));
					}
				},
				getMainThreadExecutor());
		}
	}

	/**
	 * Get JobMasterGateway, return null if JobMasterGateway is not created.
	 */
	private JobMasterGateway getJobMasterGateway(JobID jobId){
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			return null;
		} else {
			if (jobManagerRunnerFuture.isDone()) {
				try {
					CompletableFuture<JobMasterGateway> jobMasterGatewayCompletableFuture = jobManagerRunnerFuture.get().getJobMasterGateway();
					if (jobMasterGatewayCompletableFuture.isDone()) {
						return jobMasterGatewayCompletableFuture.get();
					}
				} catch (ExecutionException | InterruptedException e) {
					log.error("Get JobMasterGateway for {} failed.", jobId, e);
				}
			}
		}
		return null;
	}

	private CompletableFuture<ResourceManagerGateway> getResourceManagerGateway() {
		return resourceManagerGatewayRetriever.getFuture();
	}

	private <T> CompletableFuture<T> runResourceManagerCommand(Function<ResourceManagerGateway, CompletableFuture<T>> resourceManagerCommand) {
		return getResourceManagerGateway().thenApply(resourceManagerCommand).thenCompose(Function.identity());
	}

	private <T> List<T> flattenOptionalCollection(Collection<Optional<T>> optionalCollection) {
		return optionalCollection.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
	}

	@Nonnull
	private <T> List<CompletableFuture<Optional<T>>> queryJobMastersForInformation(Function<JobMasterGateway, CompletableFuture<T>> queryFunction) {
		final int numberJobsRunning = jobManagerRunnerFutures.size();

		ArrayList<CompletableFuture<Optional<T>>> optionalJobInformation = new ArrayList<>(
			numberJobsRunning);

		for (JobID jobId : jobManagerRunnerFutures.keySet()) {
			final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getJobMasterGatewayFuture(jobId);

			final CompletableFuture<Optional<T>> optionalRequest = jobMasterGatewayFuture
				.thenCompose(queryFunction::apply)
				.handle((T value, Throwable throwable) -> Optional.ofNullable(value));

			optionalJobInformation.add(optionalRequest);
		}
		return optionalJobInformation;
	}

	private CompletableFuture<Void> waitForTerminatingJobManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
		final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
			.exceptionally((Throwable throwable) -> {
				throw new CompletionException(
					new DispatcherException(
						String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
						throwable));
			});

		return jobManagerTerminationFuture.thenComposeAsync(
			FunctionUtils.uncheckedFunction((ignored) -> {
				jobManagerTerminationFutures.remove(jobId);
				return action.apply(jobGraph);
			}),
			getMainThreadExecutor());
	}

	private CompletableFuture<Void> waitForTerminationJobManager(
			JobID jobId,
			JobGraph jobGraph,
			ChannelHandlerContext chx,
			BiFunctionWithException<JobGraph, ChannelHandlerContext, CompletableFuture<Void>, ?> action) {
		final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
			.exceptionally((Throwable throwable) -> {
				throw new CompletionException(
					new DispatcherException(
						String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
						throwable)); });

		return jobManagerTerminationFuture.thenComposeAsync(
			FunctionUtils.uncheckedFunction((ignored) -> {
				jobManagerTerminationFutures.remove(jobId);
				return action.apply(jobGraph, chx);
			}),
			getMainThreadExecutor());
	}

	CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
		if (jobManagerRunnerFutures.containsKey(jobId)) {
			return FutureUtils.completedExceptionally(new DispatcherException(String.format("Job with job id %s is still running.", jobId)));
		} else {
			return jobManagerTerminationFutures.getOrDefault(jobId, CompletableFuture.completedFuture(null));
		}
	}

	private void registerDispatcherMetrics(MetricGroup jobManagerMetricGroup) {
		jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_JOBS,
			() -> (long) jobManagerRunnerFutures.size());
		jobManagerMetricGroup.gauge(EVENT_METRIC_NAME, warehouseJobStartEventMessageRecorder.getJobStartEventMessageSet());

		jobManagerMetricGroup.counter(MetricNames.NUM_SUBMITTED_JOBS, submittedJobCounter);
		jobManagerMetricGroup.counter(MetricNames.NUM_FINISHED_JOBS, finishedJobCounter);
		jobManagerMetricGroup.counter(MetricNames.NUM_FAILED_JOBS, failedJobCounter);
		jobManagerMetricGroup.counter(MetricNames.NUM_CANCELED_JOBS, canceledJobCounter);
		jobManagerMetricGroup.counter(MetricNames.NUM_SLOW_JOBS, slowJobsCounter);
		jobManagerMetricGroup.counter(MetricNames.NUM_REJECTED_JOBS, rejectedJobCounter);

		jobManagerMetricGroup.histogram(MetricNames.JOB_DURATION, jobDurationHistogram);
		jobManagerMetricGroup.histogram(MetricNames.FAILED_JOB_DURATION, failedJobDurationHistogram);
		jobManagerMetricGroup.histogram(MetricNames.JOB_LATENCY_UNTIL_SCHEDULED, jobToScheduleLatencyHistogram);

		ExecutorService ioExecutor = jobManagerSharedServices.getIOExecutorService();
		if (ioExecutor instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor ioThreadPool = (ThreadPoolExecutor) ioExecutor;
			jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_IO_TASK, ioThreadPool::getActiveCount);
			jobManagerMetricGroup.gauge(MetricNames.NUM_PENDING_IO_TASK, () -> ioThreadPool.getQueue().size());
			jobManagerMetricGroup.gauge(MetricNames.IO_THREAD_POOL_USAGE, () -> (double) ioThreadPool.getActiveCount() / ioThreadPool.getCorePoolSize());
		}

		ExecutorService futureExecutor = jobManagerSharedServices.getScheduledExecutorService();
		if (futureExecutor instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor futureThreadPool = (ThreadPoolExecutor) futureExecutor;
			jobManagerMetricGroup.gauge(MetricNames.NUM_RUNNING_FUTURE_TASK, futureThreadPool::getActiveCount);
			jobManagerMetricGroup.gauge(MetricNames.NUM_PENDING_FUTURE_TASK, () -> futureThreadPool.getQueue().size());
			jobManagerMetricGroup.gauge(MetricNames.FUTURE_THREAD_POOL_USAGE, () -> (double) futureThreadPool.getActiveCount() / futureThreadPool.getCorePoolSize());
		}
	}

	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return CompletableFuture.runAsync(
			() -> removeJobAndRegisterTerminationFuture(jobId, false),
			getMainThreadExecutor());
	}

	Configuration getConfiguration() {
		return this.configuration;
	}

	public WarehouseJobStartEventMessageRecorder getWarehouseJobStartEventMessageRecorder() {
		return warehouseJobStartEventMessageRecorder;
	}

	/**
	 * The listener for leader changes of the resource manager.
	 */
	private final class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(Exception exception) {
			onFatalError(exception);
		}
	}

	private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	@Nullable
	private ResourceManagerAddress createResourceManagerAddress(@Nullable String newResourceManagerAddress, @Nullable ResourceManagerId resourceManagerId) {
		if (newResourceManagerAddress != null) {
			// the contract is: address == null <=> id == null
			checkNotNull(resourceManagerId);
			return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
		} else {
			return null;
		}
	}

	private void reconnectToResourceManager(Exception cause) {
		closeResourceManagerConnection(cause);
		tryConnectToResourceManager();
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			final ResourceID resourceManagerResourceId = establishedResourceManagerConnection.getResourceManagerResourceID();

			if (log.isDebugEnabled()) {
				log.debug("Close ResourceManager connection {}.",
					resourceManagerResourceId, cause);
			} else {
				log.info("Close ResourceManager connection {}.",
					resourceManagerResourceId);
			}

			ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
			resourceManagerGateway.disconnectDispatcher(resourceId, cause);

			final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();
			resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			// stop a potentially ongoing registration process
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		Preconditions.checkNotNull(resourceManagerAddress);
		Preconditions.checkArgument(resourceManagerConnection == null);
		Preconditions.checkArgument(establishedResourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}", resourceManagerAddress);

		resourceManagerConnection = new DispatcherToResourceManagerConnection(
			log,
			getAddress(),
			getDispatcherId(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			getMainThreadExecutor());

		resourceManagerConnection.start();
	}

	private void establishResourceManagerConnection(final DispatcherRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
			&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("Dispatcher successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

			establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
				resourceManagerGateway,
				resourceManagerResourceId);

			resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					resourceManagerGateway.heartbeatFromDispatcher(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the dispatcher side
				}
			});
		} else {
			log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);
		}
	}

	//----------------------------------------------------------------------------------------------
	private class DispatcherToResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess> {

		private final String dispatcherRpcAddress;

		private final DispatcherId dispatcherId;

		public DispatcherToResourceManagerConnection(
			final Logger log,
			final String dispatcherRpcAddress,
			final DispatcherId dispatcherId,
			final String resourceManagerAddress,
			final ResourceManagerId resourceManagerId,
			final Executor executor) {

			super(log, resourceManagerAddress, resourceManagerId, executor);
			this.dispatcherRpcAddress = dispatcherRpcAddress;
			this.dispatcherId = dispatcherId;
		}

		@Override
		protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, DispatcherRegistrationSuccess>(
				log,
				getRpcService(),
				"ResourceManager",
				ResourceManagerGateway.class,
				getTargetAddress(),
				getTargetLeaderId(),
				RetryingRegistrationConfiguration.fromConfiguration(configuration)) {

				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
					ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
					Time timeout = Time.milliseconds(timeoutMillis);

					return gateway.registerDispatcher(
						dispatcherId,
						resourceId,
						dispatcherRpcAddress,
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final DispatcherRegistrationSuccess success) {
			runAsync(() -> {
				// filter out outdated connections
				//noinspection ObjectEquality
				if (this == resourceManagerConnection) {
					establishResourceManagerConnection(success);
				}
			});
		}

		@Override
		protected void onRegistrationFailure(final Throwable failure) {
			onFatalError(failure);
		}
	}

	//----------------------------------------------------------------------------------------------

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			validateRunsInMainThread();
			log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);
			if (establishedResourceManagerConnection != null
				&& establishedResourceManagerConnection.getResourceManagerResourceID().equals(resourceId)) {
				reconnectToResourceManager(
					new DispatcherException(
						String.format("The heartbeat of ResourceManager with id %s timed out.", resourceId)));
			}
		}

		@Override
		public void reportPayload(ResourceID resourceID, Void payload) {
			// nothing to do since the payload is of type Void
		}

		@Override
		public Void retrievePayload(ResourceID resourceID) {
			return null;
		}
	}
}
