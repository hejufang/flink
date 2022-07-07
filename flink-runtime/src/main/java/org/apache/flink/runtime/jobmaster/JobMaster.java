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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BenchmarkOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.event.batch.WarehouseBatchJobInfoMessage;
import org.apache.flink.metrics.ConfigMessage;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.HeartbeatTarget;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.VirtualTaskManagerSlotPool;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.PerformCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerOfferSlots;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
import org.apache.flink.runtime.socket.result.JobChannelManager;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.HttpUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single
 * {@link JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 * <ul>
 * <li>{@link #updateTaskExecutionState} updates the task execution state for
 * given task</li>
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId> implements JobMasterGateway, JobMasterService {

	/** Default names for Flink's distributed components. */
	public static final String JOB_MANAGER_NAME = "jobmanager";

	// ------------------------------------------------------------------------

	private final JobMasterConfiguration jobMasterConfiguration;

	private final ResourceID resourceId;

	private final JobGraph jobGraph;

	private final JobInfo jobInfo;

	private final Counter heartbeatTimeoutWithRM = new SimpleCounter();

	private final Counter heartbeatTimeoutWithTM = new SimpleCounter();

	private final Time rpcTimeout;

	private final HighAvailabilityServices highAvailabilityServices;

	private final BlobWriter blobWriter;

	private final HeartbeatServices heartbeatServices;

	private final JobManagerJobMetricGroupFactory jobMetricGroupFactory;

	private final ScheduledExecutorService scheduledExecutorService;

	private final ScheduledExecutorService ioExecutorService;

	private final OnCompletionActions jobCompletionActions;

	private final FatalErrorHandler fatalErrorHandler;

	private final ClassLoader userCodeLoader;

	private final SlotPool slotPool;

	private final SlotSelectionStrategy slotSelectionStrategy;

	private final Scheduler scheduler;

	private final SchedulerNGFactory schedulerNGFactory;

	private final JobResultClientManager jobResultClientManager;

	// --------- BackPressure --------

	private final BackPressureStatsTracker backPressureStatsTracker;

	// --------- ResourceManager --------

	private final LeaderRetrievalService resourceManagerLeaderRetriever;

	// --------- Dispatcher --------

	private final LeaderRetrievalService dispatcherLeaderRetriever;

	private DispatcherId dispatcherId;

	private DispatcherGateway dispatcherGateway;

	// --------- TaskManagers --------

	private final Map<ResourceID, Tuple2<TaskManagerLocation, TaskExecutorGateway>> registeredTaskManagers;

	private final ShuffleMaster<?> shuffleMaster;

	// -------- Mutable fields ---------

	private HeartbeatManager<AccumulatorReport, AllocatedSlotReport> taskManagerHeartbeatManager;

	private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

	private SchedulerNG schedulerNG;

	@Nullable
	private JobManagerJobStatusListener jobStatusListener;

	@Nullable
	private JobManagerJobMetricGroup jobManagerJobMetricGroup;

	@Nullable
	private ResourceManagerAddress resourceManagerAddress;

	@Nullable
	private ResourceManagerConnection resourceManagerConnection;

	@Nullable
	private EstablishedResourceManagerConnection establishedResourceManagerConnection;

	private Map<String, Object> accumulators;

	private final JobMasterPartitionTracker partitionTracker;

	private final RemoteBlacklistReporter remoteBlacklistReporter;

	private final boolean useAddressAsHostNameEnable;

	private final boolean requestSlotFromResourceManagerDirectEnable;

	private final boolean jobMasterResourceAllocationDirectEnabled;

	private final boolean jobReuseDispatcherEnable;

	// for batch warehouse
	private static final String FLINK_BATCH_JOB_INFO_METRICS = "flink_batch_info";
	private WarehouseBatchJobInfoMessage warehouseBatchJobInfoMessage;
	private MessageSet<WarehouseBatchJobInfoMessage> batchJobInfoMessageSet;
	private final boolean useMainScheduledExecutorEnable;
	private final boolean minResourceSlotPoolSimplifyEnabled;

	// ------------------------------------------------------------------------

	public JobMaster(
			RpcService rpcService,
			JobMasterConfiguration jobMasterConfiguration,
			ResourceID resourceId,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityService,
			SlotPoolFactory slotPoolFactory,
			SchedulerFactory schedulerFactory,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices,
			JobManagerJobMetricGroupFactory jobMetricGroupFactory,
			OnCompletionActions jobCompletionActions,
			FatalErrorHandler fatalErrorHandler,
			ClassLoader userCodeLoader,
			SchedulerNGFactory schedulerNGFactory,
			ShuffleMaster<?> shuffleMaster,
			PartitionTrackerFactory partitionTrackerFactory) throws Exception {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(JOB_MANAGER_NAME), null);

		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.blobWriter = jobManagerSharedServices.getBlobWriter();
		this.scheduledExecutorService = jobManagerSharedServices.getScheduledExecutorService();
		this.ioExecutorService = jobManagerSharedServices.getIOExecutorService();
		this.jobResultClientManager = jobManagerSharedServices.getJobResultClientManager();
		this.jobCompletionActions = checkNotNull(jobCompletionActions);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.schedulerNGFactory = checkNotNull(schedulerNGFactory);
		this.heartbeatServices = checkNotNull(heartbeatServices);
		this.jobMetricGroupFactory = checkNotNull(jobMetricGroupFactory);
		this.useAddressAsHostNameEnable = jobMasterConfiguration.getConfiguration().getBoolean(CoreOptions.USE_ADDRESS_AS_HOSTNAME_ENABLE);
		this.requestSlotFromResourceManagerDirectEnable = jobMasterConfiguration.getConfiguration().getBoolean(JobManagerOptions.JOBMANAGER_REQUEST_SLOT_FROM_RESOURCEMANAGER_ENABLE);
		this.jobMasterResourceAllocationDirectEnabled = jobMasterConfiguration.getConfiguration().getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED);
		this.jobReuseDispatcherEnable = jobMasterConfiguration.getConfiguration().getBoolean(ClusterOptions.JOB_REUSE_DISPATCHER_IN_TASKEXECUTOR_ENABLE);
		this.useMainScheduledExecutorEnable = jobMasterConfiguration.getConfiguration().getBoolean(CoreOptions.ENDPOINT_USE_MAIN_SCHEDULED_EXECUTOR_ENABLE);
		this.minResourceSlotPoolSimplifyEnabled = jobMasterConfiguration.getConfiguration().getBoolean(JobManagerOptions.MIN_RESOURCE_SLOT_POOL_SIMPLIFY_ENABLED);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.info("Initializing job {} ({}).", jobName, jid);

		resourceManagerLeaderRetriever = highAvailabilityServices.getResourceManagerLeaderRetriever();

		dispatcherLeaderRetriever = highAvailabilityServices.getDispatcherLeaderRetriever();

		int taskCount = 0;
		for (JobVertex vertex : jobGraph.getVertices()) {
			taskCount += vertex.getParallelism();
		}
		this.slotPool = checkNotNull(slotPoolFactory).createSlotPool(jobGraph.getJobID(), taskCount, minResourceSlotPoolSimplifyEnabled);
		if (jobMasterResourceAllocationDirectEnabled) {
			Preconditions.checkState(this.slotPool instanceof VirtualTaskManagerSlotPool,
					"must use VirtualTaskManagerSlotPool when jobMasterResourceAllocationDirectEnabled");
		}

		this.slotSelectionStrategy = schedulerFactory.getSlotSelectionStrategy();

		this.scheduler = checkNotNull(schedulerFactory).createScheduler(slotPool);

		this.registeredTaskManagers = new HashMap<>(4);
		this.partitionTracker = checkNotNull(partitionTrackerFactory)
			.create(resourceID -> {
				Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerInfo = registeredTaskManagers.get(resourceID);
				if (taskManagerInfo == null) {
					return Optional.empty();
				}

				return Optional.of(taskManagerInfo.f1);
			});

		this.backPressureStatsTracker = checkNotNull(jobManagerSharedServices.getBackPressureStatsTracker());

		this.shuffleMaster = checkNotNull(shuffleMaster);

		this.remoteBlacklistReporter = BlacklistUtil.createRemoteBlacklistReporter(
				jobMasterConfiguration.getConfiguration(),
				jobGraph.getJobID(),
				rpcTimeout);
		this.remoteBlacklistReporter.addIgnoreExceptionClass(RemoteTransportException.class);
		this.remoteBlacklistReporter.addIgnoreExceptionClass(PartitionException.class);

		this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
		this.schedulerNG = createScheduler(jobManagerJobMetricGroup);
		this.jobStatusListener = null;

		this.resourceManagerConnection = null;
		this.establishedResourceManagerConnection = null;

		this.accumulators = new HashMap<>();
		this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

		final int slotsPerTaskManager = jobMasterConfiguration.getConfiguration().getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		final int numTaskManagers = (jobGraph.calcMinRequiredSlotsNum() + slotsPerTaskManager - 1) / slotsPerTaskManager;
		final double initialPercentage = jobMasterConfiguration.getConfiguration().getDouble(TaskManagerOptions.NUM_INITIAL_TASK_MANAGERS_PERCENTAGE);
		final int initialExtraTaskManagers = jobMasterConfiguration.getConfiguration().getInteger(TaskManagerOptions.NUM_EXTRA_INITIAL_TASK_MANAGERS);
		this.jobInfo = new JobInfo(jobGraph.calcMinRequiredSlotsNum(), (int) (numTaskManagers * initialPercentage), initialExtraTaskManagers);

		// send job's configuration
		if (jobManagerJobMetricGroup != null) {
			final MessageSet<ConfigMessage> messageSet = new MessageSet<>(MessageType.JOB_CONFIG);
			jobManagerJobMetricGroup.gauge("jobConfig", messageSet);
			for (Map.Entry<String, String> entry : jobMasterConfiguration.getConfiguration().toMap().entrySet()) {
				messageSet.addMessage(new Message<>(new ConfigMessage(entry.getKey(), entry.getValue())));
			}

			jobManagerJobMetricGroup.counter(MetricNames.NUM_JM_HEARTBEAT_TIMOUT_FROM_RM, heartbeatTimeoutWithRM);
			jobManagerJobMetricGroup.counter(MetricNames.NUM_JM_HEARTBEAT_TIMOUT_FROM_TM, heartbeatTimeoutWithTM);

			JobCheckpointingSettings jobCheckpointingSettings = jobGraph.getCheckpointingSettings();
			if (jobCheckpointingSettings != null){
				MessageSet<CheckpointCoordinatorConfiguration> checkpointConfigMessageSet = new MessageSet<>(MessageType.CHECKPOINT_CONFIG);
				jobManagerJobMetricGroup.gauge("checkpointConfig", checkpointConfigMessageSet);
				CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = jobCheckpointingSettings.getCheckpointCoordinatorConfiguration();
				checkpointConfigMessageSet.addMessage(new Message<>(checkpointCoordinatorConfiguration));
			}

			// job version metrics
			TagGauge jobVersionTagGauge = new TagGauge.TagGaugeBuilder().build();
			jobVersionTagGauge.addMetric(1, createVersionTagValues());
			jobManagerJobMetricGroup.gauge("jobVersion", jobVersionTagGauge);
			jobManagerJobMetricGroup.gauge(MetricNames.NUM_JOB_REQUIRED_WORKERS, () -> jobInfo.getInitialTaskManagers() + jobInfo.getInitialExtraTaskManagers());

			// send flink batch info message to warehouse
			if (jobMasterConfiguration.getConfiguration().getString(ExecutionOptions.EXECUTION_APPLICATION_TYPE).equals(ConfigConstants.FLINK_BATCH_APPLICATION_TYPE)) {
				this.warehouseBatchJobInfoMessage = new WarehouseBatchJobInfoMessage();
				log.info("Init the warehouseBatchJobInfoMessage.");
				this.warehouseBatchJobInfoMessage.setJobStartTimestamp(System.currentTimeMillis());
				// set the shuffle service type
				String shuffleServiceType = jobMasterConfiguration.getConfiguration()
					.getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) ? ShuffleServiceOptions.CLOUD_SHUFFLE : ShuffleServiceOptions.NETTY_SHUFFLE;
				this.warehouseBatchJobInfoMessage.setShuffleServiceType(shuffleServiceType);
				// init message set
				batchJobInfoMessageSet = new MessageSet<>(MessageType.FLINK_BATCH);
				jobManagerJobMetricGroup.gauge(FLINK_BATCH_JOB_INFO_METRICS, this.batchJobInfoMessageSet);

				// send the init job status to css coordinator in batch mode if enabled
				if (jobMasterConfiguration.getConfiguration().getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) &&
					jobMasterConfiguration.getConfiguration().getBoolean(ShuffleServiceOptions.CLOUD_SHUFFLE_SERVICE_REPORT_JOB_STATUS_TO_COORDINATOR)) {
					reportBatchJobStatusToCss(jobMasterConfiguration.getConfiguration(), schedulerNG.requestJobStatus().name());
				}
			}
		}
	}

	private TagGaugeStore.TagValues createVersionTagValues() {
		// create tags to custom metric flink.job.jobVersion
		// tags: version, commitId, commitDate, isInDockerMode, subVersion, flinkJobType, owner, dockerImage, flinkApi, appType
		EnvironmentInformation.RevisionInformation revisionInformation = EnvironmentInformation.getRevisionInformation();
		String version = EnvironmentInformation.getVersion();
		String commitId = revisionInformation.commitId;
		String commitDate = null;
		if (revisionInformation.commitDate != null) {
			commitDate = revisionInformation.commitDate.replace(" ", "_");
		}
		boolean isInDockerMode = this.jobMasterConfiguration.getConfiguration().getBoolean(ConfigConstants.IS_IN_DOCKER_MODE_KEY, false);
		String subVersion = this.jobMasterConfiguration.getConfiguration().getString(ConfigConstants.FLINK_SUBVERSION_KEY, null);
		String flinkJobType = this.jobMasterConfiguration.getConfiguration().getString(ConfigConstants.FLINK_JOB_TYPE_KEY, ConfigConstants.FLINK_JOB_TYPE_DEFAULT);
		boolean isKubernetes = this.jobMasterConfiguration.getConfiguration().getBoolean(ConfigConstants.IS_KUBERNETES_KEY, false);
		String dockerImage;
		if (isKubernetes) {
			//k8s use kubernetes.container.image as docker conf
			dockerImage = this.jobMasterConfiguration.getConfiguration().getString("kubernetes.container.image", "");
		} else {
			//yarn use docker.image as docker conf
			dockerImage = this.jobMasterConfiguration.getConfiguration().getString(ConfigConstants.DOCKER_IMAGE, null);
		}
		String flinkApi = this.jobMasterConfiguration.getConfiguration().getString(ConfigConstants.FLINK_JOB_API_KEY, "DataSet");
		String appType = this.jobMasterConfiguration.getConfiguration().getString(ExecutionOptions.EXECUTION_APPLICATION_TYPE, ConfigConstants.FLINK_STREAMING_APPLICATION_TYPE).trim().replace(" ", "_");
		String shuffleServiceType = jobMasterConfiguration.getConfiguration().getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) ? ShuffleServiceOptions.CLOUD_SHUFFLE : ShuffleServiceOptions.NETTY_SHUFFLE;
		String appName = System.getenv().get(ConfigConstants.ENV_FLINK_YARN_JOB);
		String owner = null;
		if (!StringUtils.isNullOrWhitespaceOnly(appName)) {
			// we assume that the format of appName is {jobName}_{owner}
			int index = appName.lastIndexOf("_");
			if (index > 0) {
				if (index + 1 < appName.length()) {
					owner = appName.substring(index + 1);
				}
			}
		}
		TagGaugeStore.TagValuesBuilder tagValuesBuilder = new TagGaugeStore.TagValuesBuilder();

		if (!StringUtils.isNullOrWhitespaceOnly(version)) {
			tagValuesBuilder.addTagValue("version", version);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(commitId)) {
			tagValuesBuilder.addTagValue("commitId", commitId);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(commitDate)) {
			tagValuesBuilder.addTagValue("commitDate", commitDate);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(String.valueOf(isInDockerMode))) {
			tagValuesBuilder.addTagValue("isInDockerMode", String.valueOf(isInDockerMode));
		}
		if (!StringUtils.isNullOrWhitespaceOnly(shuffleServiceType)) {
			tagValuesBuilder.addTagValue("shuffleServiceType", shuffleServiceType);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(String.valueOf(isKubernetes))) {
			tagValuesBuilder.addTagValue(ConfigConstants.IS_KUBERNETES_KEY, String.valueOf(isKubernetes));
		}
		if (!StringUtils.isNullOrWhitespaceOnly(flinkJobType)) {
			tagValuesBuilder.addTagValue("flinkJobType", flinkJobType);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(subVersion)) {
			tagValuesBuilder.addTagValue("subVersion", subVersion);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(owner)) {
			tagValuesBuilder.addTagValue("owner", owner);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(dockerImage)) {
			tagValuesBuilder.addTagValue("dockerImage", dockerImage);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(flinkApi)) {
			tagValuesBuilder.addTagValue("flinkApi", flinkApi);
		}
		if (!StringUtils.isNullOrWhitespaceOnly(appType)) {
			tagValuesBuilder.addTagValue("appType", appType);
		}
		return tagValuesBuilder.build();
	}

	private SchedulerNG createScheduler(final JobManagerJobMetricGroup jobManagerJobMetricGroup) throws Exception {
		return schedulerNGFactory.createInstance(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutorService,
			jobMasterConfiguration.getConfiguration(),
			slotSelectionStrategy,
			slotPool,
			scheduler,
			scheduledExecutorService,
			userCodeLoader,
			highAvailabilityServices.getCheckpointRecoveryFactory(),
			rpcTimeout,
			blobWriter,
			jobManagerJobMetricGroup,
			jobMasterConfiguration.getSlotRequestTimeout(),
			shuffleMaster,
			partitionTracker,
			remoteBlacklistReporter,
			jobResultClientManager);
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
		// make sure we receive RPC and async calls
		start();

		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId)} method once we take the leadership back again.
	 *
	 * <p>This method is executed asynchronously
	 *
	 * @param cause The reason of why this job been suspended.
	 * @return Future acknowledge indicating that the job has been suspended. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> suspend(final Exception cause) {
		CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(
				() -> suspendExecution(cause),
				RpcUtils.INF_TIMEOUT);

		return suspendFuture.whenComplete((acknowledge, throwable) -> stop());
	}

	/**
	 * Suspend the job and shutdown all other services including rpc.
	 */
	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping the JobMaster for job {}({}).", jobGraph.getName(), jobGraph.getJobID());

		// disconnect from all registered TaskExecutors
		final Collection<ResourceID> taskManagerResourceIds;

		if (jobReuseDispatcherEnable && jobMasterResourceAllocationDirectEnabled) {
			taskManagerResourceIds = slotPool.getAllocatedTaskManagers();
		} else {
			taskManagerResourceIds = new HashSet<>(registeredTaskManagers.keySet());
		}

		final FlinkException cause = new FlinkException("Stopping JobMaster for job " + jobGraph.getName() +
			'(' + jobGraph.getJobID() + ").");

		slotPool.markWillBeClosed();

		for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
			disconnectTaskManager(taskManagerResourceId, cause);
		}

		// make sure there is a graceful exit
		suspendExecution(new FlinkException("JobManager is shutting down."));

		// shut down will internally release all registered slots
		slotPool.close();

		remoteBlacklistReporter.close();

		return CompletableFuture.completedFuture(null);
	}

	//----------------------------------------------------------------------------------------------
	// RPC methods
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		schedulerNG.cancel();

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Acknowledge the task execution state update
	 */
	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(
			final TaskExecutionState taskExecutionState) {
		FlinkException taskExecutionException;
		try {
			checkNotNull(taskExecutionState, "taskExecutionState");

			if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				taskExecutionException =
					new ExecutionGraphException(
						"The execution attempt "
							+ taskExecutionState.getID()
							+ " was not found.");
			}
		} catch (Exception e) {
			taskExecutionException =
				new JobMasterException(
					"Could not update the state of task execution for JobMaster.", e);
			handleJobMasterError(taskExecutionException);
		}
		return FutureUtils.completedExceptionally(taskExecutionException);
	}

	@Override
	public CompletableFuture<Acknowledge> batchUpdateTaskExecutionState(final BatchTaskExecutionState batchTaskExecutionState) {
		return batchTaskExecutionState.batchUpdateTaskExecutionState(this);
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt) {

		try {
			return CompletableFuture.completedFuture(schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
		} catch (IOException e) {
			log.warn("Error while requesting next input split", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID resultPartitionId) {

		try {
			return CompletableFuture.completedFuture(schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
		} catch (PartitionProducerDisposedException e) {
			log.info("Error while requesting partition state", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			final ResultPartitionID partitionID,
			final Time timeout) {

		schedulerNG.scheduleOrUpdateConsumers(partitionID);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(final ResourceID resourceID, final Exception cause) {
		log.debug("Disconnect TaskExecutor {} because: {}", resourceID, cause.getMessage());

		taskManagerHeartbeatManager.unmonitorTarget(resourceID);
		if (!jobMasterResourceAllocationDirectEnabled) {
			slotPool.releaseTaskManager(resourceID, cause);
		}
		partitionTracker.stopTrackingPartitionsFor(resourceID);

		if (jobReuseDispatcherEnable) {
			ResolvedTaskManagerTopology resolvedTaskManagerTopology = slotPool.getResolvedTaskManagerTopology(resourceID);
			if (resolvedTaskManagerTopology != null) {
				resolvedTaskManagerTopology.getTaskExecutorGateway().disconnectJobManager(jobGraph.getJobID(), cause);
			}
		} else {
			Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManagerConnection = registeredTaskManagers.remove(resourceID);

			if (taskManagerConnection != null) {
				taskManagerConnection.f1.disconnectJobManager(jobGraph.getJobID(), cause);
			}
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	// TODO: This method needs a leader session ID
	@Override
	public void acknowledgeCheckpoint(
			final JobID jobID,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final CheckpointMetrics checkpointMetrics,
			final TaskStateSnapshot checkpointState) {

		schedulerNG.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
	}

	// TODO: This method needs a leader session ID
	@Override
	public void declineCheckpoint(DeclineCheckpoint decline) {
		schedulerNG.declineCheckpoint(decline);
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
			final ExecutionAttemptID task,
			final OperatorID operatorID,
			final SerializedValue<OperatorEvent> serializedEvent) {

		try {
			final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
			schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public void performCheckpoint(PerformCheckpoint performCheckpoint) {
		schedulerNG.performCheckpoint(performCheckpoint);
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(final JobID jobId, final String registrationName) {
		try {
			return CompletableFuture.completedFuture(schedulerNG.requestKvStateLocation(jobId, registrationName));
		} catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
			log.info("Error while request key-value state location", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(
			final JobID jobId,
			final JobVertexID jobVertexId,
			final KeyGroupRange keyGroupRange,
			final String registrationName,
			final KvStateID kvStateId,
			final InetSocketAddress kvStateServerAddress) {

		try {
			schedulerNG.notifyKvStateRegistered(jobId, jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} catch (FlinkJobNotFoundException e) {
			log.info("Error while receiving notification about key-value state registration", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName) {
		try {
			schedulerNG.notifyKvStateUnregistered(jobId, jobVertexId, keyGroupRange, registrationName);
			return CompletableFuture.completedFuture(Acknowledge.get());
		} catch (FlinkJobNotFoundException e) {
			log.info("Error while receiving notification about key-value state de-registration", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			final Time timeout) {
		validateNotInResourceAllocationDirectMode();

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

		if (taskManager == null) {
			return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
		}

		final TaskManagerLocation taskManagerLocation = taskManager.f0;
		final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

		final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken(), requestSlotFromResourceManagerDirectEnable, getAddress());

		return CompletableFuture.completedFuture(
			slotPool.offerSlots(
				taskManagerLocation,
				rpcTaskManagerGateway,
				slots));
	}

	@Override
	public CompletableFuture<Acknowledge> offerOptimizeSlots(Collection<TaskManagerOfferSlots> slots, Time timeout) {
		validateNotInResourceAllocationDirectMode();
		CompletableFuture<Acknowledge> future = new CompletableFuture<>();

		List<CompletableFuture<?>> futureList = new ArrayList<>(slots.size());
		for (TaskManagerOfferSlots taskManagerOfferSlots : slots) {
			futureList.add(
				registerTaskManager(taskManagerOfferSlots.getTaskManagerRpcAddress(), taskManagerOfferSlots.getUnresolvedTaskManagerLocation(), timeout)
					.whenComplete((registration, throwable) -> {
						if (registration instanceof RegistrationResponse.Success) {
							Collection<SlotOffer> offerCollection = taskManagerOfferSlots.getSlotOffers();
							offerSlots(
									taskManagerOfferSlots.getUnresolvedTaskManagerLocation().getResourceID(),
									offerCollection,
									timeout)
								.whenComplete((offerResults, offerException) -> {
									if (offerResults == null || offerResults.size() != taskManagerOfferSlots.getSlotOffers().size() || offerException != null) {
										String errorMessage = "Offer slots failed for slotOffers " + offerCollection + " accept offers " + offerResults;
										throw new RuntimeException(errorMessage, offerException);
									}
								});
						} else {
							String errorMessage = "Register taskmanager " + taskManagerOfferSlots.getTaskManagerRpcAddress() +
								" with response " + registration;
							log.error(errorMessage, throwable);
							throw new RuntimeException(errorMessage, throwable);
						}
					}));
		}
		FutureUtils.completeAll(futureList).whenComplete((v, t) -> {
			if (t != null) {
				future.completeExceptionally(t);
			} else {
				future.complete(Acknowledge.get());
			}
		});

		return future;
	}

	@Override
	public void failSlot(
			final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause) {
		validateNotInResourceAllocationDirectMode();

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			internalFailAllocation(allocationId, cause);
		} else {
			log.warn("Cannot fail slot " + allocationId + " because the TaskManager " +
			taskManagerId + " is unknown.");
		}
	}

	private void internalFailAllocation(AllocationID allocationId, Exception cause) {
		final Optional<ResourceID> resourceIdOptional = slotPool.failAllocation(allocationId, cause);
		resourceIdOptional.ifPresent(taskManagerId -> {
			if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
				releaseEmptyTaskManager(taskManagerId);
			}
		});
	}

	private void releaseEmptyTaskManager(ResourceID resourceId) {
		disconnectTaskManager(resourceId, new FlinkException(String.format("No more slots registered at JobMaster %s.", resourceId)));
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
			final Time timeout) {

		final TaskManagerLocation taskManagerLocation;
		try {
			taskManagerLocation = TaskManagerLocation.fromUnresolvedLocation(unresolvedTaskManagerLocation, useAddressAsHostNameEnable);
		} catch (Throwable throwable) {
			final String errMsg = String.format(
				"Could not accept TaskManager registration. TaskManager address %s cannot be resolved. %s",
				unresolvedTaskManagerLocation.getExternalAddress(),
				throwable.getMessage());
			log.error(errMsg);
			return CompletableFuture.completedFuture(new RegistrationResponse.Decline(errMsg));
		}

		final ResourceID taskManagerId = taskManagerLocation.getResourceID();

		if (registeredTaskManagers.containsKey(taskManagerId)) {
			final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
			return CompletableFuture.completedFuture(response);
		} else {
			return getRpcService()
				.connect(taskManagerRpcAddress, TaskExecutorGateway.class)
				.handleAsync(
					(TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
						if (throwable != null) {
							return new RegistrationResponse.Decline(throwable.getMessage());
						}

						log.debug("Register TaskExecutor: {}", taskManagerId);
						slotPool.registerTaskManager(taskManagerId);
						registeredTaskManagers.put(taskManagerId, Tuple2.of(taskManagerLocation, taskExecutorGateway));

						// monitor the task manager as heartbeat target
						taskManagerHeartbeatManager.monitorTarget(taskManagerId, new HeartbeatTarget<AllocatedSlotReport>() {
							@Override
							public void receiveHeartbeat(ResourceID resourceID, AllocatedSlotReport payload) {
								// the task manager will not request heartbeat, so this method will never be called currently
							}

							@Override
							public void requestHeartbeat(ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
								taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
							}
						});

						return new JMTMRegistrationSuccess(resourceId);
					},
					getMainThreadExecutor());
		}
	}

	@Override
	public void disconnectResourceManager(
			final ResourceManagerId resourceManagerId,
			final Exception cause) {

		if (isConnectingToResourceManager(resourceManagerId)) {
			reconnectToResourceManager(cause);
		}
	}

	private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
		return resourceManagerAddress != null
				&& resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
	}

	@Override
	public void heartbeatFromTaskManager(final ResourceID resourceID, AccumulatorReport accumulatorReport) {
		taskManagerHeartbeatManager.receiveHeartbeat(resourceID, accumulatorReport);
	}

	@Override
	public void heartbeatFromResourceManager(final ResourceID resourceID) {
		resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		return CompletableFuture.completedFuture(schedulerNG.requestJobDetails());
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		return CompletableFuture.completedFuture(schedulerNG.requestJob());
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
			@Nullable final String targetDirectory,
			final boolean cancelJob,
			final long savepointTimeout,
			final Time timeout) {

		return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, savepointTimeout);
	}

	@Override
	public CompletableFuture<String> triggerDetachSavepoint(
		final String savepointId,
		final boolean blockSource,
		final long savepointTimeout,
		final Time timeout) {

		return schedulerNG.triggerDetachSavepoint(savepointId, blockSource, savepointTimeout);
	}

	@Override
	public CompletableFuture<List<String>> dumpPendingSavepoints() {
		return schedulerNG.dumpPendingSavepoints();
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
			@Nullable final String targetDirectory,
			final boolean advanceToEndOfEventTime,
			final long savepointTimeout,
			final Time timeout) {

		return schedulerNG.stopWithSavepoint(targetDirectory, advanceToEndOfEventTime, savepointTimeout);
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(final JobVertexID jobVertexId) {
		try {
			final Optional<OperatorBackPressureStats> operatorBackPressureStats = schedulerNG.requestOperatorBackPressureStats(jobVertexId);
			return CompletableFuture.completedFuture(OperatorBackPressureStatsResponse.of(
				operatorBackPressureStats.orElse(null)));
		} catch (FlinkException e) {
			log.info("Error while requesting operator back pressure stats", e);
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
		validateNotInResourceAllocationDirectMode();
		internalFailAllocation(allocationID, cause);
	}

	@Override
	public void notifyWorkerRemoved(Set<ResourceID> workers) {
		Exception cause = new FlinkException("Worker released.");
		for (ResourceID resourceID : workers) {
			slotPool.releaseTaskManager(resourceID, cause);
		}
	}

	@Override
	public void notifyWorkerAdded(Map<ResourceID, ResolvedTaskManagerTopology> workers) {
		workers.forEach(slotPool::registerTaskManager);
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

		AggregateFunction aggregateFunction = null;
		try {
			aggregateFunction = InstantiationUtil.deserializeObject(serializedAggregateFunction, userCodeLoader);
		} catch (Exception e) {
			log.error("Error while attempting to deserialize user AggregateFunction.");
			return FutureUtils.completedExceptionally(e);
		}

		Object accumulator = accumulators.get(aggregateName);
		if (null == accumulator) {
			accumulator = aggregateFunction.createAccumulator();
		}
		accumulator = aggregateFunction.add(aggregand, accumulator);
		accumulators.put(aggregateName, accumulator);
		return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			OperatorID operatorId,
			SerializedValue<CoordinationRequest> serializedRequest,
			Time timeout) {
		try {
			CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
			return schedulerNG.deliverCoordinationRequestToCoordinator(operatorId, request);
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	//-- job starting and stopping  -----------------------------------------------------------------

	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {

		validateRunsInMainThread();

		checkNotNull(newJobMasterId, "The new JobMasterId must not be null.");

		if (Objects.equals(getFencingToken(), newJobMasterId)) {
			log.info("Already started the job execution with JobMasterId {}.", newJobMasterId);

			return Acknowledge.get();
		}

		setNewFencingToken(newJobMasterId);

		startJobMasterServices();

		log.info("Starting execution of job {} ({}) under job master id {}.", jobGraph.getName(), jobGraph.getJobID(), newJobMasterId);

		if (jobMasterConfiguration.getConfiguration().get(BenchmarkOptions.JOB_ANALYZED_THEN_FINISH_ENABLE)) {
			// Job finish before schedule
			schedulerNG.setMainThreadExecutor(getMainThreadExecutor());
			schedulerNG.transitionExecutionGraphState(JobStatus.CREATED, JobStatus.FINISHED);
			schedulerNG.transitionAllExecutionState(ExecutionState.FINISHED);
			scheduledExecutorService.execute(() -> jobCompletionActions.jobReachedGloballyTerminalState(schedulerNG.requestJob()));
			if (jobResultClientManager != null) {
				JobChannelManager jobChannelManager = jobResultClientManager.getJobChannelManager(jobGraph.getJobID());
				jobChannelManager.completeAllTask();
			}
			return Acknowledge.get();
		}

		resetAndStartScheduler();

		return Acknowledge.get();
	}

	private void startJobMasterServices() throws Exception {
		startHeartbeatServices();

		// start the slot pool make sure the slot pool now accepts messages for this leader
		slotPool.start(getFencingToken(), getAddress(), getMainThreadExecutor());
		scheduler.start(getMainThreadExecutor());
		remoteBlacklistReporter.start(getFencingToken(), getMainThreadExecutor());

		//TODO: Remove once the ZooKeeperLeaderRetrieval returns the stored address upon start
		// try to reconnect to previously known leader
		reconnectToResourceManager(new FlinkException("Starting JobMaster component."));

		// job is ready to go, try to establish connection with resource manager
		//   - activate leader retrieval for the resource manager
		//   - on notification of the leader, the connection will be established and
		//     the slot pool will start requesting slots
		resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());

		// Retrieve Dispatcher Address and Gateway.
		// TODO need to register to Dispatcher and create heartbeat.
		dispatcherLeaderRetriever.start(new DispatcherLeaderListener());
	}

	private void setNewFencingToken(JobMasterId newJobMasterId) {
		if (getFencingToken() != null) {
			log.info("Restarting old job with JobMasterId {}. The new JobMasterId is {}.", getFencingToken(), newJobMasterId);

			// first we have to suspend the current execution
			suspendExecution(new FlinkException("Old job with JobMasterId " + getFencingToken() +
				" is restarted with a new JobMasterId " + newJobMasterId + '.'));
		}

		// set new leader id
		setFencingToken(newJobMasterId);
	}

	/**
	 * Suspending job, all the running tasks will be cancelled, and communication with other components
	 * will be disposed.
	 *
	 * <p>Mostly job is suspended because of the leadership has been revoked, one can be restart this job by
	 * calling the {@link #start(JobMasterId)} method once we take the leadership back again.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	private Acknowledge suspendExecution(final Exception cause) {
		validateRunsInMainThread();

		if (getFencingToken() == null) {
			log.debug("Job has already been suspended or shutdown.");
			return Acknowledge.get();
		}

		// not leader anymore --> set the JobMasterId to null
		setFencingToken(null);

		try {
			resourceManagerLeaderRetriever.stop();
			resourceManagerAddress = null;
		} catch (Throwable t) {
			log.warn("Failed to stop resource manager leader retriever when suspending.", t);
		}

		try {
			dispatcherLeaderRetriever.stop();
			dispatcherGateway = null;
		} catch (Throwable t) {
			log.warn("Failed to stop dispatcher leader retriever when suspending.", t);
		}

		suspendAndClearSchedulerFields(cause);

		// the slot pool stops receiving messages and clears its pooled slots
		slotPool.suspend();

		remoteBlacklistReporter.suspend();

		// disconnect from resource manager:
		closeResourceManagerConnection(cause);

		stopHeartbeatServices();

		return Acknowledge.get();
	}

	private void stopHeartbeatServices() {
		taskManagerHeartbeatManager.stop();
		resourceManagerHeartbeatManager.stop();
	}

	private void startHeartbeatServices() {
		if (!jobReuseDispatcherEnable) {
			taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
				resourceId,
				new TaskManagerHeartbeatListener(),
				useMainScheduledExecutorEnable ? getMainThreadExecutor().getMainScheduledExecutor() : getMainThreadExecutor(),
				log);
		}

		resourceManagerHeartbeatManager = heartbeatServices.createHeartbeatManager(
			resourceId,
			new ResourceManagerHeartbeatListener(),
			useMainScheduledExecutorEnable ? getMainThreadExecutor().getMainScheduledExecutor() : getMainThreadExecutor(),
			log);
	}

	private void assignScheduler(
			SchedulerNG newScheduler,
			JobManagerJobMetricGroup newJobManagerJobMetricGroup) {
		validateRunsInMainThread();
		checkState(schedulerNG.requestJobStatus().isTerminalState());
		checkState(jobManagerJobMetricGroup == null);

		schedulerNG = newScheduler;
		jobManagerJobMetricGroup = newJobManagerJobMetricGroup;
	}

	private void resetAndStartScheduler() throws Exception {
		validateRunsInMainThread();

		long startScheduleTime = System.currentTimeMillis();
		final CompletableFuture<Void> schedulerAssignedFuture;

		if (schedulerNG.requestJobStatus() == JobStatus.CREATED) {
			schedulerAssignedFuture = CompletableFuture.completedFuture(null);
			schedulerNG.setMainThreadExecutor(getMainThreadExecutor());
		} else {
			suspendAndClearSchedulerFields(new FlinkException("ExecutionGraph is being reset in order to be rescheduled."));
			final JobManagerJobMetricGroup newJobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
			final SchedulerNG newScheduler = createScheduler(newJobManagerJobMetricGroup);

			schedulerAssignedFuture = schedulerNG.getTerminationFuture().handle(
				(ignored, throwable) -> {
					newScheduler.setMainThreadExecutor(getMainThreadExecutor());
					assignScheduler(newScheduler, newJobManagerJobMetricGroup);
					return null;
				}
			);
		}

		schedulerAssignedFuture
				.thenRun(this::startScheduling)
				.get();

		log.info("Schedule {} take {} ms.", jobGraph.getName(), System.currentTimeMillis() - startScheduleTime);
	}

	private void startScheduling() {
		checkState(jobStatusListener == null);
		// register self as job status change listener
		jobStatusListener = new JobManagerJobStatusListener();
		schedulerNG.registerJobStatusListener(jobStatusListener);

		schedulerNG.startScheduling();

		if (this.warehouseBatchJobInfoMessage != null) {
			schedulerNG.getTerminationFuture().whenComplete(
				(result, throwable) -> {
					this.warehouseBatchJobInfoMessage
						.setJobStatus(schedulerNG.requestJobStatus().name())
						.setTaskNums(schedulerNG.requestJob().getAllVertices().values().stream().mapToLong(AccessExecutionJobVertex::getParallelism).sum())
						.setJobEndTimestamp(System.currentTimeMillis()).calculateProcessLatencyMs();

					if (this.schedulerNG instanceof SchedulerBase) {
						this.warehouseBatchJobInfoMessage.setFailoverTimes(((SchedulerBase) this.schedulerNG).getNumberOfRestarts());
						this.warehouseBatchJobInfoMessage.setJobFailoverForPartitionUnavailableTimes(((SchedulerBase) this.schedulerNG).getNumberOfPartitionExceptions());
					}
					if (throwable != null) {
						log.warn("There are some errors for this job({}).", jobGraph.getName(), throwable);
					}
					this.batchJobInfoMessageSet.addMessage(new Message<>(this.warehouseBatchJobInfoMessage));
					log.info("The warehouseBatchJobInfoMessage of this job is {}.", warehouseBatchJobInfoMessage.toString());
					if (jobMasterConfiguration.getConfiguration().getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) &&
						jobMasterConfiguration.getConfiguration().getBoolean(ShuffleServiceOptions.CLOUD_SHUFFLE_SERVICE_REPORT_JOB_STATUS_TO_COORDINATOR)) {
						reportBatchJobStatusToCss(jobMasterConfiguration.getConfiguration(), schedulerNG.requestJobStatus().name());
					}
				}
			);
		}
	}

	/**
	 * If using css, here can report the job status to css coordinator.
	 *
	 * @param configuration the job configuration
	 * @param status the job status
	 */
	@VisibleForTesting
	void reportBatchJobStatusToCss(Configuration configuration, String status) {
		try {
			ObjectMapper objectMapper = new ObjectMapper();

			Map<String, String> params = new HashMap<>();
			params.put("region", configuration.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT));
			params.put("cluster", configuration.getString(ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT));
			params.put("queue", configuration.getString(ConfigConstants.QUEUE_KEY, ConfigConstants.QUEUE_DEFAULT));
			if (status.equals(JobStatus.FAILED.name())) {
				params.put("rootCause", "Flink batch job failed.");
			}

			ReportQuery query = new ReportQuery(ConfigConstants.CLOUD_SHUFFLE_SERVICE_TYPE, configuration.get(PipelineOptions.NAME), status, params, System.currentTimeMillis());
			String coordinatorURL = configuration.getString(ConfigConstants.CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL, ConfigConstants.CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL_DEFAULT);
			if (coordinatorURL.equals(ConfigConstants.CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL_DEFAULT)) {
				log.warn("The url of css coordinator not set, could not report the job status to css.");
				return;
			}
			String targetUrl = coordinatorURL + ConfigConstants.CSS_COORDINATOR_REPORT;

			Map<String, String> header = new HashMap<>();
			header.put("Content-Type", "application/json");
			HttpUtil.HttpResponsePojo response = HttpUtil.sendPost(targetUrl, objectMapper.writeValueAsString(query), header);
			log.info("The request body is {}, and the response is {}.", objectMapper.writeValueAsString(query), response);
			int code = response.getStatusCode();
			if (code >= 200 && code < 300) {
				String jsonBody = response.getContent();
				JsonNode node = objectMapper.readTree(jsonBody);
				if (node.has("code") && node.get("code").intValue() == 200) {
					log.info("Report the job status({}) to css success.", query);
				} else {
					String errMessage = response.getContent();
					log.warn("CloudShuffleCoordinator request rejected because of {}.", errMessage);
				}
			} else {
				String errMessage = response.getContent();
				log.warn("CloudShuffleCoordinator receives invalid code : {}, message: {}.", code, errMessage);
			}
		} catch (Throwable t) {
			log.warn("Some errors occur while reporting the job status to css.", t);
		}
	}

	private void suspendAndClearSchedulerFields(Exception cause) {
		suspendScheduler(cause);
		clearSchedulerFields();
	}

	private void suspendScheduler(Exception cause) {
		schedulerNG.suspend(cause);

		if (jobManagerJobMetricGroup != null) {
			jobManagerJobMetricGroup.close();
		}

		if (jobStatusListener != null) {
			jobStatusListener.stop();
		}
	}

	@VisibleForTesting
	WarehouseBatchJobInfoMessage getWarehouseBatchJobInfoMessage() {
		return this.warehouseBatchJobInfoMessage;
	}

	private void clearSchedulerFields() {
		jobManagerJobMetricGroup = null;
		jobStatusListener = null;
	}

	//----------------------------------------------------------------------------------------------

	private void handleJobMasterError(final Throwable cause) {
		if (ExceptionUtils.isJvmFatalError(cause)) {
			log.error("Fatal error occurred on JobManager.", cause);
			// The fatal error handler implementation should make sure that this call is non-blocking
			fatalErrorHandler.onFatalError(cause);
		} else {
			jobCompletionActions.jobMasterFailed(cause);
		}
	}

	@Override
	public void onFatalError(Throwable cause){
		fatalErrorHandler.onFatalError(cause);
	}

	private void jobStatusChanged(
			final JobStatus newJobStatus,
			long timestamp,
			@Nullable final Throwable error) {
		validateRunsInMainThread();

		if (newJobStatus.isGloballyTerminalState()) {
			runAsync(() -> registeredTaskManagers.keySet()
				.forEach(newJobStatus == JobStatus.FINISHED
					? partitionTracker::stopTrackingAndReleaseOrPromotePartitionsFor
					: partitionTracker::stopTrackingAndReleasePartitionsFor));

			final ArchivedExecutionGraph archivedExecutionGraph = schedulerNG.requestJob();
			scheduledExecutorService.execute(() -> jobCompletionActions.jobReachedGloballyTerminalState(archivedExecutionGraph));
		}
	}

	private void notifyOfNewResourceManagerLeader(final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
		resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

		reconnectToResourceManager(new FlinkException(String.format("ResourceManager leader changed to new address %s", resourceManagerAddress)));
	}

	private void notifyOfNewDispatcherLeader(final String newDispatcherAddress, final DispatcherId newDispatcherId) {
		if (newDispatcherAddress ==  null) {
			log.info("Job {}: Dispatcher leader changed to null", jobGraph.getJobID());
			dispatcherGateway = null;
		} else if (this.dispatcherId == null || !this.dispatcherId.equals(newDispatcherId)) {
			getRpcService()
					.connect(newDispatcherAddress, newDispatcherId, DispatcherGateway.class)
					.thenApply(gateway -> dispatcherGateway = gateway);
		}
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

	private void tryConnectToResourceManager() {
		if (resourceManagerAddress != null) {
			connectToResourceManager();
		}
	}

	private void connectToResourceManager() {
		assert(resourceManagerAddress != null);
		assert(resourceManagerConnection == null);
		assert(establishedResourceManagerConnection == null);

		log.info("Connecting to ResourceManager {}", resourceManagerAddress);

		resourceManagerConnection = new ResourceManagerConnection(
			log,
			jobGraph.getJobID(),
			resourceId,
			getAddress(),
			getFencingToken(),
			resourceManagerAddress.getAddress(),
			resourceManagerAddress.getResourceManagerId(),
			scheduledExecutorService);

		resourceManagerConnection.start();
	}

	private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
		final ResourceManagerId resourceManagerId = success.getResourceManagerId();

		// verify the response with current connection
		if (resourceManagerConnection != null
				&& Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

			log.info("JobManager successfully registered at ResourceManager, leader id: {}.", resourceManagerId);

			final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

			final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

			establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
				resourceManagerGateway,
				resourceManagerResourceId);

			slotPool.connectToResourceManager(resourceManagerGateway);

			schedulerNG.setResourceManager(resourceManagerGateway);

			remoteBlacklistReporter.connectToResourceManager(resourceManagerGateway);

			resourceManagerHeartbeatManager.monitorTarget(resourceManagerResourceId, new HeartbeatTarget<Void>() {
				@Override
				public void receiveHeartbeat(ResourceID resourceID, Void payload) {
					resourceManagerGateway.heartbeatFromJobManager(resourceID);
				}

				@Override
				public void requestHeartbeat(ResourceID resourceID, Void payload) {
					// request heartbeat will never be called on the job manager side
				}
			});
		} else {
			log.debug("Ignoring resource manager connection to {} because it's duplicated or outdated.", resourceManagerId);

		}
	}

	private void closeResourceManagerConnection(Exception cause) {
		if (establishedResourceManagerConnection != null) {
			dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
			establishedResourceManagerConnection = null;
		}

		if (resourceManagerConnection != null) {
			// stop a potentially ongoing registration process
			resourceManagerConnection.close();
			resourceManagerConnection = null;
		}
	}

	private void dissolveResourceManagerConnection(EstablishedResourceManagerConnection establishedResourceManagerConnection, Exception cause) {
		final ResourceID resourceManagerResourceID = establishedResourceManagerConnection.getResourceManagerResourceID();

		if (log.isDebugEnabled()) {
			log.debug("Close ResourceManager connection {}.", resourceManagerResourceID, cause);
		} else {
			log.info("Close ResourceManager connection {}: {}.", resourceManagerResourceID, cause.getMessage());
		}

		resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

		ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
		resourceManagerGateway.disconnectJobManager(jobGraph.getJobID(), schedulerNG.requestJobStatus(), cause);
		slotPool.disconnectResourceManager();
		remoteBlacklistReporter.disconnectResourceManager();
	}

	private void validateNotInResourceAllocationDirectMode() {
		Preconditions.checkState(
				!this.jobMasterResourceAllocationDirectEnabled, "Not support in resource allocation direct mode");
	}

	//----------------------------------------------------------------------------------------------
	// Service methods
	//----------------------------------------------------------------------------------------------

	@Override
	public JobMasterGateway getGateway() {
		return getSelfGateway(JobMasterGateway.class);
	}

	//----------------------------------------------------------------------------------------------
	// Utility classes
	//----------------------------------------------------------------------------------------------

	private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			runAsync(
				() -> notifyOfNewResourceManagerLeader(
					leaderAddress,
					ResourceManagerId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(final Exception exception) {
			handleJobMasterError(new Exception("Fatal error in the ResourceManager leader service", exception));
		}
	}

	private class DispatcherLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(@Nullable String leaderAddress, @Nullable UUID leaderSessionID) {
			runAsync(
					() -> notifyOfNewDispatcherLeader(
							leaderAddress,
							DispatcherId.fromUuidOrNull(leaderSessionID)));
		}

		@Override
		public void handleError(Exception exception) {
			handleJobMasterError(new Exception("Fatal error in the Dispatcher leader service", exception));
		}
	}

	//----------------------------------------------------------------------------------------------

	private class ResourceManagerConnection
			extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> {
		private final JobID jobID;

		private final ResourceID jobManagerResourceID;

		private final String jobManagerRpcAddress;

		private final JobMasterId jobMasterId;

		ResourceManagerConnection(
				final Logger log,
				final JobID jobID,
				final ResourceID jobManagerResourceID,
				final String jobManagerRpcAddress,
				final JobMasterId jobMasterId,
				final String resourceManagerAddress,
				final ResourceManagerId resourceManagerId,
				final Executor executor) {
			super(log, resourceManagerAddress, resourceManagerId, executor);
			this.jobID = checkNotNull(jobID);
			this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
			this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
			this.jobMasterId = checkNotNull(jobMasterId);
		}

		@Override
		protected RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess> generateRegistration() {
			return new RetryingRegistration<ResourceManagerId, ResourceManagerGateway, JobMasterRegistrationSuccess>(
				log,
				getRpcService(),
				"ResourceManager",
				ResourceManagerGateway.class,
				getTargetAddress(),
				getTargetLeaderId(),
				jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

				@Override
				protected CompletableFuture<RegistrationResponse> invokeRegistration(
						ResourceManagerGateway gateway, ResourceManagerId fencingToken, long timeoutMillis) {
					Time timeout = Time.milliseconds(timeoutMillis);

					return gateway.registerJobManager(
						jobMasterId,
						jobManagerResourceID,
						jobManagerRpcAddress,
						jobID,
						jobInfo,
						timeout);
				}
			};
		}

		@Override
		protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
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
			handleJobMasterError(failure);
		}
	}

	//----------------------------------------------------------------------------------------------

	private class JobManagerJobStatusListener implements JobStatusListener {

		private volatile boolean running = true;

		@Override
		public void jobStatusChanges(
				final JobID jobId,
				final JobStatus newJobStatus,
				final long timestamp,
				final Throwable error) {

			if (running) {
				// run in rpc thread to avoid concurrency
				runAsync(() -> jobStatusChanged(newJobStatus, timestamp, error));
			}
		}

		private void stop() {
			running = false;
		}
	}

	private class TaskManagerHeartbeatListener implements HeartbeatListener<AccumulatorReport, AllocatedSlotReport> {

		@Override
		public void notifyHeartbeatTimeout(ResourceID resourceID) {
			validateRunsInMainThread();
			heartbeatTimeoutWithTM.inc();
			disconnectTaskManager(
				resourceID,
				new TimeoutException("Heartbeat of TaskManager with id " + resourceID + " timed out."));
		}

		@Override
		public void reportPayload(ResourceID resourceID, AccumulatorReport payload) {
			validateRunsInMainThread();
			for (AccumulatorSnapshot snapshot : payload.getAccumulatorSnapshots()) {
				schedulerNG.updateAccumulators(snapshot);
			}
		}

		@Override
		public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
			validateRunsInMainThread();
			return slotPool.createAllocatedSlotReport(resourceID);
		}
	}

	private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

		@Override
		public void notifyHeartbeatTimeout(final ResourceID resourceId) {
			validateRunsInMainThread();
			log.info("The heartbeat of ResourceManager with id {} timed out.", resourceId);
			heartbeatTimeoutWithRM.inc();
			if (establishedResourceManagerConnection != null && establishedResourceManagerConnection.getResourceManagerResourceID().equals(resourceId)) {
				reconnectToResourceManager(
					new JobMasterException(
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

	private class ReportQuery {

		private String type;
		private String appId;
		private String status;
		private Map<String, String> appInfo;
		private long endTime;

		public ReportQuery(String type, String appId, String status, Map<String, String> appInfo, long endTime) {
			this.type = type;
			this.appId = appId;
			this.status = status;
			this.appInfo = appInfo;
			this.endTime = endTime;
		}

		public String getType() {
			return type;
		}

		public String getAppId() {
			return appId;
		}

		public String getStatus() {
			return status;
		}

		public Map<String, String> getAppInfo() {
			return appInfo;
		}

		public long getEndTime() {
			return endTime;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ReportQuery that = (ReportQuery) o;
			return Objects.equals(type, that.type) && Objects.equals(appId, that.appId) && Objects.equals(status, that.status) && Objects.equals(appInfo, that.appInfo) && Objects.equals(endTime, that.endTime);
		}

		@Override
		public int hashCode() {
			return Objects.hash(type, appId, status, appInfo, endTime);
		}

		@Override
		public String toString() {
			return "ReportQuery{" +
				"type='" + type + '\'' +
				", appId='" + appId + '\'' +
				", status='" + status + '\'' +
				", appInfo=" + appInfo +
				", endTime=" + endTime +
				'}';
		}
	}
}

