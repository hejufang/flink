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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.configuration.HdfsConfigOptions;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.messages.WarehouseJobStartEventMessage;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.smartresources.ContainerResources;
import org.apache.flink.smartresources.UpdateContainersResources;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.exceptions.ContainerCompletedException;
import org.apache.flink.yarn.exceptions.ExpectedContainerCompletedException;
import org.apache.flink.yarn.slowcontainer.NoOpSlowContainerManager;
import org.apache.flink.yarn.slowcontainer.SlowContainerManager;
import org.apache.flink.yarn.slowcontainer.SlowContainerManagerImpl;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;
import org.apache.flink.shaded.byted.com.bytedance.commons.consul.ServiceNode;
import org.apache.flink.shaded.httpclient.org.apache.http.client.config.RequestConfig;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpGet;
import org.apache.flink.shaded.httpclient.org.apache.http.client.utils.URIBuilder;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.HttpClients;
import org.apache.flink.shaded.httpclient.org.apache.http.util.EntityUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.bytedance.sr.estimater.client.EstimaterClient;
import com.bytedance.sr.estimater.client.ResourcesUsage;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.ConstraintContent;
import org.apache.hadoop.yarn.api.protocolrecords.GlobalConstraint;
import org.apache.hadoop.yarn.api.protocolrecords.GlobalConstraints;
import org.apache.hadoop.yarn.api.protocolrecords.NodeSatisfyAttributesContent;
import org.apache.hadoop.yarn.api.protocolrecords.NodeSkipHighLoadContent;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RuntimeConfiguration;
import org.apache.hadoop.yarn.api.records.ConstraintType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.DeschedulerNodeSkipHighLoadContent;
import org.apache.hadoop.yarn.api.records.DeschedulerResult;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.GangSchedulerNotifyContent;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NotifyMsg;
import org.apache.hadoop.yarn.api.records.NotifyMsgType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.RtQoSLevel;
import org.apache.hadoop.yarn.api.records.SchedulerType;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.megatron.MegatronUtil;
import org.apache.hadoop.yarn.util.webshell.NMWebshellUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.yarn.Utils.pruneContainerId;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<YarnWorkerNode>
	implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);

	private static final String EVENT_METRIC_NAME = "resourceManagerEvent";

	private final MessageSet<WarehouseJobStartEventMessage> jobStartEventMessageSet = new MessageSet<>(MessageType.JOB_START_EVENT);

	static final String ENV_YARN_CONTAINER_ID = "CONTAINER_ID";

	/** The process environment variables. */
	private final Map<String, String> env;

	/** YARN container map. Package private for unit test purposes. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;
	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	static final String ERROR_MASSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	private final int numberOfTaskSlots;

	private final int defaultTaskManagerMemoryMB;

	private final double defaultCpus;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClient nodeManagerClient;

	/** Enable NMClientAsync or not. */
	private Boolean nmClientAsyncEnabled;

	/** Client to async communicate with the Node manager and launch TaskExecutor processes. */
	private NMClientAsync nodeManagerClientAsync;

	/** The number of containers requested, but not yet granted. */
	private int numPendingContainerRequests;

	private final SlowContainerManager slowContainerManager;
	private long containerStartDurationMaxMs;
	/** Interval in milliseconds of check if the container is slow. */
	private final long slowContainerCheckIntervalMs;

	private final Set<String> yarnBlackedHosts;

	private final TagGauge completedContainerGauge = new TagGauge.TagGaugeBuilder().setClearAfterReport(true).build();

	/**
	 * executor for start yarn container.
	 */
	@VisibleForTesting
	protected Executor executor;

	private final Map<ResourceProfile, Integer> resourcePriorities = new HashMap<>();

	private final Collection<ResourceProfile> slotsPerWorker;

	private final Resource resource;

	// for smart resources
	private Thread containerResourcesUpdater;
	private Map<ContainerId, Long/* expired time ms */> pendingUpdating;
	/**
	 * Resource Manager has already updated, wait NodeManager update success.
	 */
	private Map<ContainerId, Container> waitNMUpdate;
	/** The failed times to update container. */
	private Map<ContainerId, Integer> containerUpdateFailedTimesMap;
	/** Before this timestamp, the container will be skipped for updating resource.*/
	private Map<ContainerId, Long> skipUpdateContainersMap;
	/** If the container retry times exceed this, it will be add to nmUpdateFailedTimes to skip update for some times. */
	private static final int CONTAINER_MAX_RETRY_TIMES = 3;
	/** If the container retry times exceed maxRetryTimes, it will be skipping to update for this times. */
	private static final long CONTAINER_SKIP_UPDATE_TIME_MS = 30 * 60 * 1000;
	private ContainerResources targetResources;
	private long resourcesUpdateTimeoutMS = 2 * 60 * 1000;
	private EstimaterClient estimaterClient;
	private String region;
	private String cluster;
	private String applicationID;
	private String applicationName;
	private int durtionMinutes;
	private double cpuReserveRatio;
	private double memReserveRatio;
	private SmartResourcesStats smartResourcesStats;
	private boolean disableMemAdjust = false;
	private boolean srCpuAdjustDoubleEnable = false;
	private int srMemMaxMB;
	private String srCpuEstimateMode;
	private String srAdjustCheckApi;
	private int srAdjustCheckBackoffMS;
	private int srAdjustCheckTimeoutMS;
	private long srNextCheckTimeMS;

	/**
	 * Fatal on GangScheduler failed allocate containers.
	 * Only be true when first allocate after YarnResourceManager started.
	 */
	private boolean fatalOnGangFailed;
	private final int gangMaxRetryTimes;
	private int gangCurrentRetryTimes;
	private final Counter gangFailedCounter = new SimpleCounter();
	private final Counter gangDowngradeCounter = new SimpleCounter();
	private long gangLastDowngradeTimestamp;
	private final int gangDowngradeTimeoutMilli;
	private final boolean gangDowngradeOnFailed;
	private final boolean gangSchedulerEnabled;
	private final String nodeAttributesExpression;

	private final boolean cleanupRunningContainersOnStop;

	private final String currentContainerId;

	/** The conf that yarn container bind cpu core. */
	private final RuntimeConfiguration yarnRuntimeConf;

	/** Enable yarn container descheduler. */
	private final boolean deschedulerEnable;
	/** Enable yarn container descheduler for load Type. */
	private final boolean deschedulerLoadTypeEnable;
	/** Enable yarn container descheduler for disk type. */
	private final boolean deschedulerDiskTypeEnable;
	/** Interval minimum time for yarn container performance type descheduler. */
	private final long deschedulerPerfIntervalMinMs;
	/** Interval minimum time for yarn container usability type descheduler. */
	private final long deschedulerUsabilityIntervalMinMs;
	/** Request new containers time out for yarn container descheduler. */
	private final long deschedulerRequestTimeout;
	/** Check interval for handle yarn container descheduler. */
	private final long descheduledContainersCheckIntervalMs;
	/** ConstraintType list according to type priority. */
	private final List<ConstraintType> priorityConstraintTypes;
	/** Descheduled containers need to handle. */
	private List<Container> descheduledContainers;
	/** Descheduled containers' constraint type now. */
	private ConstraintType descheduledConstraintType;
	/** Completed last time of constraint types, use for interval limit. */
	private Map<ConstraintType, Long> deschedulerCompleteLastTime;
	/** The time when handle descheduler start, use for deschedulerDurationMs counter. */
	private long deschedulerStartTime;
	private long deschedulerDurationMs;
	private final Counter descheduledContainersCounter;
	private final Gauge deschedulerDurationMsGuage;
	private final TagGauge deschedulerReceivedGuage = new TagGauge.TagGaugeBuilder().setClearWhenFull(true).build();
	private final TagGauge deschedulerHandleGuage = new TagGauge.TagGaugeBuilder().setClearWhenFull(true).build();

	public YarnResourceManager(
		RpcService rpcService,
		String resourceManagerEndpointId,
		ResourceID resourceId,
		Configuration flinkConfig,
		Map<String, String> env,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		SlotManager slotManager,
		MetricRegistry metricRegistry,
		JobLeaderIdService jobLeaderIdService,
		ClusterInformation clusterInformation,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String webInterfaceUrl,
		JobManagerMetricGroup jobManagerMetricGroup,
		FailureRater failureRater) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			flinkConfig,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			jobManagerMetricGroup,
			failureRater);
		this.flinkConfig  = flinkConfig;
		this.yarnConfig = new YarnConfiguration();
		Utils.updateYarnConfigForJobManager(this.yarnConfig, this.flinkConfig);

		// hack fs.defaultFs in yanrConfiguration
		if (flinkConfig.contains(HdfsConfigOptions.HDFS_DEFAULT_FS)) {
			String defaultFS = flinkConfig.getString(HdfsConfigOptions.HDFS_DEFAULT_FS);
			if (defaultFS.length() > 0) {
				log.info("Using new fsDefault={}", defaultFS);
				yarnConfig.set(HdfsConfigOptions.HDFS_DEFAULT_FS.key(), defaultFS);
			}
		}

		blacklistReporter.addIgnoreExceptionClass(ExpectedContainerCompletedException.class);

		this.env = env;
		Utils.initRemoteFileStatusCache(this.yarnConfig, this.env);
		this.workerNodeMap = new ConcurrentHashMap<>();
		final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(
				YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

		final long yarnExpiryIntervalMS = yarnConfig.getLong(
				YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
				YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);

		if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
			log.warn("The heartbeat interval of the Flink Application master ({}) is greater " +
					"than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
					yarnHeartbeatIntervalMS, yarnExpiryIntervalMS);
		}
		yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;
		containerRequestHeartbeatIntervalMillis = flinkConfig.getInteger(YarnConfigOptions.CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS);
		numPendingContainerRequests = 0;

		containerStartDurationMaxMs = -1;

		if (flinkConfig.getBoolean(YarnConfigOptions.SLOW_CONTAINER_ENABLED)) {
			long slowContainerTimeoutMs = flinkConfig.getLong(YarnConfigOptions.SLOW_CONTAINER_TIMEOUT_MS);
			double slowContainersQuantile = flinkConfig.getDouble(YarnConfigOptions.SLOW_CONTAINERS_QUANTILE);
			Preconditions.checkArgument(
					slowContainersQuantile < 1.0,
					"%s must less than 1.0", YarnConfigOptions.SLOW_CONTAINERS_QUANTILE.key());
			double slowContainerThresholdFactor = flinkConfig.getDouble(YarnConfigOptions.SLOW_CONTAINER_THRESHOLD_FACTOR);
			Preconditions.checkArgument(
					slowContainerThresholdFactor > 1.0,
					"%s must great than 1.0", YarnConfigOptions.SLOW_CONTAINER_THRESHOLD_FACTOR.key());
			slowContainerManager = new SlowContainerManagerImpl(slowContainerTimeoutMs, slowContainersQuantile, slowContainerThresholdFactor);
		} else {
			slowContainerManager = new NoOpSlowContainerManager();
		}
		slowContainerCheckIntervalMs = flinkConfig.getLong(YarnConfigOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS);

		this.executor = Executors.newScheduledThreadPool(
			flinkConfig.getInteger(YarnConfigOptions.CONTAINER_LAUNCHER_NUMBER));

		this.webInterfaceUrl = webInterfaceUrl;
		this.numberOfTaskSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.defaultTaskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
		this.defaultCpus = flinkConfig.getDouble(YarnConfigOptions.VCORES, numberOfTaskSlots);
		this.resource = Resource.newInstance(defaultTaskManagerMemoryMB, 0, Utils.vCoresToMilliVcores(defaultCpus));

		this.slotsPerWorker = createWorkerSlotProfiles(flinkConfig);

		this.nmClientAsyncEnabled = flinkConfig.getBoolean(YarnConfigOptions.NMCLINETASYNC_ENABLED);

		this.gangSchedulerEnabled = flinkConfig.getBoolean(YarnConfigOptions.GANG_SCHEDULER);

		this.fatalOnGangFailed = true;

		this.yarnBlackedHosts = new HashSet<>();

		this.gangCurrentRetryTimes = 0;
		this.gangLastDowngradeTimestamp = -1;
		this.gangMaxRetryTimes = flinkConfig.getInteger(YarnConfigOptions.GANG_MAX_RETRY_TIMES);
		this.gangDowngradeTimeoutMilli = flinkConfig.getInteger(YarnConfigOptions.GANG_DOWNGRADE_TIMEOUT_MS);
		this.gangDowngradeOnFailed = flinkConfig.getBoolean(YarnConfigOptions.GANG_DOWNGRADE_ON_FAILED);
		this.nodeAttributesExpression = flinkConfig.getString(YarnConfigOptions.NODE_SATISFY_ATTRIBUTES_EXPRESSION);

		this.cleanupRunningContainersOnStop = flinkConfig.getBoolean(YarnConfigOptions.CLEANUP_RUNNING_CONTAINERS_ON_STOP);

		this.currentContainerId = System.getenv(ENV_YARN_CONTAINER_ID);

		this.deschedulerEnable = flinkConfig.getBoolean(YarnConfigOptions.GANG_CONTAINER_DESCHEDULER_ENABLE);
		this.deschedulerLoadTypeEnable = flinkConfig.getBoolean(YarnConfigOptions.GANG_CONTAINER_DESCHEDULER_LOAD_TYPE_ENABLE);
		this.deschedulerDiskTypeEnable = flinkConfig.getBoolean(YarnConfigOptions.GANG_CONTAINER_DESCHEDULER_DISK_TYPE_ENABLE);
		this.deschedulerPerfIntervalMinMs = flinkConfig.getLong(YarnConfigOptions.GANG_CONTAINER_DESCHEDULER_PERFORMANCE_TYPE_INTERVAL_MIN_MS);
		this.deschedulerUsabilityIntervalMinMs = flinkConfig.getLong(YarnConfigOptions.GANG_CONTAINER_DESCHEDULER_USABILITY_TYPE_INTERVAL_MIN_MS);
		this.deschedulerRequestTimeout = flinkConfig.getLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT) / 2;
		this.descheduledContainersCheckIntervalMs = deschedulerRequestTimeout / 10;
		this.deschedulerStartTime = 0L;
		this.descheduledContainers = new ArrayList<>();
		this.priorityConstraintTypes =  Arrays.asList(
			ConstraintType.NODE_DISK_HEALTHY_GUARANTEE,
			ConstraintType.NODE_SKIP_HIGH_LOAD);
		this.deschedulerCompleteLastTime = new HashMap<>();
		this.descheduledContainersCounter = new SimpleCounter();
		this.deschedulerDurationMsGuage = new Gauge() {
			@Override
			public Long getValue() {
				return deschedulerDurationMs;
			}
		};

		// smart resources
		smartResourcesStats = new SmartResourcesStats();
		boolean smartResourcesEnable = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_ENABLE_KEY,
			ConfigConstants.SMART_RESOURCES_ENABLE_DEFAULT);
		if (!smartResourcesEnable) {
			smartResourcesEnable = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_ENABLE_OLD_KEY,
				ConfigConstants.SMART_RESOURCES_ENABLE_DEFAULT);
		}

		this.yarnRuntimeConf = createRuntimeConfigurationWithQosLevel(flinkConfig.getString(YarnConfigOptions.YARN_RUNTIME_CONF_QOS_LEVEL));
		// The current yarn does not support the smart resources and the millesimal of the core
		// after turning on the CPU to bind the core
		if (this.yarnRuntimeConf.getQoSLevel() != RtQoSLevel.QOS_SHARE) {
			smartResourcesEnable = false;
		}

		srCpuAdjustDoubleEnable = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE_KEY,
			ConfigConstants.SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE_DEFAULT);
		Map<String, Object> smartResourcesConfig = new HashMap<>();
		smartResourcesConfig.put(ConfigConstants.SMART_RESOURCES_ENABLE_KEY, smartResourcesEnable);
		smartResourcesConfig.put(ConfigConstants.SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE_KEY,
			srCpuAdjustDoubleEnable);
		if (smartResourcesEnable) {
			this.pendingUpdating = new HashMap<>();
			this.waitNMUpdate = new HashMap<>();
			this.containerUpdateFailedTimesMap = new ConcurrentHashMap<>();
			this.skipUpdateContainersMap = new ConcurrentHashMap<>();
			String smartResourcesServiceName =
				flinkConfig.getString(ConfigConstants.SMART_RESOURCES_SERVICE_NAME_KEY, null);
			Preconditions.checkNotNull(smartResourcesServiceName, "SmartResources enabled and service name not set");
			this.region = flinkConfig.getString("dc", null);
			Preconditions.checkNotNull(this.region, "SmartResources enabled and get region failed");
			this.cluster = flinkConfig.getString("clusterName", null);
			Preconditions.checkNotNull(this.cluster, "SmartResources enabled and get cluster failed");
			this.applicationID = System.getenv("_APP_ID");
			Preconditions.checkNotNull(this.applicationID, "SmartResources enabled and get applicationID failed");
			String applicationNameWithUser = flinkConfig.getString("applicationName", null);
			Preconditions.checkNotNull(applicationNameWithUser, "SmartResources enabled and get applicationName failed");
			int idx = applicationNameWithUser.lastIndexOf("_");
			Preconditions.checkState(idx != -1, "SmartResources enabled and applicationName illegal, " + applicationNameWithUser);
			this.applicationName = applicationNameWithUser.substring(0, idx);
			this.estimaterClient = new EstimaterClient(smartResourcesServiceName);
			this.durtionMinutes = flinkConfig.getInteger(ConfigConstants.SMART_RESOURCES_DURTION_MINUTES_KEY,
				ConfigConstants.SMART_RESOURCES_DURTION_MINUTES_DEFAULT);
			if (this.durtionMinutes < ConfigConstants.SMART_RESOURCES_DURTION_MINUTES_MIN) {
				log.info("adjust smart-resources.durtion.minutes from {} to {}", this.durtionMinutes,
					ConfigConstants.SMART_RESOURCES_DURTION_MINUTES_MIN);
				this.durtionMinutes = ConfigConstants.SMART_RESOURCES_DURTION_MINUTES_MIN;
			}
			this.cpuReserveRatio = flinkConfig.getDouble(ConfigConstants.SMART_RESOURCES_CPU_RESERVE_RATIO,
				ConfigConstants.SMART_RESOURCES_CPU_RESERVE_RATIO_DEFAULT);
			this.memReserveRatio = flinkConfig.getDouble(ConfigConstants.SMART_RESOURCES_MEM_RESERVE_RATIO,
				ConfigConstants.SMART_RESOURCES_MEM_RESERVE_RATIO_DEFAULT);
			this.disableMemAdjust = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_DISABLE_MEM_ADJUST_KEY,
				ConfigConstants.SMART_RESOURCES_DISABLE_MEM_ADJUST_DEFAULT);
			this.srMemMaxMB = flinkConfig.getInteger(ConfigConstants.SMART_RESOURCES_MEM_MAX_MB_KEY,
				ConfigConstants.SMART_RESOURCES_MEM_MAX_MB_DEFAULT);
			this.srCpuEstimateMode = flinkConfig.getString(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_KEY,
				ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL);
			this.srAdjustCheckApi = flinkConfig.getString(ConfigConstants.SMART_RESOURCES_ADJUST_CHECK_API_KEY, "");
			Preconditions.checkState(validateSrAdjustCheckApi(srAdjustCheckApi), "Invalid sr check api, " + srAdjustCheckApi);
			this.srAdjustCheckBackoffMS = flinkConfig.getInteger(ConfigConstants.SMART_RESOURCES_ADJUST_CHECK_BACKOFF_MS_KEY,
				ConfigConstants.SMART_RESOURCES_ADJUST_CHECK_BACKOFF_MS_DEFAULT);
			this.srAdjustCheckTimeoutMS = flinkConfig.getInteger(ConfigConstants.SMART_RESOURCES_ADJUST_CHECK_TIMEOUT_MS_KEY,
				ConfigConstants.SMART_RESOURCES_ADJUST_CHECK_TIMEOUT_MS_DEFAULT);
			this.srNextCheckTimeMS = System.currentTimeMillis();
			log.info("SmartResources initialized, region: {}, cluster: {}, applicationID: {}, "
					+ "applicationName: {}, smartResourcesServiceName: {}, durtionMinutes: {}, "
					+ "cpuReserveRatio: {}, memReserveRatio: {}, disableMemAdjust: {}, memMaxMB: {}, "
					+ "cpuEstimateMode: {}, adjustCheckApi: {}, adjustCheckInterval: {}",
				this.region, this.cluster, this.applicationID, this.applicationName,
				smartResourcesServiceName, this.durtionMinutes, this.cpuReserveRatio,
				this.memReserveRatio, this.disableMemAdjust, this.srMemMaxMB, this.srCpuEstimateMode,
				this.srAdjustCheckApi, this.srAdjustCheckBackoffMS);

			this.containerResourcesUpdater = new Thread(this::containerResourcesUpdaterProc);
			this.containerResourcesUpdater.start();

			smartResourcesConfig.put(ConfigConstants.SMART_RESOURCES_SERVICE_NAME_KEY, smartResourcesServiceName);
			smartResourcesConfig.put("region", this.region);
			smartResourcesConfig.put("cluster", this.cluster);
			smartResourcesConfig.put("applicationID", this.applicationID);
			smartResourcesConfig.put("applicationName", this.applicationName);
			smartResourcesConfig.put("durtionMinutes", this.durtionMinutes);
			smartResourcesConfig.put("cpuReserveRatio", this.cpuReserveRatio);
			smartResourcesConfig.put("memReserveRatio", this.memReserveRatio);
			smartResourcesConfig.put("disableMemAdjust", this.disableMemAdjust);
			smartResourcesConfig.put("memMaxMB", this.srMemMaxMB);
			smartResourcesConfig.put("cpuEstimateMode", this.srCpuEstimateMode);
			smartResourcesConfig.put("adjustCheckApi", this.srAdjustCheckApi);
			smartResourcesConfig.put("adjustCheckBackoffMS", this.srAdjustCheckBackoffMS);
			smartResourcesConfig.put("adjustCheckTimeoutMS", this.srAdjustCheckTimeoutMS);
		}
		smartResourcesStats.setConfig(smartResourcesConfig);
	}

	@VisibleForTesting
	SlowContainerManager getSlowContainerManager() {
		return slowContainerManager;
	}

	@VisibleForTesting
	int getNumPendingContainerRequests() {
		return numPendingContainerRequests;
	}

	protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
			YarnConfiguration yarnConfiguration,
			int yarnHeartbeatIntervalMillis,
			@Nullable String webInterfaceUrl) throws Exception {
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(
			yarnHeartbeatIntervalMillis,
			this);

		resourceManagerClient.init(yarnConfiguration);
		resourceManagerClient.start();

		//TODO: change akka address to tcp host and port, the getAddress() interface should return a standard tcp address
		Tuple2<String, Integer> hostPort = parseHostPort(getAddress());

		final int restPort;

		if (webInterfaceUrl != null) {
			final int lastColon = webInterfaceUrl.lastIndexOf(':');

			if (lastColon == -1) {
				restPort = -1;
			} else {
				restPort = Integer.valueOf(webInterfaceUrl.substring(lastColon + 1));
			}
		} else {
			restPort = -1;
		}

		final RegisterApplicationMasterResponse registerApplicationMasterResponse =
			resourceManagerClient.registerApplicationMaster(hostPort.f0, restPort, webInterfaceUrl);
		getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			new RegisterApplicationMasterResponseReflector(log).getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		fatalOnGangFailed = containersFromPreviousAttempts.isEmpty();

		long ts = System.currentTimeMillis();
		for (final Container container : containersFromPreviousAttempts) {
			ResourceID resourceID = new ResourceID(container.getId().toString());
			workerNodeMap.put(resourceID, new YarnWorkerNode(container));
			slowContainerManager.containerAllocated(resourceID, ts, 0);
		}
	}

	protected NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		log.info("Using NMClient");
		NMClient nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(cleanupRunningContainersOnStop);
		return nodeManagerClient;
	}

	protected NMClientAsync createAndStartNodeManagerAsyncClient(YarnConfiguration yarnConfiguration) {
		log.info("Using NMClientAsync");
		// create the async client to communicate with the node managers
		NMClientAsync nodeManagerClientAsync = NMClientAsync.createNMClientAsync(this);
		nodeManagerClientAsync.init(yarnConfiguration);
		nodeManagerClientAsync.start();
		nodeManagerClientAsync.getClient().cleanupRunningContainersOnStop(cleanupRunningContainersOnStop);
		return nodeManagerClientAsync;
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		try {
			resourceManagerClient = createAndStartResourceManagerClient(
				yarnConfig,
				yarnHeartbeatIntervalMillis,
				webInterfaceUrl);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		if (nmClientAsyncEnabled) {
			nodeManagerClientAsync = createAndStartNodeManagerAsyncClient(yarnConfig);
		} else {
			nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);
		}
	}

	@Override
	protected void startServicesOnLeadership() {
		// let slot manager know the resources from previous attempt in case of JM failover
		final int numberWorkers = workerNodeMap.size() + numPendingContainerRequests;
		slotManager.receiveResources(numberWorkers, createWorkerSlotProfiles(flinkConfig, numberWorkers));

		super.startServicesOnLeadership();
		slowContainerManager.setYarnResourceManager(this);
		scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		if (deschedulerEnable) {
			log.info("start check descheduled containers with intervalMs: {}.", descheduledContainersCheckIntervalMs);
			scheduleRunAsync(this::checkDescheduledContainers, descheduledContainersCheckIntervalMs, TimeUnit.MILLISECONDS);
		} else {
			log.info("container descheduler is disabled.");
		}
	}

	@Override
	public CompletableFuture<Void> onStop() {
		// shut down all components
		Throwable firstException = null;

		if (containerResourcesUpdater != null) {
			containerResourcesUpdater.interrupt();
		}

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Throwable t) {
				firstException = t;
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		if (nodeManagerClientAsync != null) {
			try {
				log.info("Stop nodeManagerClientAsync.");
				nodeManagerClientAsync.stop();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		final CompletableFuture<Void> terminationFuture = super.onStop();

		if (firstException != null) {
			return FutureUtils.completedExceptionally(new FlinkException("Error while shutting down YARN resource manager", firstException));
		} else {
			return terminationFuture;
		}
	}

	@Override
	public void onStart() throws Exception {
		super.onStart();
		initTargetContainerResources();
		registerMetrics();
	}

	@Override
	protected void internalDeregisterApplication(
			ApplicationStatus finalStatus,
			@Nullable String diagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

		final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, appTrackingUrl);
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}

		Utils.deleteApplicationFiles(env);
	}

	@Override
	public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {
		if (!slotsPerWorker.iterator().next().isMatching(resourceProfile)) {
			return Collections.emptyList();
		}
		requestYarnContainer();
		return slotsPerWorker;
	}

	@Override
	public Collection<ResourceProfile> startNewWorkers(ResourceProfile resourceProfile, int resourceNumber) {
		if (!slotsPerWorker.iterator().next().isMatching(resourceProfile)) {
			return Collections.emptyList();
		}
		requestYarnContainers(resourceNumber);
		return createWorkerSlotProfiles(flinkConfig, resourceNumber);
	}

	@Override
	public Collection<ResourceProfile> initialWorkers(ResourceProfile resourceProfile, int workerNumber) {
		startNewWorkers(resourceProfile, workerNumber);
		return createWorkerSlotProfiles(flinkConfig, workerNumber);
	}

	@VisibleForTesting
	Resource getContainerResource() {
		return resource;
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode, int exitCode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}, exitCode {}.", container.getId(), exitCode);
		try {
			if (nmClientAsyncEnabled) {
				nodeManagerClientAsync.stopContainerAsync(container.getId(), container.getNodeId(), exitCode);
			} else {
				nodeManagerClient.stopContainer(container.getId(), container.getNodeId(), exitCode);
			}
		} catch (final Exception e) {
			log.warn("Error while calling YARN Node Manager to stop container", e);
		}
		resourceManagerClient.releaseAssignedContainer(container.getId());

		ResourceID resourceID = workerNode.getResourceID();
		removeContainer(resourceID);
		completedContainerGauge.addMetric(
				1,
				new TagGaugeStoreImpl.TagValuesBuilder()
						.addTagValue("container_host", workerNode.getContainer().getNodeId().getHost())
						.addTagValue("container_id", pruneContainerId(resourceID.getResourceIdString()))
						.addTagValue("exit_code", String.valueOf(exitCode))
						.build());
		return true;
	}

	@Override
	public void onBlacklistUpdated() {
		Map<String, HostFailure> newBlackedHosts = blacklistTracker.getBlackedHosts();

		Set<String> blacklistAddition = new HashSet<>(newBlackedHosts.keySet());
		blacklistAddition.removeAll(yarnBlackedHosts);

		Set<String> blacklistRemoval = new HashSet<>(yarnBlackedHosts);
		blacklistRemoval.removeAll(newBlackedHosts.keySet());

		yarnBlackedHosts.clear();
		yarnBlackedHosts.addAll(newBlackedHosts.keySet());
		resourceManagerClient.updateBlacklist(new ArrayList<>(blacklistAddition), new ArrayList<>(blacklistRemoval));
		log.info("BlacklistUpdated, yarnBlackedHosts: {}, blacklistAddition: {}, blacklistRemoval: {}",
				yarnBlackedHosts, blacklistAddition, blacklistRemoval);

		// release all blacked containers.
		newBlackedHosts.keySet().forEach(hostname -> {
			Set<ResourceID> resourceIDS = blacklistTracker.getBlackedResources(BlacklistUtil.FailureType.TASK, hostname);
			resourceIDS.forEach(resourceID -> {
				if (workerNodeMap.containsKey(resourceID)) {
					WorkerRegistration<YarnWorkerNode> registration = getTaskExecutors().get(resourceID);
					if (registration != null) {
						releaseResource(
								registration.getInstanceID(),
								new Exception("worker " + resourceID + " in blacklist."),
								WorkerExitCode.IN_BLACKLIST);
					}
				}
			});
		});
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID, TaskManagerLocation taskManagerLocation) {
		YarnWorkerNode workerNode = workerStarted(resourceID);
		slowContainerManager.containerStarted(resourceID, numPendingContainerRequests);
		containerStartDurationMaxMs = Math.max(
				containerStartDurationMaxMs,
				slowContainerManager.getContainerStartTime(resourceID));
		if (workerNode != null) {
			jobStartEventMessageSet.addMessage(new Message<>(new WarehouseJobStartEventMessage(
					WarehouseJobStartEventMessage.EVENT_MODULE_RESOURCE_MANAGER, currentContainerId, workerNode.getContainer().getId().toString(), null, 0, WarehouseJobStartEventMessage.EVENT_TYPE_START_CONTAINER, WarehouseJobStartEventMessage.EVENT_ACTION_FINISH)));
		}
		return workerNode;
	}

	// ------------------------------------------------------------------------
	//  AMRMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------

	@Override
	public float getProgress() {
		// Temporarily need not record the total size of asked and allocated containers
		return 1;
	}

	@Override
	public void onNotifyMsg(NotifyMsg msg) {
		if (msg.getNotifyMsgType() == NotifyMsgType.MSG_TYPE_GANG_SCHEDULE_FAILED) {
			log.info("Received MSG_TYPE_GANG_SCHEDULE_FAILED message, {}", msg);
			GangSchedulerNotifyContent gangSchedulerNotifyContent = msg.getNotifyContent().getGangSchedulerNotifyContent();
			gangFailedCounter.inc();
			if (!fatalOnGangFailed) {
				runAsync(() -> {
						if (numPendingContainerRequests >= gangSchedulerNotifyContent.getRequestedContainerNum()) {
							final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
							final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();
							for (int i = 0; i < gangSchedulerNotifyContent.getRequestedContainerNum() && pendingRequestsIterator.hasNext(); i++) {
								removeContainerRequest(pendingRequestsIterator.next(), false);
							}

							if (++gangCurrentRetryTimes > gangMaxRetryTimes) {
								if (gangDowngradeOnFailed) {
									// gang scheduler failed too many times, downgrade to fair scheduler.
									gangLastDowngradeTimestamp = System.currentTimeMillis();
								} else {
									String fatalMessage = "Request container by GangScheduler failed more than "
										+ gangCurrentRetryTimes + " times, " + gangSchedulerNotifyContent;
									onFatalError(new RuntimeException(fatalMessage));
								}
							}
							blacklistTracker.clearAll();
							scheduleRunAsync(
								this::recordFailureAndStartNewWorkerIfNeeded,
								flinkConfig.getInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_RETRY_MS),
								TimeUnit.MILLISECONDS);
						} else {
							log.info("Failed containerRequests:{} more than pending:{}. It is AMRMClient auto request, ignore.",
								gangSchedulerNotifyContent.getRequestedContainerNum(), numPendingContainerRequests);
						}
					}
				);
			} else {
				String fatalMessage = "Request new container by GangScheduler failed, " + gangSchedulerNotifyContent;
				onFatalError(fatalMessage, flinkConfig.getInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_FATAL_MS));
			}
		} else {
			log.info("Unknown onNotifyMsg type, {}", msg);
		}
	}

	@Override
	public void onContainersCompleted(final List<ContainerStatus> statuses) {
		runAsync(() -> {
				log.warn("YARN ResourceManager reported {} containers completed.", statuses.size());
				for (final ContainerStatus containerStatus : statuses) {
					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = removeContainer(resourceId);

					if (yarnWorkerNode != null) {
						log.error("Container {} on {} completed with exit code {}, {}",
								containerStatus.getContainerId(),
								yarnWorkerNode.getContainer().getNodeId().getHost(),
								containerStatus.getExitStatus(),
								containerStatus.getDiagnostics());

						ContainerCompletedException containerCompletedException = ContainerCompletedException.fromExitCode(
								containerStatus.getExitStatus(),
								containerStatus.getDiagnostics());

						recordFailureAndStartNewWorkerIfNeeded(
								yarnWorkerNode.getContainer().getNodeId().getHost(),
								resourceId,
								containerCompletedException);

						completedContainerGauge.addMetric(
								1,
								new TagGaugeStoreImpl.TagValuesBuilder()
										.addTagValue("container_host", yarnWorkerNode.getContainer().getNodeId().getHost())
										.addTagValue("container_id", pruneContainerId(resourceId.getResourceIdString()))
										.addTagValue("exit_code", String.valueOf(containerStatus.getExitStatus()))
										.build());
					}
					// Eagerly close the connection with task manager.
					closeTaskManagerConnection(resourceId, new Exception(containerStatus.getDiagnostics()), containerStatus.getExitStatus());
				}
			}
		);
	}

	@Override
	public void onContainersDescheduled(List<Container> receivedContainers) {
		runAsync(() -> {
			if (!deschedulerEnable) {
				log.warn("descheduler is not enable, but received descheduled containers.");
				return;
			}
			if (CollectionUtils.isEmpty(receivedContainers)) {
				log.warn("get descheduled containers empty list.");
				return;
			}
			String containerMsg = receivedContainers.stream().map(c -> c.getId().toString()).collect(Collectors.joining(","));
			log.info("received descheduled containers: {}, size: {}", containerMsg, receivedContainers.size());
			if (descheduledContainers.size() > 0) {
				log.info("already have descheduled containers in processing, nowSize: {}.", descheduledContainers.size());
				return;
			}

			// Filter containers and constraintType need handle by user config and priority
			Tuple2<ConstraintType, List<Container>> descheduledTuple2 = filterConstraintTypeAndContainers(receivedContainers);
			ConstraintType constraintType = descheduledTuple2.f0;
			List<Container> containers = descheduledTuple2.f1;
			if (constraintType == null || CollectionUtils.isEmpty(containers)) {
				log.info("no descheduled containers need handle after filter.");
				return;
			}

			// Check if descheduler in the interval limit.
			Long completeLastTime = 0L;
			if (deschedulerCompleteLastTime.containsKey(constraintType)) {
				completeLastTime = deschedulerCompleteLastTime.get(constraintType);
			}
			Long deschedulerIntervalMinMs;
			if (constraintType.equals(ConstraintType.NODE_DISK_HEALTHY_GUARANTEE)) {
				deschedulerIntervalMinMs = deschedulerUsabilityIntervalMinMs;
			} else {
				deschedulerIntervalMinMs = deschedulerPerfIntervalMinMs;
			}
			if (System.currentTimeMillis() < completeLastTime + deschedulerIntervalMinMs) {
				log.info("this deschedulerType: {} is in interval limit, last completeTime: {}, intervalMinMs: {}",
					constraintType.name(), completeLastTime, deschedulerIntervalMinMs);
				return;
			}

			for (Container container : containers) {
				descheduledContainers.add(container);
			}
			descheduledConstraintType = constraintType;
			descheduledContainersCounter.inc(containers.size());
			deschedulerStartTime = System.currentTimeMillis();

			try {
				log.info("request new containers for release descheduled containers.");
				requestExtraTaskManagers(containers.size());
			} catch (ResourceManagerException ex) {
				log.warn("request extra containers from resourceManager error, cancel descheduler process.", ex);
				deschedulerFailedCounterInc("requestResourceError");
				descheduledContainers.clear();
			}
		});
	}

	private Tuple2<ConstraintType, List<Container>> filterConstraintTypeAndContainers(List<Container> receivedContainers) {
		Map<ConstraintType, List<Container>> constraintContainerMap = new HashMap<>();
		for (Container container : receivedContainers) {
			log.info("descheduled container: {}, host: {}.", container.getId(), container.getNodeHttpAddress());
			List<DeschedulerResult> deschedulerResults = container.getDeschedulerResults().getHardDeschedulerResult();
			for (DeschedulerResult deschedulerResult : deschedulerResults) {
				deschedulerReceivedCounterInc(deschedulerResult);
				ConstraintType constraintType = deschedulerResult.getGlobalConstraint().getConstraintType();
				log.info("descheduler type: {}, content: {}.", constraintType, deschedulerResult.getDeschedulerResultContent());
				if (constraintContainerMap.containsKey(constraintType)) {
					constraintContainerMap.get(constraintType).add(container);
					continue;
				}
				if ((constraintType.equals(ConstraintType.NODE_SKIP_HIGH_LOAD) && deschedulerLoadTypeEnable)
					|| (constraintType.equals(ConstraintType.NODE_DISK_HEALTHY_GUARANTEE) && deschedulerDiskTypeEnable)) {
					List<Container> constraintContainerList = new ArrayList<>();
					constraintContainerList.add(container);
					constraintContainerMap.put(constraintType, constraintContainerList);
				} else {
					log.info("constraint type {} is not enable or support.", constraintType);
				}
			}
		}
		if (constraintContainerMap.isEmpty()) {
			return new Tuple2<>(null, Collections.emptyList());
		}
		// Filter by constraintType priority
		for (ConstraintType constraintType : priorityConstraintTypes) {
			if (constraintContainerMap.containsKey(constraintType)) {
				log.info("descheduler constraintType is {} after filter.", constraintType);
				return new Tuple2<>(constraintType, constraintContainerMap.get(constraintType));
			}
		}
		return new Tuple2<>(null, Collections.emptyList());
	}

	private void deschedulerReceivedCounterInc(DeschedulerResult deschedulerResult) {
		GlobalConstraint globalConstraint = deschedulerResult.getGlobalConstraint();
		ConstraintType constraintType = globalConstraint.getConstraintType();
		TagGaugeStoreImpl.TagValuesBuilder tagValueBuilder = new TagGaugeStoreImpl.TagValuesBuilder()
			.addTagValue("constraintType", constraintType.name());
		if (constraintType.equals(ConstraintType.NODE_SKIP_HIGH_LOAD)) {
			DeschedulerNodeSkipHighLoadContent loadContent = deschedulerResult.getDeschedulerResultContent().getDeschedulerNodeSkipHighLoadContent();
			tagValueBuilder.addTagValue("maxLoadCoreRatioLastMin", String.valueOf(
				globalConstraint.getConstraintContent().getNodeSkipHighLoadContent().getMaxLoadCoreRatioLastMin()));
			tagValueBuilder.addTagValue("currentNodeLoadAvgLastMin", String.valueOf(loadContent.getCurrentNodeLoadAvgLastMin()));
			tagValueBuilder.addTagValue("currentContainerLoad", String.valueOf(loadContent.getCurrentContainerLoad()));
		}
		deschedulerReceivedGuage.addMetric(1, tagValueBuilder.build());
	}

	private void deschedulerFailedCounterInc(String reason) {
		deschedulerHandleGuage.addMetric(
			1,
			new TagGaugeStoreImpl.TagValuesBuilder()
				.addTagValue("constraintType", descheduledConstraintType.name())
				.addTagValue("resultType", "failed")
				.addTagValue("reason", reason)
				.build());
	}

	private void deschedulerSuccessCounterInc() {
		deschedulerHandleGuage.addMetric(
			1,
			new TagGaugeStoreImpl.TagValuesBuilder()
				.addTagValue("constraintType", descheduledConstraintType.name())
				.addTagValue("resultType", "success")
				.build());
	}

	private int getDescuedulerExitCode(Container container) {
		List<DeschedulerResult> deschedulerResults = container.getDeschedulerResults().getHardDeschedulerResult();
		if (deschedulerResults != null && deschedulerResults.size() > 0) {
			ConstraintType constraintType = null;
			if (deschedulerResults.size() == 1) {
				constraintType = deschedulerResults.get(0).getGlobalConstraint().getConstraintType();
			} else {
				Set<ConstraintType> constraintTypes = new HashSet<>();
				for (DeschedulerResult deschedulerResult : deschedulerResults) {
					constraintTypes.add(deschedulerResult.getGlobalConstraint().getConstraintType());
				}
				for (ConstraintType priorityConstraintType : priorityConstraintTypes) {
					if (constraintTypes.contains(priorityConstraintType)) {
						constraintType = priorityConstraintType;
					}
				}
			}
			if (constraintType == null) {
				return ContainerExitStatus.KILLED_BY_DESCHEDULER;
			}
			if (constraintType.equals(ConstraintType.NODE_DISK_HEALTHY_GUARANTEE)) {
				return ContainerExitStatus.KILLED_BY_DESCHEDULER_WHEN_DISK_FAILED;
			} else if (constraintType.equals(ConstraintType.NODE_SKIP_HIGH_LOAD)) {
				return ContainerExitStatus.KILLED_BY_DESCHEDULER_WHEN_EXCEEDED_LOAD_RATIO;
			}
		}
		return ContainerExitStatus.KILLED_BY_DESCHEDULER;
	}

	private void checkDescheduledContainersInternal() {
		if (descheduledContainers.size() <= 0) {
			return;
		}
		if (System.currentTimeMillis() > deschedulerStartTime + deschedulerRequestTimeout) {
			log.warn("request new resources for descheduler time out, outTime: {}, descheduled contaienr size: {}, now extra task manager size: {}.",
				deschedulerRequestTimeout, descheduledContainers.size(), getNumberExtraRegisteredTaskManagers());
			descheduledContainers.clear();
			reduceExtraTaskManagers(descheduledContainers.size());
			deschedulerFailedCounterInc("requestResourceTimeOut");
			return;
		}
		if (getNumberExtraRegisteredTaskManagers() < descheduledContainers.size()) {
			log.info("extra task manager is not ready for descheduler, need: {}, now: {}.", descheduledContainers.size(), getNumberExtraRegisteredTaskManagers());
			return;
		}
		log.info("already get extra registered task manager: {}, descheduled container: {}, start to release descheduled containers.",
			getNumberExtraRegisteredTaskManagers(), descheduledContainers.size());
		int descheduledSize = descheduledContainers.size();
		// reduce extra task manager num first, avoid request new containers after release descheduled containers.
		reduceExtraTaskManagers(descheduledSize);
		try {
			for (Container container : descheduledContainers) {
				log.info("start to release descheduled container {}, host: {}.", container.getId(), container.getNodeHttpAddress());
				ResourceID resourceID = new ResourceID(container.getId().toString());
				int exitCode = getDescuedulerExitCode(container);
				releaseResource(resourceID, new Exception("this container need descheduler to keep constraint, exitCode:" + exitCode), exitCode);
				log.info("already release descheduled container {}, host: {}.", container.getId(), container.getNodeHttpAddress());
			}
			deschedulerCompleteLastTime.put(descheduledConstraintType, System.currentTimeMillis());
			deschedulerSuccessCounterInc();
			deschedulerDurationMs = System.currentTimeMillis() - deschedulerStartTime;
			log.info("handle descheduled containers success, size: {}, durationMs: {}.", descheduledSize, deschedulerDurationMs);
		} catch (Exception ex) {
			log.warn("handle descheduler error.", ex);
			deschedulerFailedCounterInc("releaseResourceError");
		} finally {
			descheduledContainers.clear();
		}
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		fatalOnGangFailed = false;
		runAsync(() -> {
			final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
			final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();
			log.info("Allocated {} containers.", containers.size());

			for (Container container : containers) {
				log.info(
					"Received new container: {} on {} - Remaining pending container requests: {}",
					container.getId(),
					container.getNodeId().getHost(),
					numPendingContainerRequests);

				if (numPendingContainerRequests > 0) {
					removeContainerRequest(pendingRequestsIterator.next());

					if (yarnBlackedHosts.contains(container.getNodeId().getHost())) {
						log.info("containers {} on blacked host: {}, release it.",
								container.getId(), container.getNodeId().getHost());
						resourceManagerClient.releaseAssignedContainer(container.getId());
						startNewWorkerIfNeeded();
						continue;
					}

					final String containerIdStr = container.getId().toString();
					final ResourceID resourceId = new ResourceID(containerIdStr);

					YarnWorkerNode yarnWorkerNode = new YarnWorkerNode(container);
					workerNodeMap.put(resourceId, yarnWorkerNode);

					slowContainerManager.containerAllocated(resourceId, System.currentTimeMillis(), numPendingContainerRequests);
					executor.execute(new Runnable() {
						@Override
						public void run() {
							boolean isStartContainerError = true;
							try {
								ContainerLaunchContext taskExecutorLaunchContext;
								try {
									jobStartEventMessageSet.addMessage(new Message<>(new WarehouseJobStartEventMessage(
											WarehouseJobStartEventMessage.EVENT_MODULE_RESOURCE_MANAGER, currentContainerId, containerIdStr, null, 0, WarehouseJobStartEventMessage.EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT, WarehouseJobStartEventMessage.EVENT_ACTION_START)));
									long ts = System.currentTimeMillis();
									// Context information used to start a TaskExecutor Java process
									taskExecutorLaunchContext = createTaskExecutorLaunchContext(
											container.getResource(),
											containerIdStr,
											container.getNodeId().getHost());
									log.info("Create context for container {} take {} milliseconds",
											containerIdStr,
											System.currentTimeMillis() - ts);
									jobStartEventMessageSet.addMessage(new Message<>(new WarehouseJobStartEventMessage(
											WarehouseJobStartEventMessage.EVENT_MODULE_RESOURCE_MANAGER, currentContainerId, containerIdStr, null, 0, WarehouseJobStartEventMessage.EVENT_TYPE_CREATE_TASK_MANAGER_CONTEXT, WarehouseJobStartEventMessage.EVENT_ACTION_FINISH)));
								} catch (Throwable t) {
									isStartContainerError = false;
									throw t;
								}
								jobStartEventMessageSet.addMessage(new Message<>(new WarehouseJobStartEventMessage(
										WarehouseJobStartEventMessage.EVENT_MODULE_RESOURCE_MANAGER, currentContainerId, containerIdStr, null, 0, WarehouseJobStartEventMessage.EVENT_TYPE_START_CONTAINER, WarehouseJobStartEventMessage.EVENT_ACTION_START)));
								if (nmClientAsyncEnabled) {
									nodeManagerClientAsync.startContainerAsync(container, taskExecutorLaunchContext);
								} else {
									nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
								}
							} catch (Throwable t) {
								log.error("Could not start TaskManager in container {}.", container.getId(), t);

								// release the failed container
								removeContainer(resourceId);
								resourceManagerClient.releaseAssignedContainer(container.getId());
								if (isStartContainerError) {
									recordFailureAndStartNewWorkerIfNeeded(
											yarnWorkerNode.getContainer().getNodeId().getHost(),
											resourceId,
											t);
								} else {
									recordFailureAndStartNewWorkerIfNeeded();
								}
							}
						}
					});
				} else {
					// return the excessive containers
					log.info("Returning excess container {}.", container.getId());
					resourceManagerClient.releaseAssignedContainer(container.getId());
				}
			}

			// if we are waiting for no further containers, we can go to the
			// regular heartbeat interval
			if (numPendingContainerRequests <= 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		});
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
		removeContainerRequest(pendingContainerRequest, true);
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest, boolean enableLog) {
		numPendingContainerRequests--;

		if (enableLog) {
			log.info("Removing container request {}. Pending container requests {}.", pendingContainerRequest, numPendingContainerRequests);
		}

		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequests() {
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests = resourceManagerClient.getMatchingRequests(
			RM_REQUEST_PRIORITY,
			ResourceRequest.ANY,
			getContainerResource());

		final Collection<AMRMClient.ContainerRequest> matchingContainerRequests;

		if (matchingRequests.isEmpty()) {
			matchingContainerRequests = Collections.emptyList();
		} else {
			final Collection<AMRMClient.ContainerRequest> collection = matchingRequests.get(0);
			matchingContainerRequests = new ArrayList<>(collection);
		}

		Preconditions.checkState(
			matchingContainerRequests.size() == numPendingContainerRequests,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests has diverged. Number client's pending container requests %s != Number RM's pending container requests %s.", matchingContainerRequests.size(), numPendingContainerRequests);

		return matchingContainerRequests;
	}

	@Override
	public void onShutdownRequest() {
		log.info("Received onShutdownRequest from Yarn ResourceManager.");
		onFatalError(new ResourceManagerException(ERROR_MASSAGE_ON_SHUTDOWN_REQUEST));
	}

	@Override
	public void onNodesUpdated(List<NodeReport> list) {
		// We are not interested in node updates
	}

	@Override
	public void onError(Throwable error) {
		log.info("Received onError from Yarn ResourceManager, ", error);
		onFatalError(error);
	}

	@Override
	public void onContainersUpdated(List<UpdatedContainer> containers) {
		runAsync(() -> {
			log.info("Received ContainersUpdate {}", containers);
			containersUpdated(containers);
		});
	}

	@Override
	public void onContainersUpdateError(List<UpdateContainerError> updateContainerErrors) {
		runAsync(() -> {
			log.error("Received ContainersUpdateError {}", updateContainerErrors);
			containersUpdateError(updateContainerErrors);
		});
	}


	// ------------------------------------------------------------------------
	//  Utility methods
	// ------------------------------------------------------------------------

	/**
	 * Converts a Flink application status enum to a YARN application status enum.
	 * @param status The Flink application status.
	 * @return The corresponding YARN application status.
	 */
	private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
		if (status == null) {
			return FinalApplicationStatus.UNDEFINED;
		}
		else {
			switch (status) {
				case SUCCEEDED:
					return FinalApplicationStatus.SUCCEEDED;
				case FAILED:
					return FinalApplicationStatus.FAILED;
				case CANCELED:
					return FinalApplicationStatus.KILLED;
				default:
					return FinalApplicationStatus.UNDEFINED;
			}
		}
	}

	// parse the host and port from akka address,
	// the akka address is like akka.tcp://flink@100.81.153.180:49712/user/$a
	private static Tuple2<String, Integer> parseHostPort(String address) {
		String[] hostPort = address.split("@")[1].split(":");
		String host = hostPort[0];
		String port = hostPort[1].split("/")[0];
		return new Tuple2<>(host, Integer.valueOf(port));
	}

	private void recordFailureAndStartNewWorkerIfNeeded() {
		recordWorkerFailure();
		// and ask for new workers.
		startNewWorkerIfNeeded();
	}

	private void recordFailureAndStartNewWorkerIfNeeded(String hostname, ResourceID resourceID, Throwable cause) {
		recordWorkerFailure(hostname, resourceID, cause);
		// and ask for new workers.
		startNewWorkerIfNeeded();
	}

	private void startNewWorkerIfNeeded() {
		if (slowContainerManager instanceof SlowContainerManagerImpl) {
			log.info("Try to startNewWorkerIfNeeded, starting container size: {}, number pending requests: {}.",
					slowContainerManager.getStartingContainerSize(), numPendingContainerRequests);

			int numberRequestedNotStartedWorkers = slowContainerManager.getStartingContainerSize() + numPendingContainerRequests;
			int numberRequestedNotStartedRedundantWorkers = slowContainerManager.getStartingRedundantContainerSize() + slowContainerManager.getPendingRedundantContainersNum();
			int numberStartedRedundantWorkers = slowContainerManager.getTotalRedundantContainersNum() - numberRequestedNotStartedRedundantWorkers;

			startNewWorkerIfNeeded(
					ResourceProfile.UNKNOWN,
					numberOfTaskSlots,
					numberRequestedNotStartedWorkers,
					numberStartedRedundantWorkers,
					numberRequestedNotStartedRedundantWorkers);
		} else {
			startNewWorkerIfNeeded(
					ResourceProfile.UNKNOWN,
					numPendingContainerRequests,
					numberOfTaskSlots,
					workerNodeMap.size());
		}
	}

	private void requestYarnContainer() {
		requestYarnContainers(1);
	}

	private void requestYarnContainers(int containerNumber) {
		boolean useGang = gangSchedulerEnabled;
		if (gangSchedulerEnabled) {
			if (System.currentTimeMillis() - gangLastDowngradeTimestamp < gangDowngradeTimeoutMilli) {
				useGang = false;
				gangDowngradeCounter.inc();
			} else if (gangLastDowngradeTimestamp > 0) {
				gangCurrentRetryTimes = 0;
				gangLastDowngradeTimestamp = -1;
			}
		}
		log.info("Allocate {} containers, Using gang scheduler: {}", containerNumber, useGang);
		ArrayList<AMRMClient.ContainerRequest> containerRequests = new ArrayList<>();
		for (int i = 0; i < containerNumber; i++) {
			AMRMClient.ContainerRequest containerRequest = getContainerRequest(useGang);
			log.debug("Add container request: {}", containerRequest);
			containerRequests.add(containerRequest);
		}
		resourceManagerClient.addContainerRequestList(containerRequests);
		numPendingContainerRequests += containerNumber;
		log.info("Requesting new TaskExecutor container with resources {}. Number pending requests {}.",
			resource,
			numPendingContainerRequests);

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
	}

	@Nonnull
	@VisibleForTesting
	AMRMClient.ContainerRequest getContainerRequest() {
		return getContainerRequest(false);
	}

	private AMRMClient.ContainerRequest getContainerRequest(boolean gang) {
		AMRMClient.ContainerRequest containerRequest;
		if (gang) {
			List<GlobalConstraint> hardConstraints = new ArrayList<>();
			// ----set hard constraints----
			if (flinkConfig.getFloat(YarnConfigOptions.GANG_NODE_SKIP_HIGH_LOAD) > 0) {
				// ----skip high load----
				GlobalConstraint load = GlobalConstraint.newInstance(ConstraintType.NODE_SKIP_HIGH_LOAD);
				NodeSkipHighLoadContent nodeSkipHighLoadContent = NodeSkipHighLoadContent.newInstance(
					flinkConfig.getFloat(YarnConfigOptions.GANG_NODE_SKIP_HIGH_LOAD));
				ConstraintContent cc = ConstraintContent.newInstance();
				cc.setNodeSkipHighLoadContent(nodeSkipHighLoadContent);
				load.setConstraintContent(cc);
				if (deschedulerEnable) {
					load.setIsDeschedulerEnabled(deschedulerLoadTypeEnable);
				}
				hardConstraints.add(load);
			}

			if (!StringUtils.isNullOrWhitespaceOnly(nodeAttributesExpression)) {
				// ----select node with attributes----
				GlobalConstraint attributes = GlobalConstraint.newInstance(ConstraintType.NODE_SATISFY_ATTRIBUTES_EXPRESSION);
				NodeSatisfyAttributesContent attributesContent = NodeSatisfyAttributesContent.newInstance(nodeAttributesExpression);
				ConstraintContent cc1 = ConstraintContent.newInstance();
				cc1.setNodeSatisfyAttributesContent(attributesContent);
				attributes.setConstraintContent(cc1);
				hardConstraints.add(attributes);
			}

			// ----disk healthy guarantee----
			if (deschedulerEnable) {
				GlobalConstraint disk = GlobalConstraint.newInstance(ConstraintType.NODE_DISK_HEALTHY_GUARANTEE);
				disk.setIsDeschedulerEnabled(deschedulerDiskTypeEnable);
				hardConstraints.add(disk);
			}

			// ----set soft constraints----
			List<GlobalConstraint> softConstraints = new ArrayList<>();
			if (flinkConfig.getInteger(YarnConfigOptions.GANG_CONTAINER_DECENTRALIZED_AVERAGE_WEIGHT) > 0) {
				// ----set decentralize container----
				GlobalConstraint decentralizeContainer = GlobalConstraint.newInstance(
					ConstraintType.GLOBAL_MAKE_CONTAINER_DECENTRALIZED_AVERAGE);
				decentralizeContainer.setConstraintWeight(
					flinkConfig.getInteger(YarnConfigOptions.GANG_CONTAINER_DECENTRALIZED_AVERAGE_WEIGHT));
				softConstraints.add(decentralizeContainer);
			}

			if (flinkConfig.getInteger(YarnConfigOptions.GANG_NODE_QUOTA_USAGE_AVERAGE_WEIGHT) > 0) {
				// ----set quota avg----
				GlobalConstraint quotaAvg = GlobalConstraint.newInstance(
					ConstraintType.GLOBAL_MAKE_NODE_QUOTA_USAGE_AVERAGE);
				quotaAvg.setConstraintWeight(
					flinkConfig.getInteger(YarnConfigOptions.GANG_NODE_QUOTA_USAGE_AVERAGE_WEIGHT));
				softConstraints.add(quotaAvg);
			}

			GlobalConstraints globalConstraints = GlobalConstraints.newInstance(hardConstraints, softConstraints);
			containerRequest = new AMRMClient.ContainerRequest(getContainerResource(), null, null, RM_REQUEST_PRIORITY,
				0, true, null, null, ExecutionTypeRequest.newInstance(),
				SchedulerType.GANG_SCHEDULER, false, globalConstraints, null, yarnRuntimeConf);
		} else {
			containerRequest = new AMRMClient.ContainerRequest(getContainerResource(), null, null, RM_REQUEST_PRIORITY,
				0, true, null, nodeAttributesExpression, ExecutionTypeRequest.newInstance(),
				SchedulerType.FAIR_SCHEDULER, true, null, null, yarnRuntimeConf);
		}
		return containerRequest;
	}

	private RuntimeConfiguration createRuntimeConfigurationWithQosLevel(String yarnRuntimeConfQosLevel) {
		RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.newInstance();
			switch (yarnRuntimeConfQosLevel.toLowerCase()) {
				case "reserved":
					runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_RESERVED);
					break;
				case "share":
					runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_SHARE);
					break;
				case "any":
					runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_ANY);
					break;
				default:
					log.warn("QosLevel is {} which not match any mode, therefore QosLevel use default share mode", yarnRuntimeConf);
					runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_SHARE);
			}
		return runtimeConfiguration;
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		double containerVcores = resource.getVirtualCoresMilli() / 1000.0;

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), containerVcores, numberOfTaskSlots);

		log.debug("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
				containerId,
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB());

		Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerConfig,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_CONTAINER_ID, containerId);
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}

	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
		log.info("Started TaskManager in container {} {}.", containerId, map);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		log.info("Received container status {} {}.", containerId, containerStatus);
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		log.info("Stopped container {}.", containerId);
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable throwable) {
		runAsync(() -> {
			log.error("Could not start TaskManager in container {}.", containerId, throwable);
			// release the failed container
			ResourceID resourceID = new ResourceID(containerId.toString());
			final YarnWorkerNode yarnWorkerNode = removeContainer(resourceID);
			if (yarnWorkerNode != null) {
				completedContainerGauge.addMetric(
								1,
								new TagGaugeStoreImpl.TagValuesBuilder()
										.addTagValue("container_host", yarnWorkerNode.getContainer().getNodeId().getHost())
										.addTagValue("container_id", pruneContainerId(resourceID.getResourceIdString()))
										.addTagValue("exit_code", String.valueOf(WorkerExitCode.START_CONTAINER_ERROR))
										.build());
				resourceManagerClient.releaseAssignedContainer(containerId);
				recordFailureAndStartNewWorkerIfNeeded(
						yarnWorkerNode.getContainer().getNodeId().getHost(),
						resourceID,
						throwable);
			}
		});
	}

	@Override
	public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
		log.info("Update container resource success {}", containerId);
		Container updatedContainer = waitNMUpdate.get(containerId);
		Container oldContainer = workerNodeMap.get(getResourceID(updatedContainer)).getContainer();
		workerNodeMap.put(getResourceID(updatedContainer), new YarnWorkerNode(updatedContainer));
		waitNMUpdate.remove(containerId);
		log.info("[NM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
				containerId, oldContainer.getResource().getMemorySize(),
				oldContainer.getResource().getVirtualCoresDecimal(),
				updatedContainer.getResource().getMemorySize(),
				updatedContainer.getResource().getVirtualCoresDecimal());
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
		log.error("Get container status error {}", containerId, throwable);
	}

	@Override
	public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
		log.error("Update container resource error {}", containerId, t);
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable throwable) {
		log.error("Stop container error {}", containerId, throwable);
	}

	// ------------------------------------------------------------------------
	//	Smart Resources
	// ------------------------------------------------------------------------

	private void containersUpdated(List<UpdatedContainer> updatedContainers) {
		for (UpdatedContainer updatedContainer : updatedContainers) {
			// remove containerId from skipUpdateContainersMap while receiving update success response from yarn RM
			if (skipUpdateContainersMap.containsKey(updatedContainer.getContainer().getId())) {
				skipUpdateContainersMap.remove(updatedContainer.getContainer().getId());
			}

			if (!workerNodeMap.containsKey(getResourceID(updatedContainer.getContainer()))) {
				log.info("Container {} resources was updated but container has complete",
					updatedContainer.getContainer().getId());
				continue;
			}

			try {
				Container old = workerNodeMap.get(getResourceID(updatedContainer.getContainer())).getContainer();
				double oldContainerVCores = old.getResource().getVirtualCoresDecimal();
				double newContainerVCores = updatedContainer.getContainer().getResource().getVirtualCoresDecimal();
				log.info("[RM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
					updatedContainer.getContainer().getId(),
					old.getResource().getMemory(),
					oldContainerVCores,
					updatedContainer.getContainer().getResource().getMemory(),
					newContainerVCores);
				if (nmClientAsyncEnabled) {
					nodeManagerClientAsync.updateContainerResourceAsync(updatedContainer.getContainer());
					waitNMUpdate.put(updatedContainer.getContainer().getId(), updatedContainer.getContainer());
				} else {
					nodeManagerClient.updateContainerResource(updatedContainer.getContainer());
					workerNodeMap.put(getResourceID(updatedContainer.getContainer()),
							new YarnWorkerNode(updatedContainer.getContainer()));
					log.info("[NM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
							old.getId(),
							old.getResource().getMemory(),
							oldContainerVCores,
							updatedContainer.getContainer().getResource().getMemory(),
							newContainerVCores);
				}
			} catch (YarnException | IOException e) {
				log.error("update container resources error, "
					+ updatedContainer.getContainer().getId(), e);
			} catch (Throwable t) {
				log.error("update container resources error, "
					+ updatedContainer.getContainer().getId(), t);
			} finally {
				pendingUpdating.remove(updatedContainer.getContainer().getId());
			}
		}
	}

	private void containersUpdateError(List<UpdateContainerError> updateContainerErrors) {
		for (UpdateContainerError updateContainerError : updateContainerErrors) {
			log.error("Container {} resources update failed, reason: {}",
				updateContainerError.getUpdateContainerRequest().getContainerId(),
				updateContainerError.getReason());
			pendingUpdating.remove(updateContainerError.getUpdateContainerRequest().getContainerId());

			if (updateContainerError.getReason().equals("INCORRECT_CONTAINER_VERSION_ERROR")) {
				ResourceID resourceID = new ResourceID(updateContainerError.getUpdateContainerRequest().getContainerId().toString());
				Container currentContainer = workerNodeMap.get(resourceID).getContainer();
				if (currentContainer != null) {
					log.info("Container {} version updated, {} -> {}",
						currentContainer.getId(),
						currentContainer.getVersion(),
						updateContainerError.getCurrentContainerVersion());
					currentContainer.setVersion(updateContainerError.getCurrentContainerVersion());
				} else {
					log.info("Container {} has been released",
						updateContainerError.getUpdateContainerRequest().getContainerId());
				}
			} else {
				int containerUpdateFailedTimes = containerUpdateFailedTimesMap
					.getOrDefault(updateContainerError.getUpdateContainerRequest().getContainerId(),
						0) + 1;
				if (containerUpdateFailedTimes >= CONTAINER_MAX_RETRY_TIMES) {
					// add container to updateSkipList
					skipUpdateContainersMap
						.put(updateContainerError.getUpdateContainerRequest().getContainerId(),
							System.currentTimeMillis() + CONTAINER_SKIP_UPDATE_TIME_MS);
					containerUpdateFailedTimesMap
						.remove(updateContainerError.getUpdateContainerRequest().getContainerId());
				} else {
					// update container update failed times
					containerUpdateFailedTimesMap
						.put(updateContainerError.getUpdateContainerRequest().getContainerId(),
							containerUpdateFailedTimes);
				}
			}
		}
	}

	private void updateContainersResources(UpdateContainersResources updateContainersResources) {
		log.debug("Receive update resources req: {}", updateContainersResources);
		ContainerResources newResources = new ContainerResources(updateContainersResources.getMemoryMB(),
			updateContainersResources.getVcores());
		if (updateContainersResources.getDurtionMinutes() < this.durtionMinutes) {
			if (targetResources.getMemoryMB() > newResources.getMemoryMB()) {
				newResources.setMemoryMB(targetResources.getMemoryMB());
			}
			if (targetResources.getVcores() > newResources.getVcores()) {
				newResources.setVcores(targetResources.getVcores());
			}
		}

		if (disableMemAdjust) {
			newResources.setMemoryMB(targetResources.getMemoryMB());
		}

		if (!newResources.equals(targetResources)) {
			if (!StringUtils.isNullOrWhitespaceOnly(srAdjustCheckApi)) {
				if (System.currentTimeMillis() < srNextCheckTimeMS) {
					log.info("Resources update check was limited, need later then: {}", srNextCheckTimeMS);
				} else {
					boolean updated = false;
					// Check memory
					if (newResources.getMemoryMB() != targetResources.getMemoryMB()) {
						if (!checkIfCouldUpdateResources(new ContainerResources(newResources.getMemoryMB(), targetResources.getVcores()))) {
							log.warn("Container memory update was rejected by sr check api, original: {} MB, target: {} MB",
								targetResources.getMemoryMB(), newResources.getMemoryMB());
						} else {
							log.info("Container memory updated from {} MB to {} MB", targetResources.getMemoryMB(), newResources.getMemoryMB());
							targetResources.setMemoryMB(newResources.getMemoryMB());
							updated = true;
						}
					}

					// Check vcores
					if (newResources.getVcores() != targetResources.getVcores()) {
						if (!checkIfCouldUpdateResources(new ContainerResources(targetResources.getMemoryMB(), newResources.getVcores()))) {
							log.warn("Container vcores update was rejected by sr check api, original: {} vcore, target: {} vcore",
								targetResources.getVcores(), newResources.getVcores());
						} else {
							log.info("Container vcores updated from {} vcore to {} vcore", targetResources.getVcores(), newResources.getVcores());
							targetResources.setVcores(newResources.getVcores());
							updated = true;
						}
					}

					if (updated) {
						smartResourcesStats.updateCurrentResources(
							new SmartResourcesStats.Resources(targetResources.getMemoryMB(), targetResources.getVcores()));
					} else {
						srNextCheckTimeMS = System.currentTimeMillis() + srAdjustCheckBackoffMS;
					}
				}
			} else {
				log.info("Container resources updated from {} to {}", targetResources,
					newResources);
				targetResources = newResources;
				smartResourcesStats.updateCurrentResources(
					new SmartResourcesStats.Resources(targetResources.getMemoryMB(), targetResources.getVcores()));
			}

		}

		for (Map.Entry<ResourceID, YarnWorkerNode> entry : workerNodeMap.entrySet()){
			Container container = entry.getValue().getContainer();
			ContainerId containerId = container.getId();

			if (pendingUpdating.containsKey(containerId) &&
				pendingUpdating.get(containerId) < System.currentTimeMillis()) {
				continue;
			}

			if (skipUpdateContainersMap.containsKey(containerId)) {
				if (skipUpdateContainersMap.get(containerId) < System.currentTimeMillis()) {
					continue;
				} else {
					skipUpdateContainersMap.remove(containerId);
				}
			}

			long targetVCoresMilli = Utils.vCoresToMilliVcores(targetResources.getVcores());

			Resource currentResource = container.getResource();
			if (currentResource.getMemory() == targetResources.getMemoryMB()
				&& currentResource.getVirtualCoresMilli() == targetVCoresMilli) {
				continue;
			}

			UpdateContainerRequest request = null;
			if (targetResources.getMemoryMB() >= currentResource.getMemory() &&
				// compare double with int
				targetVCoresMilli >= currentResource.getVirtualCoresMilli()) {
				// increase all
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					ContainerUpdateType.INCREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), 0, targetVCoresMilli));
			} else if (targetResources.getMemoryMB() <= currentResource.getMemory() &&
				// compare double with int
				targetVCoresMilli <= currentResource.getVirtualCoresMilli()) {
				// decrease all
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), 0, targetVCoresMilli));
			} else if (targetResources.getMemoryMB() != currentResource.getMemory()) {
				// increase | decrease memory
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					targetResources.getMemoryMB() > currentResource.getMemory() ?
						ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), 0, currentResource.getVirtualCoresMilli()));
			} else if (targetVCoresMilli != currentResource.getVirtualCoresMilli()) {
				// increase | decrease vcores
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					targetVCoresMilli > currentResource.getVirtualCoresMilli() ?
						ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(currentResource.getMemory(), 0, targetVCoresMilli));
			}
			try {
				resourceManagerClient.requestContainerUpdate(container, request);
				pendingUpdating.put(containerId, System.currentTimeMillis() + resourcesUpdateTimeoutMS);
				log.info("request update {} resources from ({} MB, {} vcores) to ({} MB, {} vcores)",
					containerId, currentResource.getMemory(), currentResource.getVirtualCoresDecimal(),
					targetResources.getMemoryMB(), targetResources.getVcores());
			} catch (Throwable t) {
				log.error("update container resources error", t);
			}
		}
	}

	private boolean checkIfCouldUpdateResources(ContainerResources newResources) {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet checkGet = buildSrCheckGet(newResources);

			try (CloseableHttpResponse response = client.execute(checkGet)) {
				if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
					log.warn("Call sr check api failed, status code: {}",
						response.getStatusLine().getStatusCode());
					return false;
				}
				String responseJson = EntityUtils.toString(response.getEntity());
				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode jsonNode = objectMapper.readTree(responseJson);
				if (jsonNode.get("allow") == null) {
					log.warn("Invalid sr check result, {}", responseJson);
					return false;
				}

				if (jsonNode.get("allow").asBoolean()) {
					return true;
				} else {
					log.warn("Resources update was rejected by sr check api, original: {}, target: {}, msg: {}",
						targetResources, newResources, jsonNode.get("msg") != null ? jsonNode.get("msg").asText() : "");
					return false;
				}
			}
		} catch (Exception e) {
			log.error("Resources update check error ", e);
			return false;
		}
	}

	private HttpGet buildSrCheckGet(ContainerResources newResources) throws Exception {
		// srAdjustCheckApi example
		// http://{service_name}/check or http://127.0.0.1:9613/check
		// uriPieces -> ["http:", "", "{service_name}", "check"]
		String[] uriPieces = srAdjustCheckApi.split("/", 4);

		URIBuilder uriBuilder = new URIBuilder();

		uriBuilder.setScheme("http");

		// set host port
		if (uriPieces[2].startsWith("{")) {
			// api addrs was configed by service name
			Discovery discovery = new Discovery();
			String serviceName = uriPieces[2].replace("{", "").replace("}", "");
			List<ServiceNode> nodes = discovery.lookupName(serviceName);
			if (nodes.size() == 0) {
				throw new Exception("Build SR check url error, no available nodes.");
			}
			ServiceNode node = nodes.get(new Random().nextInt(nodes.size()));
			uriBuilder.setHost(node.getHost())
				.setPort(node.getPort());
		} else if (uriPieces[2].indexOf(":") != -1) {
			// port is configured
			String[] hostPieces = uriPieces[2].split(":");
			uriBuilder.setHost(hostPieces[0])
				.setPort(Integer.parseInt(hostPieces[1]));
		} else {
			// use default port 80
			uriBuilder.setHost(uriPieces[2])
				.setPort(80);
		}

		uriBuilder.setPath("/" + uriPieces[3]);

		// set param
		uriBuilder.addParameter("region", region)
			.addParameter("cluster", cluster)
			.addParameter("queue", System.getenv("_FLINK_YARN_QUEUE"))
			.addParameter("jobname", applicationName)
			.addParameter("original_tm_num", "" + workerNodeMap.size())
			.addParameter("original_tm_memory", "" + targetResources.getMemoryMB())
			.addParameter("original_tm_core", "" + targetResources.getVcores())
			.addParameter("target_tm_num", "" + workerNodeMap.size())
			.addParameter("target_tm_memory", "" + newResources.getMemoryMB())
			.addParameter("target_tm_core", "" + newResources.getVcores());

		URI checkUri = uriBuilder.build();
		log.info("SR check uri: {}", checkUri);

		HttpGet get = new HttpGet(checkUri);
		RequestConfig requestConfig = RequestConfig.custom()
			.setConnectTimeout(srAdjustCheckTimeoutMS)
			.setConnectionRequestTimeout(1000)
			.setSocketTimeout(srAdjustCheckTimeoutMS).build();
		get.setConfig(requestConfig);
		return get;
	}

	private boolean validateSrAdjustCheckApi(String srAdjustCheckApi) {
		if (StringUtils.isNullOrWhitespaceOnly(srAdjustCheckApi)) {
			return true;
		}

		String[] apiPieces = srAdjustCheckApi.split("/", 4);
		if (apiPieces.length != 4) {
			return false;
		}

		if (!srAdjustCheckApi.startsWith("http://")) {
			return false;
		}

		return true;
	}

	@Override
	public CompletableFuture<SmartResourcesStats> requestSmartResourcesStats(Time timeout) {
		Map<SmartResourcesStats.Resources, Integer> containerStats = new HashMap<>();
		List<Container> containers = workerNodeMap.values()
			.stream()
			.map(YarnWorkerNode::getContainer)
			.collect(Collectors.toList());
		for (Container container : containers) {
			SmartResourcesStats.Resources resources =
				new SmartResourcesStats.Resources(
						container.getResource().getMemory(),
						container.getResource().getVirtualCoresDecimal());
			containerStats.put(resources, containerStats.getOrDefault(resources, 0) + 1);
		}
		List<SmartResourcesStats.ResourcesCount> resourcesCounts = containerStats.entrySet().stream()
			.map(entry -> new SmartResourcesStats.ResourcesCount(
					entry.getKey().getMemoryMB(),
					entry.getKey().getVcores(),
					entry.getValue()))
			.collect(Collectors.toList());
		smartResourcesStats.setContainersStats(resourcesCounts);
		return CompletableFuture.completedFuture(smartResourcesStats);
	}

	private void containerResourcesUpdaterProc() {
		Thread.currentThread().setName("ContainerResourcesUpdaterProc");

		ResourcesUsage containerMaxResources;
		ResourcesUsage applicationTotalResources;
		while (true) {
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				return;
			}

			try {
				containerMaxResources = estimaterClient.estimateContainerMaxResources(applicationID,
					durtionMinutes);
				applicationTotalResources = estimaterClient.estimateApplicationResources(region,
					cluster, applicationName, durtionMinutes);

				int newMemoryMB = new Double(Math.ceil(containerMaxResources.getMemTotalMB()
					* (1 + memReserveRatio) / 1024)).intValue() * 1024;
				newMemoryMB = newMemoryMB > srMemMaxMB ? srMemMaxMB : newMemoryMB;

				double newVcores = Double.parseDouble(String.format("%.3f",
					applicationTotalResources.getCpuTotalVcores() * (1 + cpuReserveRatio)
						/ workerNodeMap.size()));
				if (!srCpuAdjustDoubleEnable) {
					if (srCpuEstimateMode
						.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_FLOOR)) {
						newVcores = new Double(Math.floor(newVcores));
					} else if (srCpuEstimateMode
						.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_ROUND)) {
						newVcores = new Long(Math.round(newVcores));
					} else if (srCpuEstimateMode
						.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL)) {
						newVcores = new Double(Math.ceil(newVcores));
					}
				}
				log.info("newVcores: {}, newMemoryMB: {}.", newVcores, newMemoryMB);

				newVcores = newVcores > 8 ? 8 : newVcores;
				newVcores = newVcores < 1 ? 1 : newVcores;
				final UpdateContainersResources updateContainersResources = new UpdateContainersResources(
					newMemoryMB, newVcores, new Long(containerMaxResources.getDurtion()).intValue());

				runAsync(() -> updateContainersResources(updateContainersResources));

			} catch (InterruptedException e) {
				log.info("estimate application resources interrupted");
				return;
			} catch (Throwable e) {
				log.warn("estimate application resources error", e);
				if (e.getMessage() != null && e.getMessage().contains("The duration of the application is too short")) {
					try {
						Thread.sleep(CONTAINER_SKIP_UPDATE_TIME_MS);
					} catch (InterruptedException e1) {
						return;
					}
				}
			}
		}
	}

	private void initTargetContainerResources() {
		int containerMemorySizeMB = this.defaultTaskManagerMemoryMB;
		final double vcores = this.defaultCpus;

		containerMemorySizeMB = new Double(Math.ceil(containerMemorySizeMB / 1024.0)).intValue() * 1024;

		targetResources = new ContainerResources(containerMemorySizeMB, vcores);
		smartResourcesStats.setInitialResources(
			new SmartResourcesStats.Resources(containerMemorySizeMB, vcores));
	}

	private ResourceID getResourceID(Container container) {
		return new ResourceID(container.getId().toString());
	}

	private void registerMetrics() {
		jobManagerMetricGroup.gauge("allocatedContainerNum", () -> (TagGaugeStore) () -> {
			List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = new ArrayList<>();
			tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
				workerNodeMap.size(),
				new TagGaugeStore.TagValuesBuilder()
					.addTagValue("cores", String.valueOf(defaultCpus))
					.addTagValue("memory", String.valueOf(defaultTaskManagerMemoryMB))
					.build()));
			return tagGaugeMetrics;
		});
		jobManagerMetricGroup.gauge("allocatedCPU", () -> getAllocateCpu());
		jobManagerMetricGroup.gauge("allocatedMemory", () -> getAllocateMemory());
		jobManagerMetricGroup.gauge("pendingCPU", () -> defaultCpus * numPendingContainerRequests);
		jobManagerMetricGroup.gauge("pendingMemory", () -> defaultTaskManagerMemoryMB * numPendingContainerRequests);
		jobManagerMetricGroup.gauge("pendingRequestedContainerNum", () -> numPendingContainerRequests);
		jobManagerMetricGroup.gauge("startingContainers", () -> (TagGaugeStore) () -> {
			long ts = System.currentTimeMillis();
			return getSlowContainerManager().getStartingContainers().entrySet().stream()
					.map(resourceIDLongEntry -> new TagGaugeStore.TagGaugeMetric(
							ts - resourceIDLongEntry.getValue(),
							new TagGaugeStore.TagValuesBuilder()
									.addTagValue("container_id", pruneContainerId(resourceIDLongEntry.getKey().getResourceIdString()))
									.addTagValue("container_host", getContainerHost(workerNodeMap.get(resourceIDLongEntry.getKey())))
									.build()))
					.collect(Collectors.toList()); });
		jobManagerMetricGroup.gauge("slowContainerNum", slowContainerManager::getSlowContainerSize);
		jobManagerMetricGroup.gauge("totalRedundantContainerNum", slowContainerManager::getTotalRedundantContainersNum);
		jobManagerMetricGroup.gauge("pendingRedundantContainerNum", slowContainerManager::getPendingRedundantContainersNum);
		jobManagerMetricGroup.gauge("startingRedundantContainerNum", slowContainerManager::getStartingRedundantContainerSize);
		jobManagerMetricGroup.gauge("speculativeSlowContainerTimeoutMs", slowContainerManager::getSpeculativeSlowContainerTimeoutMs);
		jobManagerMetricGroup.gauge("containerStartDurationMaxMs", () -> containerStartDurationMaxMs);
		jobManagerMetricGroup.gauge("completedContainer", completedContainerGauge);
		jobManagerMetricGroup.counter("descheduledContainers", descheduledContainersCounter);
		jobManagerMetricGroup.gauge("deschedulerDurationMs", deschedulerDurationMsGuage);
		jobManagerMetricGroup.gauge("deschedulerHandleResult", deschedulerHandleGuage);
		jobManagerMetricGroup.gauge("deschedulerReceivedInfo", deschedulerReceivedGuage);
		jobManagerMetricGroup.counter("gangFailedNum", gangFailedCounter);
		jobManagerMetricGroup.counter("gangDowngradeNum", gangDowngradeCounter);
		jobManagerMetricGroup.gauge(EVENT_METRIC_NAME, jobStartEventMessageSet);
		jobManagerMetricGroup.gauge("numLackWorkers", () -> (long) getNumLackWorks());
	}

	private int getNumLackWorks(){
		int numberRequestedNotStartedWorkers = slowContainerManager.getStartingContainerSize() + numPendingContainerRequests;
		int numberRequestedNotStartedRedundantWorkers = slowContainerManager.getStartingRedundantContainerSize() + slowContainerManager.getPendingRedundantContainersNum();
		int numberRequiredWorkers = (int) Math.ceil(getNumberRequiredTaskManagerSlots() / (double) numberOfTaskSlots) + slowContainerManager.getTotalRedundantContainersNum() - numberRequestedNotStartedRedundantWorkers;
		return numberRequiredWorkers - (numberRequestedNotStartedWorkers - numberRequestedNotStartedRedundantWorkers);
	}

	private double getAllocateCpu() {
		double allocatedCpu = 0.0;
		for (Map.Entry<ResourceID, YarnWorkerNode> entry : workerNodeMap.entrySet()){
			allocatedCpu += entry.getValue().getContainer().getResource().getVirtualCoresMilli() / 1000.0;
		}
		return allocatedCpu;
	}

	private long getAllocateMemory() {
		long allocatedMemory = 0;
		for (Map.Entry<ResourceID, YarnWorkerNode> entry : workerNodeMap.entrySet()){
			allocatedMemory += entry.getValue().getContainer().getResource().getMemorySize();
		}
		return allocatedMemory;
	}

	public static String getContainerHost(YarnWorkerNode yarnWorkerNode) {
		if (yarnWorkerNode != null) {
			return yarnWorkerNode.getContainer().getNodeId().getHost();
		} else {
			return "Unknown";
		}
	}

	private void checkSlowContainers() {
		try {
			slowContainerManager.checkSlowContainer();
		} catch (Exception e) {
			log.warn("Error while checkSlowContainers.", e);
		} finally {
			scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		}
	}

	private void checkDescheduledContainers() {
		try {
			checkDescheduledContainersInternal();
		} catch (Exception e) {
			log.warn("Error while check descheduled containers.", e);
		} finally {
			scheduleRunAsync(this::checkDescheduledContainers, descheduledContainersCheckIntervalMs, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public CompletableFuture<String> requestJMWebShell(Time timeout) {
		CompletableFuture<String> jmWebShell = new CompletableFuture<>();
		try {
			jmWebShell.complete(
				NMWebshellUtil.getWeshellRelayFullUrl(
					Utils.getYarnHostname(),
					Utils.getCurrentContainerID(),
					EnvironmentInformation.getHadoopUser())
			);
		} catch (Exception e) {
			jmWebShell.completeExceptionally(e);
		}
		return jmWebShell;
	}

	@Override
	public CompletableFuture<String> requestJobManagerLogUrl(Time timeout) {
		CompletableFuture<String> jmLog = new CompletableFuture<>();
		try {
			String region = flinkConfig.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
			String jmHost = Utils.getYarnHostname() + ":" + System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
			jmLog.complete(
				MegatronUtil.getMegatronLogUrl(region, jmHost, Utils.getCurrentContainerID(), EnvironmentInformation.getHadoopUser(), ""));
		} catch (Exception e) {
			log.error("Error while get relay log.", e);
			jmLog.completeExceptionally(e);
		}
		return jmLog;
	}

	@Override
	public String getTaskManagerWebShell(ResourceID resourceID, String host) {
		try {
			return NMWebshellUtil.getWeshellRelayFullUrl(host, resourceID.getResourceIdString(), EnvironmentInformation.getHadoopUser());
		} catch (Exception e) {
			log.error("Error while get relay webshell, fallback to default webshell.", e);
			return super.getTaskManagerWebShell(resourceID, host);
		}
	}

	@Override
	public String getTaskManagerLogUrl(ResourceID resourceID, String host) {
		try {
			String nmPort = System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
			String nmHostWithPort = host + ":" + nmPort;
			String region = flinkConfig.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
			return MegatronUtil.getMegatronLogUrl(region, nmHostWithPort, resourceID.getResourceIdString(), EnvironmentInformation.getHadoopUser(), "");
		} catch (Exception e) {
			log.error("Error while get relay log, fallback to default log.", e);
			return super.getTaskManagerLogUrl(resourceID, host);
		}
	}

	@Override
	protected void closeTaskManagerConnection(final ResourceID resourceID, final Exception cause, final int exitCode) {
		YarnWorkerNode workerNode = removeContainer(resourceID);
		if (workerNode != null) {
			stopWorker(workerNode, exitCode);
		}
		super.closeTaskManagerConnection(resourceID, cause, exitCode);
	}

	private YarnWorkerNode removeContainer(ResourceID resourceID) {
		slowContainerManager.containerRemoved(resourceID);
		return workerNodeMap.remove(resourceID);
	}

	public void stopWorker(ResourceID resourceID, int exitCode) {
		YarnWorkerNode workerNode = workerNodeMap.get(resourceID);
		if (workerNode != null) {
			stopWorker(workerNode, exitCode);
		} else {
			log.warn("Resource {} not found, can not stop it.", resourceID);
		}
	}

	public boolean startNewWorker(ResourceID resourceID) {
		if (workerNodeMap.containsKey(resourceID)) {
			return !startNewWorker(ResourceProfile.UNKNOWN).isEmpty();
		} else {
			log.warn("Resource {} not found, will not start new worker.", resourceID);
			return false;
		}
	}

	public void removePendingRequests(int pendingRequestsNumber) {
		final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();
		for (int i = 0; i < pendingRequestsNumber && pendingRequestsIterator.hasNext(); i++) {
			removeContainerRequest(pendingRequestsIterator.next());
		}
	}
}
