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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.event.CompoundRecorder;
import org.apache.flink.event.WarehouseJobStartEventMessageRecorder;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.configuration.HdfsConfigOptions;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.LoggerHelper;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.exceptions.ContainerCompletedException;
import org.apache.flink.yarn.exceptions.ExpectedContainerCompletedException;
import org.apache.flink.yarn.slowcontainer.NoOpSlowContainerManager;
import org.apache.flink.yarn.slowcontainer.SlowContainerActions;
import org.apache.flink.yarn.slowcontainer.SlowContainerManager;
import org.apache.flink.yarn.slowcontainer.SlowContainerManagerImpl;
import org.apache.flink.yarn.smartresources.ContainerResources;
import org.apache.flink.yarn.smartresources.SmartResourceManager;
import org.apache.flink.yarn.smartresources.UpdateContainersResources;

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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.megatron.MegatronUtil;
import org.apache.hadoop.yarn.util.webshell.NMWebshellUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class YarnResourceManager extends ActiveResourceManager<YarnWorkerNode>
		implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);

	static final String ENV_YARN_CONTAINER_ID = "CONTAINER_ID";

	private static final int YARN_MEM_MIN_MB = 1024;

	/** YARN container map. Package private for unit test purposes. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;

	/** Whether make recovered WorkerNode as pending working. */
	private final boolean yarnPreviousContainerAsPending;

	/** Get recovered WorkerNode Set from YARN When AM failover. */
	private final Set<ResourceID> recoveredWorkerNodeSet;

	/** Timeout to wait previous container register. */
	private final Long yarnPreviousContainerTimeoutMs;

	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	static final String ERROR_MASSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClientAsync nodeManagerClient;

	private final WorkerSpecContainerResourceAdapter workerSpecContainerResourceAdapter;

	private final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector;

	private WorkerSpecContainerResourceAdapter.MatchingStrategy matchingStrategy;

	private final SlowContainerManager slowContainerManager;
	/** Interval in milliseconds of check if the container is slow. */
	private final long slowContainerCheckIntervalMs;

	private final TagGauge completedContainerGauge = new TagGauge.TagGaugeBuilder().setClearAfterReport(true).build();

	private final boolean cleanupRunningContainersOnStop;

	private final int defaultTaskManagerMemoryMB;

	private final double defaultCpus;

	/** The conf that yarn container bind cpu core. */
	private final RuntimeConfiguration yarnRuntimeConf;

	private final SmartResourceManager smartResourceManager;
	private Thread containerResourcesUpdater;

	private final Set<String> yarnBlackedHosts;

	private static final String EVENT_METRIC_NAME = "resourceManagerEvent";

	private final WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder;

	private final CompoundRecorder compoundRecorder;

	private final Executor startContainerExecutor;

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

	public YarnResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			FailureRater failureRater) {
		super(
			flinkConfig,
			env,
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			clusterPartitionTrackerFactory,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			failureRater);
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

		this.workerNodeMap = new ConcurrentHashMap<>();
		this.recoveredWorkerNodeSet = new HashSet<>();
		this.yarnPreviousContainerTimeoutMs = flinkConfig.getLong(YarnConfigOptions.YARN_PREVIOUS_CONTAINER_TIMEOUT_MS);
		this.yarnPreviousContainerAsPending = flinkConfig.getBoolean(YarnConfigOptions.YARN_PREVIOUS_CONTAINER_AS_PENDING);

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

		this.defaultTaskManagerMemoryMB = TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig).getFlinkMemory().getTotalFlinkMemorySize().getMebiBytes();
		this.defaultCpus = flinkConfig.getDouble(YarnConfigOptions.VCORES, flinkConfig.getInteger(
			TaskManagerOptions.NUM_TASK_SLOTS));

		this.gangSchedulerEnabled = flinkConfig.getBoolean(YarnConfigOptions.GANG_SCHEDULER);
		this.gangCurrentRetryTimes = 0;
		this.gangLastDowngradeTimestamp = -1;
		this.gangMaxRetryTimes = flinkConfig.getInteger(YarnConfigOptions.GANG_MAX_RETRY_TIMES);
		this.gangDowngradeTimeoutMilli = flinkConfig.getInteger(YarnConfigOptions.GANG_DOWNGRADE_TIMEOUT_MS);
		this.gangDowngradeOnFailed = flinkConfig.getBoolean(YarnConfigOptions.GANG_DOWNGRADE_ON_FAILED);
		this.nodeAttributesExpression = flinkConfig.getString(YarnConfigOptions.NODE_SATISFY_ATTRIBUTES_EXPRESSION);
		this.fatalOnGangFailed = true;

		this.yarnRuntimeConf = createRuntimeConfigurationWithQosLevel(
			flinkConfig.getOptional(YarnConfigOptions.YARN_RUNTIME_CONF_QOS_LEVEL).orElse(YarnConfigOptions.RtQoSLevelEnum.SHARE));

		// init the SmartResource
		this.smartResourceManager = new SmartResourceManager(flinkConfig, yarnRuntimeConf, env);
		this.smartResourceManager.setYarnResourceManager(this);
		if (smartResourceManager.getSmartResourcesEnable()) {
			this.containerResourcesUpdater = new Thread(this::containerResourcesUpdaterProc);
			this.containerResourcesUpdater.start();
			log.info("SmartResource started.");
		}

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

		this.webInterfaceUrl = webInterfaceUrl;

		this.workerSpecContainerResourceAdapter = new WorkerSpecContainerResourceAdapter(
			flinkConfig,
			Math.max(
					yarnConfig.getInt(
							YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
							YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
					YARN_MEM_MIN_MB),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES),
			ExternalResourceUtils.getExternalResources(flinkConfig, YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX));
		this.registerApplicationMasterResponseReflector = new RegisterApplicationMasterResponseReflector(log);

		this.matchingStrategy = flinkConfig.getBoolean(YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES) ?
			WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;

		this.cleanupRunningContainersOnStop = flinkConfig.getBoolean(YarnConfigOptions.CLEANUP_RUNNING_CONTAINERS_ON_STOP);
		this.yarnBlackedHosts = new HashSet<>();

		String currentContainerId = System.getenv(ENV_YARN_CONTAINER_ID);
		this.warehouseJobStartEventMessageRecorder = new WarehouseJobStartEventMessageRecorder(currentContainerId, false);
		this.compoundRecorder = new CompoundRecorder(this.warehouseJobStartEventMessageRecorder);
		this.startContainerExecutor = Executors.newScheduledThreadPool(
				flinkConfig.getInteger(YarnConfigOptions.CONTAINER_LAUNCHER_NUMBER),
				new ExecutorThreadFactory("resourcemanager-start-container"));
	}

	private RuntimeConfiguration createRuntimeConfigurationWithQosLevel(YarnConfigOptions.RtQoSLevelEnum rtQoSLevel) {
		RuntimeConfiguration runtimeConfiguration = RuntimeConfiguration.newInstance();
		switch (rtQoSLevel) {
			case RESERVED:
				runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_RESERVED);
				break;
			case SHARE:
				runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_SHARE);
				break;
			case ANY:
				runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_ANY);
				break;
			default:
				log.warn("QosLevel is {} which not match any mode, therefore QosLevel use default share mode", yarnRuntimeConf);
				runtimeConfiguration.setQosLevel(RtQoSLevel.QOS_SHARE);
		}
		return runtimeConfiguration;
	}

	@VisibleForTesting
	public SlowContainerManager getSlowContainerManager() {
		return slowContainerManager;
	}

	@VisibleForTesting
	SmartResourceManager getSmartResourceManager() {
		return smartResourceManager;
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
		updateMatchingStrategy(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	@VisibleForTesting
	protected void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			registerApplicationMasterResponseReflector.getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		fatalOnGangFailed = containersFromPreviousAttempts.isEmpty();

		for (final Container container : containersFromPreviousAttempts) {
			ResourceID resourceID = new ResourceID(container.getId().toString());
			workerNodeMap.put(resourceID, new YarnWorkerNode(container));
			if (this.yarnPreviousContainerAsPending) {
				recoveredWorkerNodeSet.add(resourceID);
			}
		}
	}

	private void updateMatchingStrategy(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final Optional<Set<String>> schedulerResourceTypesOptional =
			registerApplicationMasterResponseReflector.getSchedulerResourceTypeNames(registerApplicationMasterResponse);

		if (schedulerResourceTypesOptional.isPresent()) {
			Set<String> types = schedulerResourceTypesOptional.get();
			log.info("Register application master response contains scheduler resource types: {}.", types);
			matchingStrategy = types.contains("CPU") ?
				WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
				WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		} else {
			log.info("Register application master response does not contain scheduler resource types, use '{}'.",
				YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES.key());
		}
		log.info("Container matching strategy: {}.", matchingStrategy);
	}

	protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClientAsync nodeManagerClient = NMClientAsync.createNMClientAsync(this);
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.getClient().cleanupRunningContainersOnStop(cleanupRunningContainersOnStop);
		return nodeManagerClient;
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration(env.get(ApplicationConstants.Environment.PWD.key()));
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		slowContainerManager.setSlowContainerActions(new SlowContainerActionsImpl());
		try {
			resourceManagerClient = createAndStartResourceManagerClient(
				yarnConfig,
				yarnHeartbeatIntervalMillis,
				webInterfaceUrl);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);
	}

	@Override
	protected void startServicesOnLeadership() {
		scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		if (!recoveredWorkerNodeSet.isEmpty() && this.yarnPreviousContainerTimeoutMs > 0) {
			log.info("Will check {} previous container timeout in {} ms.", recoveredWorkerNodeSet.size(), yarnPreviousContainerTimeoutMs);
			Set<ResourceID> workerToCheckTimeout = new HashSet<>(recoveredWorkerNodeSet);
			scheduleRunAsync(
					() -> releasePreviousContainer(workerToCheckTimeout),
					this.yarnPreviousContainerTimeoutMs,
					TimeUnit.MILLISECONDS);
		}
		super.startServicesOnLeadership();
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

		return getStopTerminationFutureOrCompletedExceptionally(firstException);
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
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		return startNewWorkers(workerResourceSpec, 1);
	}

	@Override
	public boolean startNewWorkers(WorkerResourceSpec workerResourceSpec, int resourceNumber) {
		return requestYarnContainers(workerResourceSpec, resourceNumber);
	}

	@VisibleForTesting
	Optional<Resource> getContainerResource(WorkerResourceSpec workerResourceSpec) {
		return workerSpecContainerResourceAdapter.tryComputeContainerResource(workerResourceSpec);
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		return stopWorker(workerNode, WorkerExitCode.UNKNOWN);
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode, int exitCode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}, exitCode {}.", container.getId(), exitCode);
		nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId(), exitCode);
		resourceManagerClient.releaseAssignedContainer(container.getId(), exitCode);
		workerNodeMap.remove(workerNode.getResourceID());
		notifyAllocatedWorkerStopped(workerNode.getResourceID());
		completedContainerGauge.addMetric(
				1,
				new TagGaugeStoreImpl.TagValuesBuilder()
						.addTagValue("container_host", workerNode.getContainer().getNodeId().getHost())
						.addTagValue("container_id", pruneContainerId(workerNode.getResourceID().getResourceIdString()))
						.addTagValue("exit_code", String.valueOf(exitCode))
						.build());
		return true;
	}

	public boolean stopWorker(ResourceID resourceID, int exitCode) {
		YarnWorkerNode yarnWorkerNode = workerNodeMap.get(resourceID);
		if (yarnWorkerNode != null) {
			return stopWorker(yarnWorkerNode, exitCode);
		} else {
			return true;
		}
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		YarnWorkerNode workerNode = workerNodeMap.get(resourceID);
		if (workerNode != null) {
			compoundRecorder.startContainerFinish(workerNode.getContainer().getId().toString());
		}
		return workerNode;
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
				// todo gang does not support different resource Type.
				if (getNumRequestedNotAllocatedWorkers() >= gangSchedulerNotifyContent.getRequestedContainerNum()) {
					runAsync(() -> {
							removeContainerRequest(getDefaultWorkerResourceSpec(), gangSchedulerNotifyContent.getRequestedContainerNum());
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
							if (blacklistTracker != null) {
								blacklistTracker.clearAll();
							}
							scheduleRunAsync(
								this::requestYarnContainerIfRequired,
								flinkConfig.getInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_RETRY_MS),
								TimeUnit.MILLISECONDS);
						}
					);
				} else {
					log.info("Failed containerRequests:{} more than pending:{}. It is AMRMClient auto request, ignore.",
						gangSchedulerNotifyContent.getRequestedContainerNum(), getNumRequestedNotAllocatedWorkers());
				}
			} else {
				String fatalMessage = "Request new container by GangScheduler failed, " + gangSchedulerNotifyContent;
				onFatalError(fatalMessage, flinkConfig.getInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_FATAL_MS));
			}
		} else {
			log.warn("Unknown onNotifyMsg type, {}", msg);
		}
	}

	@Override
	public void onContainersCompleted(final List<ContainerStatus> statuses) {
		runAsync(() -> {
				log.warn("YARN ResourceManager reported {} containers completed.", statuses.size());
				for (final ContainerStatus containerStatus : statuses) {

					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);

					notifyAllocatedWorkerStopped(resourceId);

					if (yarnWorkerNode != null) {
						log.warn("Container {} on {} completed with exit code {}, {}",
								LoggerHelper.secMark("containerId", containerStatus.getContainerId()),
								LoggerHelper.secMark("host", yarnWorkerNode.getContainer().getNodeId().getHost()),
								LoggerHelper.secMark("exitStatus", containerStatus.getExitStatus()),
								LoggerHelper.secMark("diagnostics", containerStatus.getDiagnostics()));
						ContainerCompletedException containerCompletedException = ContainerCompletedException.fromExitCode(
								containerStatus.getExitStatus(),
								containerStatus.getDiagnostics());
						recordWorkerFailure(yarnWorkerNode.getContainer().getNodeId().getHost(), resourceId, containerCompletedException);
						// Container completed unexpectedly ~> start a new one
						requestYarnContainerIfRequired();

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
	public void onContainersAllocated(List<Container> containers) {
		fatalOnGangFailed = false;
		runAsync(() -> {
			log.info("Received {} containers.", containers.size());

			for (Map.Entry<Resource, List<Container>> entry : groupContainerByResource(containers).entrySet()) {
				onContainersOfResourceAllocated(entry.getKey(), entry.getValue());
			}

			// if we are waiting for no further containers, we can go to the
			// regular heartbeat interval
			if (getNumRequestedNotAllocatedWorkers() <= 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		});
	}

	@Override
	public void onContainersDescheduled(List<Container> list) {
		// TODO implement this function later.
	}

	private Map<Resource, List<Container>> groupContainerByResource(List<Container> containers) {
		return containers.stream().collect(Collectors.groupingBy(Container::getResource));
	}

	private void onContainersOfResourceAllocated(Resource resource, List<Container> containers) {
		final List<WorkerResourceSpec> pendingWorkerResourceSpecs =
			workerSpecContainerResourceAdapter.getWorkerSpecs(resource, matchingStrategy).stream()
				.flatMap(spec -> Collections.nCopies(getNumRequestedNotAllocatedWorkersFor(spec), spec).stream())
				.collect(Collectors.toList());

		int numPending = pendingWorkerResourceSpecs.size();
		log.info("Received {} containers with resource {}, {} pending container requests.",
			containers.size(),
			resource,
			numPending);

		final Iterator<Container> containerIterator = containers.iterator();
		final Iterator<WorkerResourceSpec> pendingWorkerSpecIterator = pendingWorkerResourceSpecs.iterator();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator =
			getPendingRequestsAndCheckConsistency(resource, pendingWorkerResourceSpecs.size()).iterator();

		int numAccepted = 0;
		while (containerIterator.hasNext() && pendingWorkerSpecIterator.hasNext()) {
			final Container container = containerIterator.next();
			if (yarnBlackedHosts.contains(container.getNodeId().getHost())) {
				returnBlackedContainer(container);
				continue;
			}
			final WorkerResourceSpec workerResourceSpec = pendingWorkerSpecIterator.next();
			final AMRMClient.ContainerRequest pendingRequest = pendingRequestsIterator.next();
			final ResourceID resourceId = getContainerResourceId(container);
			log.info("Received new container: {} on {}", container.getId(), container.getNodeId().getHost());

			notifyNewWorkerAllocated(workerResourceSpec, resourceId);
			startTaskExecutorInContainer(container, workerResourceSpec, resourceId);
			removeContainerRequest(pendingRequest, workerResourceSpec);

			numAccepted++;
		}
		numPending -= numAccepted;

		int numExcess = 0;
		while (containerIterator.hasNext()) {
			returnExcessContainer(containerIterator.next());
			numExcess++;
		}

		log.info("Accepted {} requested containers, returned {} excess containers, {} pending container requests of resource {}.",
			numAccepted, numExcess, numPending, resource);
	}

	private static ResourceID getContainerResourceId(Container container) {
		return new ResourceID(container.getId().toString());
	}

	private void startTaskExecutorInContainer(Container container, WorkerResourceSpec workerResourceSpec, ResourceID resourceId) {
		workerNodeMap.put(resourceId, new YarnWorkerNode(container));

		startContainerExecutor.execute(() -> {
			try {
				compoundRecorder.createTaskManagerContextStart(container.getId().toString());
				// Context information used to start a TaskExecutor Java process
				ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
						resourceId.toString(),
						container.getNodeId().getHost(),
						TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec));

				compoundRecorder.createTaskManagerContextFinish(container.getId().toString());
				compoundRecorder.startContainerStart(container.getId().toString());
				nodeManagerClient.startContainerAsync(container, taskExecutorLaunchContext);
			} catch (Throwable t) {
				getMainThreadExecutor().execute(() -> releaseFailedContainerAndRequestNewContainerIfRequired(container.getId(), t));
			}
		});
	}

	private void releaseFailedContainerAndRequestNewContainerIfRequired(ContainerId containerId, Throwable throwable) {
		validateRunsInMainThread();

		log.error("Could not start TaskManager in container {}.", containerId, throwable);

		final ResourceID resourceId = new ResourceID(containerId.toString());
		// release the failed container
		YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);
		if (yarnWorkerNode != null) {
			recordWorkerFailure(yarnWorkerNode.getContainer().getNodeId().getHost(), resourceId, throwable);
			completedContainerGauge.addMetric(
					1,
					new TagGaugeStoreImpl.TagValuesBuilder()
							.addTagValue("container_host", yarnWorkerNode.getContainer().getNodeId().getHost())
							.addTagValue("container_id", pruneContainerId(resourceId.getResourceIdString()))
							.addTagValue("exit_code", String.valueOf(WorkerExitCode.START_CONTAINER_ERROR))
							.build());
		}
		resourceManagerClient.releaseAssignedContainer(containerId);
		notifyAllocatedWorkerStopped(resourceId);
		// and ask for a new one
		requestYarnContainerIfRequired();
	}

	private void returnExcessContainer(Container excessContainer) {
		log.info("Returning excess container {}.", excessContainer.getId());
		resourceManagerClient.releaseAssignedContainer(excessContainer.getId(), WorkerExitCode.EXCESS_CONTAINER);
	}

	private void returnBlackedContainer(Container container) {
		log.info("Returning blacked container {}.", container.getId());
		resourceManagerClient.releaseAssignedContainer(container.getId(), WorkerExitCode.IN_BLACKLIST);
		requestYarnContainerIfRequired();
	}

	private void returnBlackedPreviousContainer(ResourceID resourceID) {
		log.info("Returning blacked previous container {}.", resourceID);
		stopWorker(resourceID, WorkerExitCode.IN_BLACKLIST);
		requestYarnContainerIfRequired();
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest, WorkerResourceSpec workerResourceSpec) {
		log.info("Removing container request {}.", pendingContainerRequest);
		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private void removeContainerRequest(WorkerResourceSpec workerResourceSpec, int expectedNum) {
		Optional<Resource> resource = workerSpecContainerResourceAdapter.tryComputeContainerResource(workerResourceSpec);
		if (resource.isPresent()) {
			final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator =
					getPendingRequestsAndCheckConsistency(resource.get(), getNumRequestedNotAllocatedWorkersFor(workerResourceSpec)).iterator();
			for (int i = 0; i < expectedNum; i++) {
				if (pendingRequestsIterator.hasNext()) {
					AMRMClient.ContainerRequest containerRequest = pendingRequestsIterator.next();
					removeContainerRequest(containerRequest, workerResourceSpec);
					notifyNewWorkerRequestReleased(workerResourceSpec);
				} else {
					break;
				}
			}
		}
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequestsAndCheckConsistency(Resource resource, int expectedNum) {
		final Collection<Resource> equivalentResources = workerSpecContainerResourceAdapter.getEquivalentContainerResource(resource, matchingStrategy);
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests =
			equivalentResources.stream()
				.flatMap(equivalentResource -> resourceManagerClient.getMatchingRequests(
					RM_REQUEST_PRIORITY,
					ResourceRequest.ANY,
					equivalentResource).stream())
				.collect(Collectors.toList());

		final Collection<AMRMClient.ContainerRequest> matchingContainerRequests;

		if (matchingRequests.isEmpty()) {
			matchingContainerRequests = Collections.emptyList();
		} else {
			final Collection<AMRMClient.ContainerRequest> collection = matchingRequests.get(0);
			matchingContainerRequests = new ArrayList<>(collection);
		}

		Preconditions.checkState(
			matchingContainerRequests.size() == expectedNum,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests for resource %s has diverged. " +
				"Number client's pending container requests %s != Number RM's pending container requests %s.",
			resource, matchingContainerRequests.size(), expectedNum);

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
			smartResourceManager.containersUpdated(containers);
		});
	}

	@Override
	public void onContainersUpdateError(List<UpdateContainerError> updateContainerErrors) {
		runAsync(() -> {
			log.error("Received ContainersUpdateError {}", updateContainerErrors);
			smartResourceManager.containersUpdateError(updateContainerErrors);
		});
	}

	// ------------------------------------------------------------------------
	//  NMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------
	@Override
	public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
		log.debug("Succeeded to call YARN Node Manager to start container {}.", containerId);
	}

	@Override
	public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
		// We are not interested in getting container status
	}

	@Override
	public void onContainerStopped(ContainerId containerId) {
		log.debug("Succeeded to call YARN Node Manager to stop container {}.", containerId);
	}

	@Override
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		runAsync(() -> releaseFailedContainerAndRequestNewContainerIfRequired(containerId, t));
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
		// We are not interested in getting container status
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable throwable) {
		log.warn("Error while calling YARN Node Manager to stop container {}.", containerId, throwable);
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

	/**
	 * Request new container if pending containers cannot satisfy pending slot requests.
	 */
	private void requestYarnContainerIfRequired() {
		for (Map.Entry<WorkerResourceSpec, Integer> requiredWorkersPerResourceSpec : getRequiredResources().entrySet()) {
			final WorkerResourceSpec workerResourceSpec = requiredWorkersPerResourceSpec.getKey();
			log.info("requestYarnContainerIfRequired, resourceSpec: {}, requiredWorkersNum: {}, " +
					"requestedNotRegisteredWorkersNum: {}, totalRedundantContainerNum: {}, " +
					"pendingRedundantContainersNum: {}, startingRedundantContainerNum: {}.",
				workerResourceSpec.toString(),
				requiredWorkersPerResourceSpec.getValue(),
				getNumRequestedNotRegisteredWorkersFor(workerResourceSpec),
				slowContainerManager.getRedundantContainerNum(workerResourceSpec),
				slowContainerManager.getPendingRedundantContainersNum(workerResourceSpec),
				slowContainerManager.getStartingRedundantContainerNum(workerResourceSpec));
			// exclude redundant container requests for slow container.
			while (requiredWorkersPerResourceSpec.getValue() >
					(getNumRequestedNotRegisteredWorkersFor(workerResourceSpec)
							- slowContainerManager.getRedundantContainerNum(workerResourceSpec))) {
				final boolean requestContainerSuccess = tryStartNewWorker(workerResourceSpec);
				Preconditions.checkState(requestContainerSuccess,
					"Cannot request container for worker resource spec {}.", workerResourceSpec);
			}
		}
	}

	private boolean requestYarnContainers(WorkerResourceSpec workerResourceSpec, int workerNumber) {
		Optional<Resource> containerResourceOptional = getContainerResource(workerResourceSpec);

		if (containerResourceOptional.isPresent() && workerNumber > 0) {
			// try to fetch recovered containers and return unallocated number
			int unallocatedWorkerNumber = fetchFromRecoveredContainers(workerResourceSpec, workerNumber);
			if (workerNumber - unallocatedWorkerNumber > 0) {
				log.info("Allocate {} containers from previous", workerNumber - unallocatedWorkerNumber);
			}

			if (unallocatedWorkerNumber == 0) {
				return true;
			}

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
			log.info("Allocate {} containers, Using gang scheduler: {}", unallocatedWorkerNumber, useGang);
			AMRMClient.ContainerRequest containerRequest = null;
			for (int i = 0; i < unallocatedWorkerNumber; i++) {
				containerRequest = getContainerRequest(containerResourceOptional.get(), useGang);
				resourceManagerClient.addContainerRequest(containerRequest);
			}

			// make sure we transmit the request fast and receive fast news of granted allocations
			resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
			int numPendingWorkers = 0;
			for (int i = 0; i < unallocatedWorkerNumber; i++) {
				numPendingWorkers = notifyNewWorkerRequested(workerResourceSpec).getNumNotAllocated();
			}

			log.info("Requesting new TaskExecutor container with resource {}, resourceRequest {}. Number pending workers of this resource is {}.",
				workerResourceSpec,
				containerRequest,
				numPendingWorkers);
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Fetch workerNumber of matching WorkerResourceSpec container from recovered Containers.
	 * @param workerResourceSpec WorkerResourceSpec.
	 * @param workerNumber The quantity of need to allocate.
	 * @return The quantity of unallocated.
	 */
	private int fetchFromRecoveredContainers(WorkerResourceSpec workerResourceSpec, int workerNumber) {
		Optional<Resource> containerResourceOptional = getContainerResource(workerResourceSpec);

		List<ResourceID> blackedRecoveredContainers = new ArrayList<>(4);

		if (containerResourceOptional.isPresent()) {
			Resource requestResource = containerResourceOptional.get();
			Iterator<ResourceID> resourceIDIterator = recoveredWorkerNodeSet.iterator();

			while (resourceIDIterator.hasNext()) {
				if (workerNumber <= 0){
					break;
				}

				ResourceID resourceID = resourceIDIterator.next();
				if (workerNodeMap.containsKey(resourceID)) {
					Container container = workerNodeMap.get(resourceID).getContainer();
					if (yarnBlackedHosts.contains(container.getNodeId().getHost())){
						blackedRecoveredContainers.add(resourceID);
						continue;
					}

					if (requestResource.equals(container.getResource())) {
						workerNumber--;
						resourceIDIterator.remove();
						notifyRecoveredWorkerAllocated(workerResourceSpec, resourceID);
					}
				} else {
					log.error("Worker {} is previous but not in workerNodeMap.", resourceID);
					resourceIDIterator.remove();
				}
			}
		}

		for (ResourceID resourceID : blackedRecoveredContainers) {
			returnBlackedPreviousContainer(resourceID);
		}

		return workerNumber;
	}

	@Nonnull
	@VisibleForTesting
	AMRMClient.ContainerRequest getContainerRequest(Resource containerResource) {
		return getContainerRequest(containerResource, false);
	}

	private AMRMClient.ContainerRequest getContainerRequest(Resource containerResource, boolean useGang) {
		AMRMClient.ContainerRequest containerRequest;
		if (useGang) {
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
			containerRequest = new AMRMClient.ContainerRequest(containerResource, null, null, RM_REQUEST_PRIORITY,
				0, true, null, null, ExecutionTypeRequest.newInstance(),
				SchedulerType.GANG_SCHEDULER, false, globalConstraints, null, yarnRuntimeConf);
		} else {
			containerRequest = new AMRMClient.ContainerRequest(containerResource, null, null, RM_REQUEST_PRIORITY,
				0, true, null, nodeAttributesExpression, ExecutionTypeRequest.newInstance(),
				SchedulerType.FAIR_SCHEDULER, true, null, null, yarnRuntimeConf);
		}
		return containerRequest;
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(
		String containerId,
		String host,
		TaskExecutorProcessSpec taskExecutorProcessSpec) throws Exception {

		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		log.info("TaskExecutor {} will be started on {} with {}.",
			containerId,
			host,
			taskExecutorProcessSpec);

		final Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);

		final String taskManagerDynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerDynamicProperties,
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
	protected void closeTaskManagerConnection(ResourceID resourceID, Exception cause, int exitCode) {
		YarnWorkerNode workerNode = workerNodeMap.get(resourceID);
		if (workerNode != null) {
			stopWorker(workerNode, exitCode);
		}
		super.closeTaskManagerConnection(resourceID, cause, exitCode);
	}

	private void registerMetrics() {
		resourceManagerMetricGroup.gauge("allocatedContainerNum", () -> (TagGaugeStore) () -> {
			List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = new ArrayList<>();
			tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
				workerNodeMap.size(),
				new TagGaugeStore.TagValuesBuilder()
					.addTagValue("cores", String.valueOf(defaultCpus))
					.addTagValue("memory", String.valueOf(defaultTaskManagerMemoryMB))
					.build()));
			return tagGaugeMetrics;
		});
		resourceManagerMetricGroup.gauge("allocatedCPU", () -> getAllocateCpu());
		resourceManagerMetricGroup.gauge("allocatedMemory", () -> getAllocateMemory());
		resourceManagerMetricGroup.gauge("pendingCPU", () -> defaultCpus * getNumRequestedNotAllocatedWorkers());
		resourceManagerMetricGroup.gauge("pendingMemory", () -> defaultTaskManagerMemoryMB * getNumRequestedNotAllocatedWorkers());
		resourceManagerMetricGroup.gauge("pendingRequestedContainerNum", this::getNumRequestedNotAllocatedWorkers);
		resourceManagerMetricGroup.gauge("startingContainers", () -> (TagGaugeStore) () -> {
			long ts = System.currentTimeMillis();
			return getSlowContainerManager().getStartingContainerWithTimestamp().entrySet().stream()
					.map(resourceIDLongEntry -> new TagGaugeStore.TagGaugeMetric(
							ts - resourceIDLongEntry.getValue(),
							new TagGaugeStore.TagValuesBuilder()
									.addTagValue("container_id", pruneContainerId(resourceIDLongEntry.getKey().getResourceIdString()))
									.addTagValue("container_host", getContainerHost(workerNodeMap.get(resourceIDLongEntry.getKey())))
									.build()))
					.collect(Collectors.toList()); });
		resourceManagerMetricGroup.gauge("slowContainerNum", slowContainerManager::getSlowContainerTotalNum);
		resourceManagerMetricGroup.gauge("totalRedundantContainerNum", slowContainerManager::getRedundantContainerTotalNum);
		resourceManagerMetricGroup.gauge("pendingRedundantContainerNum", slowContainerManager::getPendingRedundantContainersTotalNum);
		resourceManagerMetricGroup.gauge("startingRedundantContainerNum", slowContainerManager::getStartingRedundantContainerTotalNum);
		resourceManagerMetricGroup.gauge("speculativeSlowContainerTimeoutMs", slowContainerManager::getSpeculativeSlowContainerTimeoutMs);
		resourceManagerMetricGroup.gauge("completedContainer", completedContainerGauge);
		resourceManagerMetricGroup.counter("gangFailedNum", gangFailedCounter);
		resourceManagerMetricGroup.counter("gangDowngradeNum", gangDowngradeCounter);
		resourceManagerMetricGroup.gauge(EVENT_METRIC_NAME, warehouseJobStartEventMessageRecorder.getJobStartEventMessageSet());
		resourceManagerMetricGroup.gauge(
			MetricNames.NUM_LACK_WORKERS,
			() -> (long) getNumLackWorks());
	}

	private int getNumLackWorks(){
		for (Map.Entry<WorkerResourceSpec, Integer> requiredWorkersPerResourceSpec : getRequiredResources().entrySet()) {
			final WorkerResourceSpec workerResourceSpec = requiredWorkersPerResourceSpec.getKey();
			//There is only one element in requiredWorkersPerResourceSpec, so the result is returned after the first calculation is completed
			return requiredWorkersPerResourceSpec.getValue() -
				(getNumRequestedNotRegisteredWorkersFor(workerResourceSpec) -
					slowContainerManager.getStartingRedundantContainerNum(workerResourceSpec) -
					slowContainerManager.getPendingRedundantContainersNum(workerResourceSpec));
		}
		return 0;
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

	// ------------------------------------------------------------------------
	//	previous container timeout checker
	// ------------------------------------------------------------------------

	private void releasePreviousContainer(Collection<ResourceID> recoveredWorkerNodeSet) {
		for (ResourceID resourceID : recoveredWorkerNodeSet) {
			if (!getTaskExecutors().containsKey(resourceID)) {
				log.info("Container {} not registered in {} ms", resourceID, yarnPreviousContainerTimeoutMs);
				stopWorker(resourceID, WorkerExitCode.PREVIOUS_TM_TIMEOUT);
			}
		}
		requestYarnContainerIfRequired();
	}

	// ------------------------------------------------------------------------
	//	Slow start container
	// ------------------------------------------------------------------------

	private void checkSlowContainers() {
		validateRunsInMainThread();
		try {
			slowContainerManager.checkSlowContainer();
		} catch (Exception e) {
			log.warn("Error while checkSlowContainers.", e);
		} finally {
			scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	protected PendingWorkerNums notifyNewWorkerRequestReleased(WorkerResourceSpec workerResourceSpec) {
		slowContainerManager.notifyPendingWorkerFailed(workerResourceSpec);
		return super.notifyNewWorkerRequestReleased(workerResourceSpec);
	}

	@Override
	protected PendingWorkerNums notifyNewWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		slowContainerManager.notifyWorkerAllocated(workerResourceSpec, resourceID);
		return super.notifyNewWorkerAllocated(workerResourceSpec, resourceID);
	}

	@Override
	protected void notifyAllocatedWorkerStopped(ResourceID resourceID) {
		recoveredWorkerNodeSet.remove(resourceID);
		slowContainerManager.notifyWorkerStopped(resourceID);
		super.notifyAllocatedWorkerStopped(resourceID);
	}

	@Override
	protected void notifyAllocatedWorkerRegistered(ResourceID resourceID) {
		// When previous container is registered, represent recovered worker node been used.
		recoveredWorkerNodeSet.remove(resourceID);
		slowContainerManager.notifyWorkerStarted(resourceID);
		super.notifyAllocatedWorkerRegistered(resourceID);
	}

	@Override
	protected void notifyRecoveredWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		slowContainerManager.notifyRecoveredWorkerAllocated(workerResourceSpec, resourceID);
		super.notifyRecoveredWorkerAllocated(workerResourceSpec, resourceID);
	}

	// ------------------------------------------------------------------------
	//	Smart Resources
	// ------------------------------------------------------------------------

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
					container.getResource().getMemorySize(),
					container.getResource().getVirtualCoresDecimal());
			containerStats.put(resources, containerStats.getOrDefault(resources, 0) + 1);
		}
		List<SmartResourcesStats.ResourcesCount> resourcesCounts = containerStats.entrySet().stream()
			.map(entry -> new SmartResourcesStats.ResourcesCount(
				entry.getKey().getMemoryMB(),
				entry.getKey().getVcores(),
				entry.getValue()))
			.collect(Collectors.toList());
		smartResourceManager.getSmartResourcesStats().setContainersStats(resourcesCounts);
		return CompletableFuture.completedFuture(smartResourceManager.getSmartResourcesStats());
	}

	/**
	 * Init the default container resource.
	 */
	private void initTargetContainerResources() {
		int containerMemorySizeMB = this.defaultTaskManagerMemoryMB;
		double vcores = this.defaultCpus;

		containerMemorySizeMB = new Double(Math.ceil(containerMemorySizeMB / 1024.0)).intValue() * 1024;

		smartResourceManager.setTargetResources(new ContainerResources(containerMemorySizeMB, vcores));
		smartResourceManager.getSmartResourcesStats().setInitialResources(new SmartResourcesStats.Resources(containerMemorySizeMB, vcores));
	}

	private void containerResourcesUpdaterProc() {
		Thread.currentThread().setName("ContainerResourcesUpdaterProc");

		while (true) {
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				return;
			}
			try {
				final UpdateContainersResources updateContainersResources = smartResourceManager.calculateContainerResource();
				runAsync(() -> smartResourceManager.updateContainersResources(updateContainersResources));
			} catch (InterruptedException e) {
				log.info("estimate application resources interrupted");
				return;
			} catch (Throwable e) {
				log.warn("estimate application resources error", e);
				if (e.getMessage() != null && e.getMessage().contains("The duration of the application is too short")) {
					try {
						Thread.sleep(SmartResourceManager.CONTAINER_SKIP_UPDATE_TIME_MS);
					} catch (InterruptedException e1) {
						return;
					}
				}
			}
		}
	}

	/**
	 * Request to update resource of container through rm client.
	 *
	 * @param container container need to update resource
	 * @param request   update container request
	 */
	public void requestContainerUpdate(Container container, UpdateContainerRequest request) {
		resourceManagerClient.requestContainerUpdate(container, request);
	}

	/**
	 *
	 * @param container
	 */
	public void updateContainerResourceAsync(Container container) {
		nodeManagerClient.updateContainerResourceAsync(container);
	}

	public ConcurrentMap<ResourceID, YarnWorkerNode> getWorkerNodeMap() {
		return workerNodeMap;
	}

	@VisibleForTesting
	public Set<ResourceID> getRecoveredWorkerNodeSet() {
		return recoveredWorkerNodeSet;
	}

	@VisibleForTesting
	public Set<String> getYarnBlackedHosts() {
		return yarnBlackedHosts;
	}

	public ResourceID getResourceID(Container container) {
		return new ResourceID(container.getId().toString());
	}

	public static String getContainerHost(YarnWorkerNode yarnWorkerNode) {
		if (yarnWorkerNode != null) {
			return yarnWorkerNode.getContainer().getNodeId().getHost();
		} else {
			return "Unknown";
		}
	}

	@Override
	public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
		log.info("Update container resource success {}", containerId);
		Container updatedContainer = smartResourceManager.getWaitNMUpdate().get(containerId);
		Container oldContainer = workerNodeMap.get(getResourceID(updatedContainer)).getContainer();
		workerNodeMap.put(getResourceID(updatedContainer), new YarnWorkerNode(updatedContainer));
		smartResourceManager.getWaitNMUpdate().remove(containerId);
		log.info("[NM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
			containerId, oldContainer.getResource().getMemorySize(), oldContainer.getResource().getVirtualCores(),
			updatedContainer.getResource().getMemorySize(), updatedContainer.getResource().getVirtualCores());
	}

	@Override
	public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
		log.error("Update container resource error {}", containerId, t);
	}

	private class SlowContainerActionsImpl implements SlowContainerActions {
		@Override
		public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
			validateRunsInMainThread();
			return YarnResourceManager.this.startNewWorker(workerResourceSpec);
		}

		@Override
		public boolean stopWorker(ResourceID resourceID, int exitCode) {
			validateRunsInMainThread();
			return YarnResourceManager.this.stopWorker(resourceID, exitCode);
		}

		@Override
		public void releasePendingRequests(WorkerResourceSpec workerResourceSpec, int expectedNum) {
			validateRunsInMainThread();
			YarnResourceManager.this.removeContainerRequest(workerResourceSpec, expectedNum);
		}

		@Override
		public int getNumRequestedNotAllocatedWorkersFor(WorkerResourceSpec workerResourceSpec) {
			validateRunsInMainThread();
			return YarnResourceManager.this.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec);
		}
	}
}
