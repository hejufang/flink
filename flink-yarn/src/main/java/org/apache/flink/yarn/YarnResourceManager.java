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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.smartresources.ContainerResources;
import org.apache.flink.smartresources.UpdateContainersResources;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import com.bytedance.sr.estimater.client.EstimaterClient;
import com.bytedance.sr.estimater.client.ResourcesUsage;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.ConstraintContent;
import org.apache.hadoop.yarn.api.protocolrecords.GlobalConstraint;
import org.apache.hadoop.yarn.api.protocolrecords.GlobalConstraints;
import org.apache.hadoop.yarn.api.protocolrecords.NodeSkipHighLoadContent;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ConstraintType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.GangSchedulerNotifyContent;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NotifyMsg;
import org.apache.hadoop.yarn.api.records.NotifyMsgType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<YarnWorkerNode>
	implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);
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

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	private final int numberOfTaskSlots;

	private final int defaultTaskManagerMemoryMB;

	private final int defaultCpus;

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
		this.env = env;
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

		this.executor = Executors.newScheduledThreadPool(
			flinkConfig.getInteger(YarnConfigOptions.CONTAINER_LAUNCHER_NUMBER));

		this.webInterfaceUrl = webInterfaceUrl;
		this.numberOfTaskSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.defaultTaskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
		this.defaultCpus = flinkConfig.getInteger(YarnConfigOptions.VCORES, numberOfTaskSlots);
		this.resource = Resource.newInstance(defaultTaskManagerMemoryMB, defaultCpus);

		this.slotsPerWorker = createWorkerSlotProfiles(flinkConfig);

		this.nmClientAsyncEnabled = flinkConfig.getBoolean(YarnConfigOptions.NMCLINETASYNC_ENABLED);

		this.fatalOnGangFailed = true;

		// smart resources
		smartResourcesStats = new SmartResourcesStats();
		boolean smartResourcesEnable = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_ENABLE_KEY,
			ConfigConstants.SMART_RESOURCES_ENABLE_DEFAULT);
		if (!smartResourcesEnable) {
			smartResourcesEnable = flinkConfig.getBoolean(ConfigConstants.SMART_RESOURCES_ENABLE_OLD_KEY,
				ConfigConstants.SMART_RESOURCES_ENABLE_DEFAULT);
		}
		Map<String, Object> smartResourcesConfig = new HashMap<>();
		smartResourcesConfig.put(ConfigConstants.SMART_RESOURCES_ENABLE_KEY, smartResourcesEnable);
		if (smartResourcesEnable) {
			this.pendingUpdating = new HashMap<>();
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

		for (final Container container : containersFromPreviousAttempts) {
			workerNodeMap.put(new ResourceID(container.getId().toString()), new YarnWorkerNode(container));
		}
	}

	protected NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		log.info("Using NMClient");
		NMClient nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);
		return nodeManagerClient;
	}

	protected NMClientAsync createAndStartNodeManagerAsyncClient(YarnConfiguration yarnConfiguration) {
		log.info("Using NMClientAsync");
		// create the async client to communicate with the node managers
		NMClientAsync nodeManagerClientAsync = NMClientAsync.createNMClientAsync(this);
		nodeManagerClientAsync.init(yarnConfiguration);
		nodeManagerClientAsync.start();
		nodeManagerClientAsync.getClient().cleanupRunningContainersOnStop(true);
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
		int currentWorkers = workerNodeMap.size();
		if (workerNumber > (currentWorkers + numPendingContainerRequests)) {
			log.info("Initialize {} workers, we have {} workers from YARN, so request {} new workers",
				workerNumber, currentWorkers, workerNumber - currentWorkers - numPendingContainerRequests);
			return startNewWorkers(resourceProfile, workerNumber - currentWorkers - numPendingContainerRequests);
		} else {
			log.info("Initialize {} workers, but we found {} workers by getContainersFromPreviousAttempts, and pending {} requests.",
				workerNumber, currentWorkers, numPendingContainerRequests);
			return Collections.emptyList();
		}
	}

	@VisibleForTesting
	Resource getContainerResource() {
		return resource;
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}.", container.getId());
		try {
			if (nmClientAsyncEnabled) {
				nodeManagerClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
			} else {
				nodeManagerClient.stopContainer(container.getId(), container.getNodeId());
			}
		} catch (final Exception e) {
			log.warn("Error while calling YARN Node Manager to stop container", e);
		}
		resourceManagerClient.releaseAssignedContainer(container.getId());
		workerNodeMap.remove(workerNode.getResourceID());
		return true;
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
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
			if (!fatalOnGangFailed) {
				if (numPendingContainerRequests >= gangSchedulerNotifyContent.getRequestedContainerNum()) {
					runAsync(() -> {
							final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
							final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();
							for (int i = 0; i < gangSchedulerNotifyContent.getRequestedContainerNum(); i++) {
								removeContainerRequest(pendingRequestsIterator.next());
							}
							try {
								Thread.sleep(flinkConfig.getInteger(YarnConfigOptions.WAIT_TIME_BEFORE_GANG_RETRY_MS));
							} catch (InterruptedException e) {
								log.error("InterruptedException when waiting to retry Request by GangScheduler.", e);
							}
							recordFailureAndStartNewWorkerIfNeeded();
						}
					);
				} else {
					log.info("Failed containerRequests:{} more than pending:{}. It is AMRMClient auto request, ignore.",
						gangSchedulerNotifyContent.getRequestedContainerNum(), numPendingContainerRequests);
				}
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
				log.info("YARN ResourceManager reported the following containers completed: {}.", statuses);
				for (final ContainerStatus containerStatus : statuses) {

					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);

					if (yarnWorkerNode != null) {
						recordFailureAndStartNewWorkerIfNeeded();
					}
					// Eagerly close the connection with task manager.
					closeTaskManagerConnection(resourceId, new Exception(containerStatus.getDiagnostics()));
				}
			}
		);
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
					"Received new container: {} - Remaining pending container requests: {}",
					container.getId(),
					numPendingContainerRequests);

				if (numPendingContainerRequests > 0) {
					removeContainerRequest(pendingRequestsIterator.next());

					final String containerIdStr = container.getId().toString();
					final ResourceID resourceId = new ResourceID(containerIdStr);

					workerNodeMap.put(resourceId, new YarnWorkerNode(container));

					executor.execute(new Runnable() {
						@Override
						public void run() {
							try {
								// Context information used to start a TaskExecutor Java process
								ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
									container.getResource(),
									containerIdStr,
									container.getNodeId().getHost());

								if (nmClientAsyncEnabled) {
									nodeManagerClientAsync.startContainerAsync(container, taskExecutorLaunchContext);
								} else {
									nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
								}
							} catch (Throwable t) {
								log.error("Could not start TaskManager in container {}.", container.getId(), t);

								// release the failed container
								workerNodeMap.remove(resourceId);
								resourceManagerClient.releaseAssignedContainer(container.getId());
								recordFailureAndStartNewWorkerIfNeeded();
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
		numPendingContainerRequests--;

		log.info("Removing container request {}. Pending container requests {}.", pendingContainerRequest, numPendingContainerRequests);

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
		closeAsync();
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
		// and ask for a new one
		startNewWorkerIfNeeded(ResourceProfile.UNKNOWN, numPendingContainerRequests, numberOfTaskSlots);
	}

	private void requestYarnContainer() {
		requestYarnContainers(1);
	}

	private void requestYarnContainers(int containerNumber) {
		boolean useGang = flinkConfig.getBoolean(YarnConfigOptions.GANG_SCHEDULER);
		log.info("Allocate {} containers, Using gang scheduler: {}", containerNumber, useGang);
		ArrayList<AMRMClient.ContainerRequest> containerRequests = new ArrayList<>();
		for (int i = 0; i < containerNumber; i++) {
			containerRequests.add(getContainerRequest(useGang));
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
				hardConstraints.add(load);
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
			containerRequest = new AMRMClient.ContainerRequest(getContainerResource(), null, null,
				RM_REQUEST_PRIORITY, 0, true, null, null,
				ExecutionTypeRequest.newInstance(), SchedulerType.GANG_SCHEDULER, false, globalConstraints);
		} else {
			containerRequest = new AMRMClient.ContainerRequest(
				getContainerResource(),
				null,
				null,
				RM_REQUEST_PRIORITY);
		}
		return containerRequest;
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), numberOfTaskSlots);

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
			final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(new ResourceID(containerId.toString()));
			if (yarnWorkerNode != null) {
				resourceManagerClient.releaseAssignedContainer(containerId);
				recordFailureAndStartNewWorkerIfNeeded();
			}
		});
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
		log.error("Get container status error {}", containerId, throwable);
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
			if (!workerNodeMap.containsKey(getResourceID(updatedContainer.getContainer()))) {
				log.info("Container {} resources was updated but container has complete",
					updatedContainer.getContainer().getId());
				continue;
			}

			try {
				Container old = workerNodeMap.get(getResourceID(updatedContainer.getContainer())).getContainer();
				log.info("[RM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
					updatedContainer.getContainer().getId(),
					old.getResource().getMemory(), old.getResource().getVirtualCores(),
					updatedContainer.getContainer().getResource().getMemory(),
					updatedContainer.getContainer().getResource().getVirtualCores());
				nodeManagerClient.updateContainerResource(updatedContainer.getContainer());
				workerNodeMap.put(getResourceID(updatedContainer.getContainer()),
					new YarnWorkerNode(updatedContainer.getContainer()));
				log.info("[NM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
					old.getId(), old.getResource().getMemory(), old.getResource().getVirtualCores(),
					updatedContainer.getContainer().getResource().getMemory(),
					updatedContainer.getContainer().getResource().getVirtualCores());
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
				} else if (!checkIfCouldUpdateResources(newResources)) {
					srNextCheckTimeMS = System.currentTimeMillis() + srAdjustCheckBackoffMS;
					log.warn("Resources update was rejected by sr check api, original: {}, target: {}",
						targetResources, newResources);
				} else {
					log.info("Container resources updated from {} to {}", targetResources,
						newResources);
					targetResources = newResources;
					smartResourcesStats.updateCurrentResources(
						new SmartResourcesStats.Resources(targetResources.getMemoryMB(), targetResources.getVcores()));
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

			Resource currentResource = container.getResource();
			if (currentResource.getMemory() == targetResources.getMemoryMB()
				&& currentResource.getVirtualCores() == targetResources.getVcores()) {
				continue;
			}

			UpdateContainerRequest request = null;
			if (targetResources.getMemoryMB() >= currentResource.getMemory() &&
				targetResources.getVcores() >= currentResource.getVirtualCores()) {
				// increase all
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					ContainerUpdateType.INCREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), targetResources.getVcores()));
			} else if (targetResources.getMemoryMB() <= currentResource.getMemory() &&
				targetResources.getVcores() <= currentResource.getVirtualCores()) {
				// decrease all
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), targetResources.getVcores()));
			} else if (targetResources.getMemoryMB() != currentResource.getMemory()) {
				// increase | decrease memory
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					targetResources.getMemoryMB() > currentResource.getMemory() ?
						ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), currentResource.getVirtualCores()));
			} else if (targetResources.getVcores() != currentResource.getVirtualCores()) {
				// increase | decrease vcores
				request = UpdateContainerRequest.newInstance(container.getVersion(),
					containerId,
					targetResources.getVcores() > currentResource.getVirtualCores() ?
						ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(currentResource.getMemory(), targetResources.getVcores()));
			}
			try {
				resourceManagerClient.requestContainerUpdate(container, request);
				pendingUpdating.put(containerId, System.currentTimeMillis() + resourcesUpdateTimeoutMS);
				log.info("request update {} resources from ({} MB, {} vcores) to ({} MB, {} vcores)",
					containerId, currentResource.getMemory(), currentResource.getVirtualCores(),
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
			List<ServiceNode> nodes = discovery.translateOne(serviceName);
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
					container.getResource().getMemory(), container.getResource().getVirtualCores());
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

				int newVcores = 0;
				if (srCpuEstimateMode.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_FLOOR)) {
					newVcores = new Double(Math.floor(applicationTotalResources.getCpuTotalVcores()
						* (1 + cpuReserveRatio) / workerNodeMap.size())).intValue();
				} else if (srCpuEstimateMode.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_ROUND)) {
					newVcores = new Long(Math.round(applicationTotalResources.getCpuTotalVcores()
						* (1 + cpuReserveRatio) / workerNodeMap.size())).intValue();
				} else if (srCpuEstimateMode.equals(ConfigConstants.SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL)) {
					newVcores = new Double(Math.ceil(applicationTotalResources.getCpuTotalVcores()
						* (1 + cpuReserveRatio) / workerNodeMap.size())).intValue();
				}
				newVcores = newVcores > 8 ? 8 : newVcores;
				newVcores = newVcores == 0 ? 1 : newVcores;
				final UpdateContainersResources updateContainersResources = new UpdateContainersResources(
					newMemoryMB, newVcores, new Long(containerMaxResources.getDurtion()).intValue());

				runAsync(() -> updateContainersResources(updateContainersResources));

			} catch (InterruptedException e) {
				log.info("estimate application resources interrupted");
				return;
			} catch (Throwable e) {
				log.warn("estimate application resources error", e);
			}
		}
	}

	private void initTargetContainerResources() {
		int containerMemorySizeMB = this.defaultTaskManagerMemoryMB;
		final int vcores = this.defaultCpus;

		containerMemorySizeMB = new Double(Math.ceil(containerMemorySizeMB / 1024.0)).intValue() * 1024;

		targetResources = new ContainerResources(containerMemorySizeMB, vcores);
		smartResourcesStats.setInitialResources(
			new SmartResourcesStats.Resources(containerMemorySizeMB, vcores));
	}

	private ResourceID getResourceID(Container container) {
		return new ResourceID(container.getId().toString());
	}
}
