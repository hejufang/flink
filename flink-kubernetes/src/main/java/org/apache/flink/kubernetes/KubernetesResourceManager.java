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

package org.apache.flink.kubernetes;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.event.WarehouseJobStartEventMessageRecorder;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;

import com.bytedance.openplatform.arcee.ArceeUtils;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NAME;
import static org.apache.flink.kubernetes.utils.KubernetesUtils.genLogUrl;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ActiveResourceManager<KubernetesWorkerNode>
	implements FlinkKubeClient.WatchCallbackHandler<KubernetesPod> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	/** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
	private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

	private final Map<ResourceID, KubernetesWorkerNode> workerNodes = new ConcurrentHashMap<>();

	/** When ResourceManager failover, the max attempt should recover. */
	private long currentMaxAttemptId = 0;

	/** Current max pod index. When creating a new pod, it should increase one. */
	private long currentMaxPodId = 0;

	private final String clusterId;

	private final FlinkKubeClient kubeClient;

	private final KubernetesResourceManagerConfiguration configuration;

	/** Map from pod name to worker resource. */
	private final Map<String, WorkerResourceSpec> podWorkerResources;

	// All pods in Kubernetes Pending phase.
	private final Map<String, Long> pendingPhasePods;

	private final int minimalNodesNum;

	private Optional<KubernetesWatch> podsWatchOpt;

	private volatile boolean running;

	/** Map from pod name to hostIP. */
	private final HashMap<String, String> podNameAndHostIPMap;

	private final Map<WorkerResourceSpec, KubernetesPod.CpuMemoryResource> realResourceToWorkerResourceSpec;

	@Nullable
	private String webInterfaceUrl;

	private final int restServerPort;

	private final int socketServerPort;

	private final String jobManagerPodName;
	private final String region;

	private final boolean enableWebShell;

	private final boolean streamLogEnabled;
	private final String streamLogUrlTemplate;
	private final String streamLogDomain;
	private final String streamLogQueryTemplate;
	private final String streamLogSearchView;

	private final TagGauge completedContainerGauge = new TagGauge.TagGaugeBuilder().setClearAfterReport(true).build();

	private static final String EVENT_METRIC_NAME = "resourceManagerEvent";
	private final WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder;

	/** Whether make recovered WorkerNode as pending working. */
	private final boolean previousContainerAsPending;

	/** Get recovered WorkerNode Set from Kubernetes When AM failover. */
	private final Map<ResourceID, KubernetesPod> recoveredWorkerNodeSet;

	/** Timeout to wait previous container register. */
	private final Long previousContainerTimeoutMs;

	public KubernetesResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			Configuration flinkConfig,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			FlinkKubeClient kubeClient,
			KubernetesResourceManagerConfiguration configuration,
			String webInterfaceUrl,
			FailureRater failureRater,
			Clock clock) {
		super(
			flinkConfig,
			System.getenv(),
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
			failureRater,
			clock);
		this.clusterId = configuration.getClusterId();
		this.minimalNodesNum = flinkConfig.getInteger(KubernetesConfigOptions.KUBERNETES_TASK_MANAGER_MINIMAL_NUM);
		this.kubeClient = kubeClient;
		this.configuration = configuration;
		this.podWorkerResources = new HashMap<>();
		this.pendingPhasePods = new HashMap<>();
		this.running = false;
		this.podNameAndHostIPMap = new HashMap<>();
		this.realResourceToWorkerResourceSpec = new HashMap<>(4);
		this.webInterfaceUrl = webInterfaceUrl;
		this.restServerPort = clusterInformation.getRestServerPort();
		this.socketServerPort = clusterInformation.getSocketServerPort();
		this.enableWebShell = flinkConfig.getBoolean(KubernetesConfigOptions.KUBERNETES_WEB_SHELL_ENABLED);
		this.jobManagerPodName = env.get(ENV_FLINK_POD_NAME);
		this.streamLogEnabled = flinkConfig.getBoolean(KubernetesConfigOptions.STREAM_LOG_ENABLED);
		this.streamLogUrlTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_URL_TEMPLATE);
		this.streamLogDomain = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_DOMAIN);
		this.streamLogQueryTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_QUERY_TEMPLATE);
		this.streamLogSearchView = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_SEARCH_VIEW);
		this.region = flinkConfig.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);

		this.previousContainerAsPending = flinkConfig.getBoolean(ResourceManagerOptions.PREVIOUS_CONTAINER_AS_PENDING);
		this.previousContainerTimeoutMs = flinkConfig.getLong(ResourceManagerOptions.PREVIOUS_CONTAINER_TIMEOUT_MS);
		this.recoveredWorkerNodeSet = new HashMap<>();

		this.warehouseJobStartEventMessageRecorder = new WarehouseJobStartEventMessageRecorder(jobManagerPodName, false);
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		super.initialize();
		updateServiceTargetPortIfNecessary();
		recoverWorkerNodesFromPreviousAttempts();

		podsWatchOpt = watchTaskManagerPods();
		this.running = true;
	}

	private void updateServiceTargetPortIfNecessary() throws ResourceManagerException {
		if (!KubernetesUtils.isHostNetworkEnabled(flinkConfig)) {
			return;
		}
		if (isPortPreAllocated()) {
			// we write the name of port as target port in k8s service. If the ports are allocated by Kubernetes,
			// then the service could automatically update their target port to the allocated port.
			return;
		}
		CompletableFuture<Void> updateFuture = kubeClient
			.updateServiceTargetPort(
				ExternalServiceDecorator.getExternalServiceName(clusterId),
				Constants.REST_PORT_NAME,
				restServerPort);
		if (socketServerPort > 0) {
			updateFuture.thenCompose(o ->
				kubeClient.updateServiceTargetPort(
					ExternalServiceDecorator.getExternalServiceName(clusterId),
					Constants.SOCKET_PORT_NAME,
					socketServerPort));
		}
		if (!HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
			updateFuture.thenCompose(o -> kubeClient
				.updateServiceTargetPort(
					InternalServiceDecorator.getInternalServiceName(clusterId),
					Constants.BLOB_SERVER_PORT_NAME,
					Integer.parseInt(flinkConfig.getString(BlobServerOptions.PORT)))
			).thenCompose(o -> kubeClient
				.updateServiceTargetPort(
					InternalServiceDecorator.getInternalServiceName(clusterId),
					Constants.JOB_MANAGER_RPC_PORT_NAME,
					flinkConfig.getInteger(JobManagerOptions.PORT))
			);
		}
		try {
			updateFuture.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new ResourceManagerException("Can not update target port of service", e);
		}
	}

	private static boolean isPortPreAllocated() {
		// check whether the rpc, blob server, and rest port are pre-allocated by kubernetes
		// if so, there will be environment variables like "PORT", "PORT0", "PORT1"
		return !StringUtils.isNullOrWhitespaceOnly(System.getenv(Constants.PORT0_ENV));
	}

	@Override
	public void onStart() throws Exception {
		super.onStart();
		registerMetrics();
	}

	@Override
	public CompletableFuture<Void> onStop() {
		LOG.info("stopping");
		if (!running) {
			return FutureUtils.completedVoidFuture();
		}
		running = false;

		// shut down all components
		Throwable throwable = null;

		try {
			podsWatchOpt.ifPresent(KubernetesWatch::close);
		} catch (Throwable t) {
			throwable = t;
		}

		return getStopTerminationFutureOrCompletedExceptionally(throwable).whenComplete((ignored, t) -> {
			kubeClient.stopAndCleanupCluster(clusterId);
			LOG.info("delete the cluster {}", clusterId);

			try {
				kubeClient.close();
			} catch (Throwable t1) {
				LOG.error("There is some error while closing the kubeClient.", t1);
			}
			LOG.info("kubeClient closed.");
		});
	}

	@Override
	protected void startServicesOnLeadership() {
		if (!recoveredWorkerNodeSet.isEmpty() && previousContainerTimeoutMs > 0) {
			log.info("Will check {} previous container timeout in {} ms.", recoveredWorkerNodeSet.size(), previousContainerTimeoutMs);
			Set<ResourceID> workersToCheckTimeout = new HashSet<>(recoveredWorkerNodeSet.keySet());
			scheduleRunAsync(
					() -> releasePreviousContainer(workersToCheckTimeout),
					previousContainerTimeoutMs,
					TimeUnit.MILLISECONDS);
		}
		super.startServicesOnLeadership();
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
		LOG.info(
			"Stopping kubernetes cluster, clusterId: {}, diagnostics: {}",
			clusterId,
			diagnostics == null ? "" : diagnostics);
		// TODO: 2021/9/15 here change the behavior of register application in k8s, but not changed in yarn, we need to look with the community at how to fix it better
		//kubeClient.stopAndCleanupCluster(clusterId);
	}

	@Override
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		requestKubernetesPod(workerResourceSpec, 1);
		return true;
	}

	@Override
	public boolean startNewWorkers(WorkerResourceSpec workerResourceSpec, int resourceNumber) {
		LOG.info("Starting new worker with worker resource spec: {}, " +
			"and request resource number: {}", workerResourceSpec, resourceNumber);
		requestKubernetesPod(workerResourceSpec, resourceNumber);
		return true;
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		KubernetesWorkerNode node = workerNodes.get(resourceID);
		if (node != null) {
			warehouseJobStartEventMessageRecorder.registerTaskManagerFinish(resourceID.getResourceIdString());
		}
		return node;
	}

	@Override
	public void removePendingContainerRequest(WorkerResourceSpec workerResourceSpec, int expectedNum) {
		List<String> pendingPods = podWorkerResources.entrySet().stream()
				.filter(e -> !workerNodes.containsKey(new ResourceID(e.getKey())))
				.filter(e -> e.getValue().equals(workerResourceSpec))
				.map(Map.Entry::getKey)
				.collect(Collectors.toList());
		int remainingExceptedNum = expectedNum;
		for (String podName : pendingPods) {
			if (remainingExceptedNum <= 0) {
				break;
			}
			log.info("Stopping pending worker {}", podName);
			internalStopPod(podName);
			remainingExceptedNum--;
		}
	}

	@Override
	public boolean stopWorker(ResourceID resourceID, int exitCode) {
		KubernetesWorkerNode workerNode = workerNodes.get(resourceID);
		if (workerNode != null) {
			return stopWorker(workerNode, exitCode);
		} else {
			return true;
		}
	}

	@Override
	public boolean stopWorkerAndStartNewIfRequired(ResourceID resourceID, int exitCode) {
		boolean stopResult = stopWorker(resourceID, exitCode);
		requestKubernetesPodIfRequired();
		return stopResult;
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker) {
		return stopWorker(worker, WorkerExitCode.UNKNOWN);
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker, int exitCode) {
		final ResourceID resourceId = worker.getResourceID();
		// In Kubernetes, there is a minimal number of workers which are reserved to avoid
		// allocating resources frequently. If the remaining worker number is less than
		// the minimal worker number, the worker will not be stopped.
		if (minimalNodesNum > 0 && !canStopWorker()) {
			LOG.debug("Skip stopping worker {} with remaining worker number[{}] and "
					+ "minimal worker number[{}]", resourceId, getTaskExecutors().size(), minimalNodesNum);
			return false;
		}

		if (workerNodes.containsKey(resourceId)) {
			completedContainerGauge.addMetric(
					1,
					new TagGaugeStoreImpl.TagValuesBuilder()
							.addTagValue("pod_host", getPodHost(resourceId))
							.addTagValue("pod_name", prunePodName(resourceId.getResourceIdString()))
							.addTagValue("exit_code", String.valueOf(exitCode))
							.build());
		}
		LOG.info("Stopping Worker {} with exit code {}", resourceId, exitCode);
		internalStopPod(resourceId.toString());
		return true;
	}

	@Override
	protected void closeTaskManagerConnection(ResourceID resourceID, Exception cause, int exitCode) {
		KubernetesWorkerNode workerNode = workerNodes.get(resourceID);
		if (workerNode != null) {
			stopWorker(workerNode, exitCode);
		}
		super.closeTaskManagerConnection(resourceID, cause, exitCode);
	}

	@Override
	protected void notifyAllocatedWorkerStopped(ResourceID resourceID) {
		if (recoveredWorkerNodeSet.remove(resourceID) != null) {
			log.debug("Previous pod {} stopped", resourceID.getResourceIdString());
		}
		super.notifyAllocatedWorkerStopped(resourceID);
	}

	@Override
	protected void notifyAllocatedWorkerRegistered(ResourceID resourceID) {
		if (recoveredWorkerNodeSet.remove(resourceID) != null) {
			log.debug("Previous pod {} started", resourceID.getResourceIdString());
		}
		super.notifyAllocatedWorkerRegistered(resourceID);
	}

	@Override
	public void onAdded(List<KubernetesPod> pods) {
		runAsync(() -> {
			int duplicatePodNum = 0;
			for (KubernetesPod pod : pods) {
				final String podName = pod.getName();
				final ResourceID resourceID = new ResourceID(podName);

				if (workerNodes.containsKey(resourceID)) {
					log.debug("Ignore TaskManager pod that is already added: {}", podName);
					++duplicatePodNum;
					continue;
				}

				final WorkerResourceSpec workerResourceSpec = Preconditions.checkNotNull(
					podWorkerResources.get(podName),
					"Unrecognized pod " + podName + ". Pods from previous attempt should have already been added.");

				final int pendingNum = getNumRequestedNotAllocatedWorkersFor(workerResourceSpec);
				Preconditions.checkState(pendingNum > 0, "Should not receive more workers than requested.");

				notifyNewWorkerAllocated(workerResourceSpec, resourceID);
				final KubernetesWorkerNode worker = new KubernetesWorkerNode(resourceID);
				workerNodes.put(resourceID, worker);
				warehouseJobStartEventMessageRecorder.startContainerStart(podName);

				log.info("Received new TaskManager pod: {}", podName);
			}
			log.info("Received {} new TaskManager pods. Remaining pending pod requests: {}",
				pods.size() - duplicatePodNum, getNumRequestedNotAllocatedWorkers());
		});
	}

	@Override
	public void onModified(List<KubernetesPod> pods) {
		runAsync(() -> {
			for (KubernetesPod pod : pods) {
				PodStatus status = pod.getInternalResource().getStatus();
				if (status != null) {
					String podName = pod.getName();
					String hostIP = status.getHostIP();
					if (!StringUtils.isNullOrWhitespaceOnly(hostIP) && !hostIP.equals(podNameAndHostIPMap.get(podName))) {
						podNameAndHostIPMap.put(podName, hostIP);
						LOG.info("modified event: a taskManager pod, it's podName: {}, hostIP: {}", podName, hostIP);
					}
				}
				handlePendingPodToRunning(pod);
				removePodAndTryRestartIfRequired(pod);
			}
		});
	}

	@Override
	public void onDeleted(List<KubernetesPod> pods) {
		runAsync(() -> {
			for (KubernetesPod pod : pods) {
				recordWorkerFailureAndStop(pod, new FlinkException("Pod Deleted."), WorkerExitCode.POD_DELETED);
			}
		});
	}

	@Override
	public void onError(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodAndTryRestartIfRequired));
	}

	@Override
	public void handleError(Throwable throwable) {
		if (throwable instanceof KubernetesTooOldResourceVersionException) {
			getMainThreadExecutor()
				.execute(
					() -> {
						if (running) {
							podsWatchOpt.ifPresent(KubernetesWatch::close);
							log.info("Creating a new watch on TaskManager pods.");
							podsWatchOpt = watchTaskManagerPods();
						}
					});
		} else {
			onFatalError(throwable);
		}
	}

	private KubernetesPod.CpuMemoryResource getRealResourceByWorkerSpec(WorkerResourceSpec workerResourceSpec) {
		if (realResourceToWorkerResourceSpec.containsKey(workerResourceSpec)) {
			return realResourceToWorkerResourceSpec.get(workerResourceSpec);
		} else {
			log.warn("Get real resource by worker spec {} failed.", workerResourceSpec);
			return new KubernetesPod.CpuMemoryResource(0, 0);
		}
	}

	/**
	 * Only keep {attemptId}-{podIndex}.
	 */
	private String prunePodName(String podName) {
		if (!StringUtils.isNullOrWhitespaceOnly(podName)) {
			String[] podNameParts = podName.split("-");
			int length = podNameParts.length;
			if (length > 2) {
				return podNameParts[length - 2] + "-" + podNameParts[length - 1];
			}
			return podName;
		}
		return podName;
	}

	private String getPodHost(ResourceID resourceID) {
		return getPodHost(resourceID.getResourceIdString());
	}

	private String getPodHost(String podName) {
		return podNameAndHostIPMap.getOrDefault(podName, "unKnown");
	}

	private void registerMetrics() {
		resourceManagerMetricGroup.gauge(MetricNames.ALLOCATED_CONTAINER_NUM, () -> (TagGaugeStore) () ->
				Stream.concat(
						getNumAllocatedWorkersDetail().entrySet().stream()
								.map(workerNumberEntry -> {
									KubernetesPod.CpuMemoryResource realResource = getRealResourceByWorkerSpec(workerNumberEntry.getKey());
									return new TagGaugeStore.TagGaugeMetric(
											workerNumberEntry.getValue(),
											new TagGaugeStore.TagValuesBuilder()
													.addTagValue("cores", String.valueOf(realResource.getCpu()))
													.addTagValue("memory", String.valueOf(realResource.getMemoryInMB()))
													.build());
								}),
						recoveredWorkerNodeSet.entrySet().stream()
								.collect(Collectors.groupingBy(entry -> entry.getValue().getMainContainerResource(), Collectors.counting()))
								.entrySet().stream().map(entry -> new TagGaugeStore.TagGaugeMetric(
										entry.getValue(),
										new TagGaugeStore.TagValuesBuilder()
												.addTagValue("cores", String.valueOf(entry.getKey().getCpu()))
												.addTagValue("memory", String.valueOf(entry.getKey().getMemoryInMB()))
												.build()))
						)
						.collect(Collectors.toList()));

		resourceManagerMetricGroup.gauge(MetricNames.PENDING_REQUESTED_CONTAINER_NUM, () -> (TagGaugeStore) () ->
				getNumRequestedNotAllocatedWorkersDetail().entrySet().stream()
						.map(workerNumberEntry -> {
							KubernetesPod.CpuMemoryResource realResource = getRealResourceByWorkerSpec(workerNumberEntry.getKey());
							return new TagGaugeStore.TagGaugeMetric(
									workerNumberEntry.getValue(),
									new TagGaugeStore.TagValuesBuilder()
											.addTagValue("cores", String.valueOf(realResource.getCpu()))
											.addTagValue("memory", String.valueOf(realResource.getMemoryInMB()))
											.build());
						})
						.collect(Collectors.toList()));

		resourceManagerMetricGroup.gauge(MetricNames.STARTING_CONTAINERS, () -> (TagGaugeStore) () -> {
			long ts = System.currentTimeMillis();
			return getWorkerAllocatedTime().entrySet().stream()
					.map(resourceIDLongEntry -> new TagGaugeStore.TagGaugeMetric(
							ts - resourceIDLongEntry.getValue(),
							new TagGaugeStore.TagValuesBuilder()
									.addTagValue("pod_name", prunePodName(resourceIDLongEntry.getKey().getResourceIdString()))
									.addTagValue("pod_host", getPodHost(resourceIDLongEntry.getKey()))
									.build()))
					.collect(Collectors.toList()); });

		resourceManagerMetricGroup.gauge(MetricNames.ALLOCATED_CPU, () -> DoubleStream.concat(
				getNumAllocatedWorkersDetail().entrySet().stream().mapToDouble(value -> getRealResourceByWorkerSpec(value.getKey()).getCpu() * value.getValue()),
				recoveredWorkerNodeSet.values().stream().mapToDouble(pod -> pod.getMainContainerResource().getCpu()))
				.sum());
		resourceManagerMetricGroup.gauge(MetricNames.ALLOCATED_MEMORY, () -> LongStream.concat(
				getNumAllocatedWorkersDetail().entrySet().stream().mapToLong(value -> (long) getRealResourceByWorkerSpec(value.getKey()).getMemoryInMB() * value.getValue()),
				recoveredWorkerNodeSet.values().stream().mapToLong(pod -> pod.getMainContainerResource().getMemoryInMB()))
				.sum());
		resourceManagerMetricGroup.gauge(MetricNames.PENDING_CPU, () -> getNumRequestedNotAllocatedWorkersDetail().entrySet().stream()
				.mapToDouble(value -> getRealResourceByWorkerSpec(value.getKey()).getCpu() * value.getValue())
				.sum());
		resourceManagerMetricGroup.gauge(MetricNames.PENDING_MEMORY, () -> getNumRequestedNotAllocatedWorkersDetail().entrySet().stream()
				.mapToDouble(value -> getRealResourceByWorkerSpec(value.getKey()).getMemoryInMB() * value.getValue())
				.sum());

		resourceManagerMetricGroup.gauge(MetricNames.PENDING_PHASE_PODS, () -> (TagGaugeStore) () -> {
			long ts = System.currentTimeMillis();
			return pendingPhasePods.entrySet().stream()
					.map(podNameWithInitTs -> new TagGaugeStore.TagGaugeMetric(
							ts - podNameWithInitTs.getValue(),
							new TagGaugeStore.TagValuesBuilder()
									.addTagValue("pod_name", prunePodName(podNameWithInitTs.getKey()))
									.addTagValue("pod_host", getPodHost(podNameWithInitTs.getKey()))
									.build()))
					.collect(Collectors.toList()); });

		resourceManagerMetricGroup.gauge(MetricNames.COMPLETED_CONTAINER, completedContainerGauge);

		resourceManagerMetricGroup.gauge(EVENT_METRIC_NAME, warehouseJobStartEventMessageRecorder.getJobStartEventMessageSet());
	}

	public CompletableFuture<String> requestJMWebShell(@RpcTimeout Time timeout) {
		CompletableFuture<String> jmWebShell = new CompletableFuture<>();
		if (enableWebShell) {
			if (flinkConfig.getBoolean(KubernetesConfigOptions.ARCEE_ENABLED)) {
				try {
					String webShell = ArceeUtils.getJMWebShell();
					jmWebShell.complete(webShell);
				} catch (Throwable t) {
					LOG.warn("Get JobManager web shell error.", t);
					jmWebShell.complete("");
				}
			} else {
				String podName = System.getenv(ENV_FLINK_POD_NAME);
				String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
				String webShell = KubernetesUtils.getWebShell(podName, namespace);
				jmWebShell.complete(webShell);
			}
		} else {
			jmWebShell.complete("");
		}
		return jmWebShell;
	}

	public String getTaskManagerWebShell(ResourceID resourceId, String host) {
		if (enableWebShell) {
			String podName = resourceId.getResourceIdString();
			String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
			if (flinkConfig.getBoolean(KubernetesConfigOptions.ARCEE_ENABLED)) {
				try {
					return ArceeUtils.getTMWebShell(namespace, this.clusterId, podName, KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME);
				} catch (Throwable t) {
					LOG.warn("Get TaskManager web shell error.", t);
					return super.getTaskManagerWebShell(resourceId, host);
				}
			} else {
				return KubernetesUtils.getWebShell(podName, namespace);
			}
		} else {
			return super.getTaskManagerWebShell(resourceId, host);
		}
	}

	public CompletableFuture<String> requestJobManagerLogUrl(@RpcTimeout Time timeout) {
		if (streamLogEnabled) {
			CompletableFuture<String> jmLog = new CompletableFuture<>();
			try {
				if (flinkConfig.getBoolean(KubernetesConfigOptions.ARCEE_ENABLED)) {
					String jmLogStr = ArceeUtils.getJMLogUrl();
					jmLog.complete(jmLogStr);
					return jmLog;
				} else if (!StringUtils.isNullOrWhitespaceOnly(streamLogDomain)) {
					String jmLogStr = genLogUrl(streamLogUrlTemplate, streamLogDomain, streamLogQueryTemplate, jobManagerPodName, region, streamLogSearchView);
					jmLog.complete(jmLogStr);
					return jmLog;
				} else {
					return super.requestJobManagerLogUrl(timeout);
				}
			} catch (Throwable t) {
				LOG.warn("Get JobManager log url error.", t);
				return super.requestJobManagerLogUrl(timeout);
			}
		}
		return super.requestJobManagerLogUrl(timeout);
	}

	@Override
	public String getTaskManagerLogUrl(ResourceID resourceId, String host) {
		if (streamLogEnabled) {
			try {
				if (flinkConfig.getBoolean(KubernetesConfigOptions.ARCEE_ENABLED)) {
					String podName = resourceId.getResourceIdString();
					return ArceeUtils.getTMLogUrl(podName);
				} else if (!StringUtils.isNullOrWhitespaceOnly(streamLogDomain)) {
					return genLogUrl(streamLogUrlTemplate, streamLogDomain, streamLogQueryTemplate,
						resourceId.getResourceIdString(), region, streamLogSearchView);
				}
			} catch (Throwable t) {
				LOG.warn("Get TaskManager {} log url error.", resourceId, t);
				return super.getTaskManagerLogUrl(resourceId, host);
			}
		} else {
			return super.getTaskManagerLogUrl(resourceId, host);
		}
		return super.getTaskManagerLogUrl(resourceId, host);
	}

	@VisibleForTesting
	Map<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodes;
	}

	@VisibleForTesting
	public Map<ResourceID, KubernetesPod> getRecoveredWorkerNodeSet() {
		return recoveredWorkerNodeSet;
	}

	@VisibleForTesting
	public Map<String, WorkerResourceSpec> getPodWorkerResources() {
		return podWorkerResources;
	}

	private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
		for (KubernetesPod pod : podList) {
			final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
			workerNodes.put(worker.getResourceID(), worker);
			final long attempt = worker.getAttempt();
			if (attempt > currentMaxAttemptId) {
				currentMaxAttemptId = attempt;
			}
			if (previousContainerAsPending) {
				recoveredWorkerNodeSet.put(worker.getResourceID(), pod);
			}
		}

		log.info("Recovered {} pods from previous attempts, current attempt id is {}.",
			workerNodes.size(),
			++currentMaxAttemptId);
	}

	private void requestKubernetesPod(WorkerResourceSpec workerResourceSpec, int podNumber) {
		int requestPodNumber = fetchFromRecoveredContainers(workerResourceSpec, podNumber);

		long ts = System.currentTimeMillis();
		for (int i = 0; i < requestPodNumber; i++) {
			final KubernetesTaskManagerParameters parameters =
				createKubernetesTaskManagerParameters(workerResourceSpec);
			warehouseJobStartEventMessageRecorder.createTaskManagerContextStart(parameters.getPodName());

			podWorkerResources.put(parameters.getPodName(), workerResourceSpec);
			pendingPhasePods.put(parameters.getPodName(), ts);
			int pendingWorkerNum = notifyNewWorkerRequested(workerResourceSpec).getNumNotAllocated();
			log.info("Requesting new TaskManager pod {} with <{},{}>. Number pending requests {}.",
				parameters.getPodName(),
				parameters.getTaskManagerMemoryMB(),
				parameters.getTaskManagerCPU(),
				pendingWorkerNum);

			realResourceToWorkerResourceSpec.putIfAbsent(
					workerResourceSpec,
					new KubernetesPod.CpuMemoryResource(parameters.getTaskManagerCPU(), parameters.getTaskManagerMemoryMB()));

			final KubernetesPod taskManagerPod =
				KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);

			warehouseJobStartEventMessageRecorder.createTaskManagerContextFinish(parameters.getPodName());
			warehouseJobStartEventMessageRecorder.createPodRpcStart(parameters.getPodName());
			kubeClient.createTaskManagerPod(taskManagerPod)
				.whenCompleteAsync(
					(ignore, throwable) -> {
						if (throwable != null) {
							final Time retryInterval = configuration.getPodCreationRetryInterval();
							log.warn("Could not start TaskManager in pod {}, retry in {}. ",
								taskManagerPod.getName(), retryInterval, throwable);
							podWorkerResources.remove(parameters.getPodName());
							pendingPhasePods.remove(parameters.getPodName());
							notifyNewWorkerAllocationFailed(workerResourceSpec);
							scheduleRunAsync(
								this::requestKubernetesPodIfRequired,
								retryInterval);

							completedContainerGauge.addMetric(
									1,
									new TagGaugeStoreImpl.TagValuesBuilder()
											.addTagValue("pod_host", getPodHost(parameters.getPodName()))
											.addTagValue("pod_name", prunePodName(parameters.getPodName()))
											.addTagValue("exit_code", String.valueOf(WorkerExitCode.START_CONTAINER_ERROR))
											.build());
						} else {
							log.info("TaskManager {} will be started with {}.", parameters.getPodName(), workerResourceSpec);
						}
						warehouseJobStartEventMessageRecorder.createPodRpcFinish(parameters.getPodName());
					},
					getMainThreadExecutor());
		}
	}

	private int fetchFromRecoveredContainers(WorkerResourceSpec workerResourceSpec, final int workerNumber) {
		final KubernetesTaskManagerParameters requestedKubernetesTaskManagerParameters =
				createKubernetesTaskManagerParameters(workerResourceSpec, "Default");

		int remainingRequestedWorkerNumber = workerNumber;

		Iterator<Map.Entry<ResourceID, KubernetesPod>> workerIterator = recoveredWorkerNodeSet.entrySet().iterator();

		while (workerIterator.hasNext()) {
			if (remainingRequestedWorkerNumber <= 0){
				break;
			}
			Map.Entry<ResourceID, KubernetesPod> worker = workerIterator.next();
			ResourceID resourceID = worker.getKey();
			KubernetesPod pod = worker.getValue();
			if (workerNodes.containsKey(resourceID)) {
				if (requestedKubernetesTaskManagerParameters.matchKubernetesPod(pod)) {
					remainingRequestedWorkerNumber--;
					workerIterator.remove();
					notifyRecoveredWorkerAllocated(workerResourceSpec, resourceID);
				}
			} else {
				log.error("Worker {} is previous but not in workerNodes.", resourceID);
				workerIterator.remove();
			}
		}

		if (workerNumber - remainingRequestedWorkerNumber > 0) {
			log.info("Request {} worker from previous containers, remaining previous container {}", workerNumber - remainingRequestedWorkerNumber, recoveredWorkerNodeSet.size());
		}

		return remainingRequestedWorkerNumber;
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(WorkerResourceSpec workerResourceSpec) {
		final String podName = String.format(
				TASK_MANAGER_POD_FORMAT,
				clusterId,
				currentMaxAttemptId,
				++currentMaxPodId);
		return createKubernetesTaskManagerParameters(workerResourceSpec, podName);
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(WorkerResourceSpec workerResourceSpec, String podName) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		final String dynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, flinkConfig);

		return new KubernetesTaskManagerParameters(
			flinkConfig,
			podName,
			dynamicProperties,
			taskManagerParameters,
			getMinNumberOfTaskManagerForPodGroup(),
			ExternalResourceUtils.getExternalResources(flinkConfig, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
	}

	private Optional<KubernetesWatch> watchTaskManagerPods() {
		return Optional.of(
			kubeClient.watchPodsAndDoCallback(
				KubernetesUtils.getTaskManagerLabels(clusterId),
				this));
	}

	/**
	 * Request new pod if pending pods cannot satisfy pending slot requests.
	 */
	private void requestKubernetesPodIfRequired() {
		for (Map.Entry<WorkerResourceSpec, Integer> entry : getRequiredResources().entrySet()) {
			final WorkerResourceSpec workerResourceSpec = entry.getKey();
			final int requiredTaskManagers = entry.getValue();

			log.info("Check whether need to require new task manager pod, requiredTaskManagers: {} NumRequestedNotRegisteredWorkersFor: {}, redundantWorkerNumber: {}",
					requiredTaskManagers,
					getNumRequestedNotRegisteredWorkersFor(workerResourceSpec),
					getSlowContainerManager().getRedundantContainerNum(workerResourceSpec));

			while (requiredTaskManagers > (getNumRequestedNotRegisteredWorkersFor(workerResourceSpec) - getSlowContainerManager().getRedundantContainerNum(workerResourceSpec))) {
				tryStartNewWorker(workerResourceSpec);
			}
		}
	}

	private void handlePendingPodToRunning(KubernetesPod pod) {
		if (pendingPhasePods.containsKey(pod.getName()) && pod.isRunning()) {
			log.debug("Pod {} phase switch from Pending to Running.", pod.getName());
			pendingPhasePods.remove(pod.getName());
			warehouseJobStartEventMessageRecorder.startContainerFinish(pod.getName());
			warehouseJobStartEventMessageRecorder.registerTaskManagerStart(pod.getName());
		}
	}

	private void removePodAndTryRestartIfRequired(KubernetesPod pod) {
		if (pod.isTerminated()) {
			recordWorkerFailureAndStop(pod, new FlinkException("Pod Terminated."), WorkerExitCode.POD_TERMINATED);
		}
	}

	private void recordWorkerFailureAndStop(KubernetesPod pod, Exception cause, int exitCode) {
		recordWorkerFailure();
		if (podWorkerResources.containsKey(pod.getName())) {
			Optional<ContainerStateTerminated> containerStateTerminatedOptional = pod.getContainerStateTerminated();
			int containerExitCode = exitCode;
			if (containerStateTerminatedOptional.isPresent()) {
				if (containerStateTerminatedOptional.get().getExitCode() != null) {
					containerExitCode = containerStateTerminatedOptional.get().getExitCode();
				}
				log.error("Pod {} failed with container terminated: {}", pod.getName(), containerStateTerminatedOptional.get());
			}
			completedContainerGauge.addMetric(
					1,
					new TagGaugeStoreImpl.TagValuesBuilder()
							.addTagValue("pod_host", getPodHost(pod.getName()))
							.addTagValue("pod_name", prunePodName(pod.getName()))
							.addTagValue("exit_code", String.valueOf(containerExitCode))
							.build());
		}
		internalStopPod(pod.getName());
		requestKubernetesPodIfRequired();
		closeTaskManagerConnection(new ResourceID(pod.getName()), cause, exitCode);
	}

	private void internalStopPod(String podName) {
		String hostIP = podNameAndHostIPMap.remove(podName);
		if (!StringUtils.isNullOrWhitespaceOnly(hostIP)){
			LOG.info("deleted event: a taskManager pod, it's podName: {}, hostIP: {}", podName, hostIP);
		}

		final ResourceID resourceId = new ResourceID(podName);
		final boolean isPendingWorkerOfCurrentAttempt = isPendingWorkerOfCurrentAttempt(podName);

		kubeClient.stopPod(podName)
			.whenComplete(
				(ignore, throwable) -> {
					if (throwable != null) {
						log.warn("Could not stop TaskManager in pod {}.", podName, throwable);
					}
				}
			);

		final WorkerResourceSpec workerResourceSpec = podWorkerResources.remove(podName);
		pendingPhasePods.remove(podName);
		workerNodes.remove(resourceId);

		if (isPendingWorkerOfCurrentAttempt) {
			notifyNewWorkerAllocationFailed(
				Preconditions.checkNotNull(workerResourceSpec,
					"Worker resource spec of current attempt pending worker should be known."));
		} else {
			notifyAllocatedWorkerStopped(resourceId);
		}
	}

	private boolean isPendingWorkerOfCurrentAttempt(String podName) {
		return podWorkerResources.containsKey(podName) &&
			!workerNodes.containsKey(new ResourceID(podName));
	}

	private void releasePreviousContainer(Collection<ResourceID> recoveredWorkerNodeSet) {
		for (ResourceID resourceID : recoveredWorkerNodeSet) {
			if (!getTaskExecutors().containsKey(resourceID)) {
				KubernetesWorkerNode node = workerNodes.get(resourceID);
				if (node != null) {
					log.error("Previous container {} not registered in {} ms", resourceID, previousContainerTimeoutMs);
					stopWorker(node, WorkerExitCode.PREVIOUS_TM_TIMEOUT);
				}
			}
		}
		requestKubernetesPodIfRequired();
	}

	/**
	 * Check whether a worker could be stopped. A worker could be stopped if the remaining
	 * task managers are more than the required minimal number.
	 *
	 * @return true if the worker could be stopped, or false if the worker could not be stopped.
	 */
	private synchronized boolean canStopWorker() {
		return getTaskExecutors().size() > minimalNodesNum;
	}
}
