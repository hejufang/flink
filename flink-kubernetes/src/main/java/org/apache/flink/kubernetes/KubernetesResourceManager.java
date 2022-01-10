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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
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
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.PodStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

	/** Running taskmanager pods number which is not stopping. Only work when enable minimal taskmanager pods.*/
	private final AtomicInteger runningNodesNum = new AtomicInteger(0);

	/** When ResourceManager failover, the max attempt should recover. */
	private long currentMaxAttemptId = 0;

	/** Current max pod index. When creating a new pod, it should increase one. */
	private long currentMaxPodId = 0;

	private final String clusterId;

	private final FlinkKubeClient kubeClient;

	private final KubernetesResourceManagerConfiguration configuration;

	/** Map from pod name to worker resource. */
	private final Map<String, WorkerResourceSpec> podWorkerResources;

	private final int minimalNodesNum;

	private Optional<KubernetesWatch> podsWatchOpt;

	private volatile boolean running;

	/** Map from pod name to hostIP. */
	private final HashMap<String, String> podNameAndHostIPMap;

	private final String jobManagerPodName;
	private final String region;

	private final boolean enableWebShell;

	private final boolean streamLogEnabled;
	private final String streamLogUrlTemplate;
	private final String streamLogDomain;
	private final String streamLogQueryTemplate;
	private final String streamLogSearchView;
	private final int streamLogQueryRange;

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
			FailureRater failureRater) {
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
			failureRater);
		this.clusterId = configuration.getClusterId();
		this.minimalNodesNum = flinkConfig.getInteger(KubernetesConfigOptions.KUBERNETES_TASK_MANAGER_MINIMAL_NUM);
		this.kubeClient = kubeClient;
		this.configuration = configuration;
		this.podWorkerResources = new HashMap<>();
		this.running = false;
		this.podNameAndHostIPMap = new HashMap<>();
		this.enableWebShell = flinkConfig.getBoolean(KubernetesConfigOptions.KUBERNETES_WEB_SHELL_ENABLED);
		this.jobManagerPodName = env.get(ENV_FLINK_POD_NAME);
		this.streamLogEnabled = flinkConfig.getBoolean(KubernetesConfigOptions.STREAM_LOG_ENABLED);
		this.streamLogUrlTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_URL_TEMPLATE);
		this.streamLogDomain = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_DOMAIN);
		this.streamLogQueryTemplate = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_QUERY_TEMPLATE);
		this.streamLogSearchView = flinkConfig.getString(KubernetesConfigOptions.STREAM_LOG_SEARCH_VIEW);
		this.region = flinkConfig.getString(ConfigConstants.DC_KEY, ConfigConstants.DC_DEFAULT);
		this.streamLogQueryRange = flinkConfig.getInteger(KubernetesConfigOptions.STREAM_LOG_QUERY_RANGE_SECONDS);
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		recoverWorkerNodesFromPreviousAttempts();

		podsWatchOpt = watchTaskManagerPods();
		this.running = true;
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
		return workerNodes.get(resourceID);
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker) {
		final ResourceID resourceId = worker.getResourceID();
		// In Kubernetes, there is a minimal number of workers which are reserved to avoid
		// allocating resources frequently. If the remaining worker number is less than
		// the minimal worker number, the worker will not be stopped.
		if (minimalNodesNum > 0 && !canStopWorker()) {
			LOG.debug("Skip stopping worker {} with remaining worker number[{}] and "
					+ "minimal worker number[{}]", resourceId, runningNodesNum.get(), minimalNodesNum);
			return false;
		}
		LOG.info("Stopping Worker {}.", resourceId);
		internalStopPod(resourceId.toString());
		return true;
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
				if (minimalNodesNum > 0) {
					runningNodesNum.incrementAndGet();
				}

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
				removePodAndTryRestartIfRequired(pod);
			}
		});
	}

	@Override
	public void onDeleted(List<KubernetesPod> pods) {
		runAsync(() -> {
			for (KubernetesPod pod : pods) {
				String hostIP = podNameAndHostIPMap.remove(pod.getName());
				if (!StringUtils.isNullOrWhitespaceOnly(hostIP)){
					LOG.info("deleted event: a taskManager pod, it's podName: {}, hostIP: {}", pod.getName(), hostIP);
				}
				removePodAndTryRestartIfRequired(pod);
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

	public CompletableFuture<String> requestJMWebShell(@RpcTimeout Time timeout) {
		CompletableFuture<String> jmWebShell = new CompletableFuture<>();
		if (enableWebShell) {
			String podName = System.getenv(ENV_FLINK_POD_NAME);
			String namespace =  flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
			String webShell = KubernetesUtils.getWebShell(podName, namespace);
			jmWebShell.complete(webShell);
		} else {
			jmWebShell.complete("");
		}
		return jmWebShell;
	}

	public String getTaskManagerWebShell(ResourceID resourceId, String host) {
		if (enableWebShell) {
			String podName = resourceId.getResourceIdString();
			String namespace =  flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
			return KubernetesUtils.getWebShell(podName, namespace);
		} else {
			return super.getTaskManagerWebShell(resourceId, host);
		}
	}

	public CompletableFuture<String> requestJobManagerLogUrl(@RpcTimeout Time timeout) {
		if (streamLogEnabled && !StringUtils.isNullOrWhitespaceOnly(streamLogDomain)) {
			try {
				CompletableFuture<String> jmLog = new CompletableFuture<>();
				String jmLogStr = genLogUrl(streamLogUrlTemplate, streamLogDomain, streamLogQueryRange, streamLogQueryTemplate, jobManagerPodName, region, streamLogSearchView);
				jmLog.complete(jmLogStr);
				return jmLog;
			} catch (Throwable t) {
				LOG.warn("Get JobManager log url error.", t);
				return super.requestJobManagerLogUrl(timeout);
			}
		} else {
			return super.requestJobManagerLogUrl(timeout);
		}
	}

	@Override
	public String getTaskManagerLogUrl(ResourceID resourceId, String host) {
		if (streamLogEnabled && !StringUtils.isNullOrWhitespaceOnly(streamLogDomain)) {
			try {
				return genLogUrl(streamLogUrlTemplate, streamLogDomain, streamLogQueryRange, streamLogQueryTemplate, resourceId.getResourceIdString(), region, streamLogSearchView);
			} catch (Throwable t) {
				LOG.warn("Get TaskManager {} log url error.", resourceId, t);
				return super.getTaskManagerLogUrl(resourceId, host);
			}
		} else {
			return super.getTaskManagerLogUrl(resourceId, host);
		}
	}

	@VisibleForTesting
	Map<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodes;
	}

	private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
		for (KubernetesPod pod : podList) {
			final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
			workerNodes.put(worker.getResourceID(), worker);
			if (minimalNodesNum > 0) {
				runningNodesNum.incrementAndGet();
			}
			final long attempt = worker.getAttempt();
			if (attempt > currentMaxAttemptId) {
				currentMaxAttemptId = attempt;
			}
		}

		log.info("Recovered {} pods from previous attempts, current attempt id is {}.",
			workerNodes.size(),
			++currentMaxAttemptId);
	}

	private void requestKubernetesPod(WorkerResourceSpec workerResourceSpec, int podNumber) {
		for (int i = 0; i < podNumber; i++) {
			final KubernetesTaskManagerParameters parameters =
				createKubernetesTaskManagerParameters(workerResourceSpec);

			podWorkerResources.put(parameters.getPodName(), workerResourceSpec);
			int pendingWorkerNum = notifyNewWorkerRequested(workerResourceSpec).getNumNotAllocated();
			log.info("Requesting new TaskManager pod with <{},{}>. Number pending requests {}.",
				parameters.getTaskManagerMemoryMB(),
				parameters.getTaskManagerCPU(),
				pendingWorkerNum);

			final KubernetesPod taskManagerPod =
				KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);
			kubeClient.createTaskManagerPod(taskManagerPod)
				.whenCompleteAsync(
					(ignore, throwable) -> {
						if (throwable != null) {
							final Time retryInterval = configuration.getPodCreationRetryInterval();
							log.warn("Could not start TaskManager in pod {}, retry in {}. ",
								taskManagerPod.getName(), retryInterval, throwable);
							podWorkerResources.remove(parameters.getPodName());
							notifyNewWorkerAllocationFailed(workerResourceSpec);
							scheduleRunAsync(
								this::requestKubernetesPodIfRequired,
								retryInterval);
						} else {
							log.info("TaskManager {} will be started with {}.", parameters.getPodName(), workerResourceSpec);
						}
					},
					getMainThreadExecutor());
		}
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(WorkerResourceSpec workerResourceSpec) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);

		final String podName = String.format(
			TASK_MANAGER_POD_FORMAT,
			clusterId,
			currentMaxAttemptId,
			++currentMaxPodId);

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

			while (requiredTaskManagers > getNumRequestedNotRegisteredWorkersFor(workerResourceSpec)) {
				tryStartNewWorker(workerResourceSpec);
			}
		}
	}

	private void removePodAndTryRestartIfRequired(KubernetesPod pod) {
		if (pod.isTerminated()) {
			recordWorkerFailure();
			internalStopPod(pod.getName());
			requestKubernetesPodIfRequired();
		}
	}

	private void internalStopPod(String podName) {
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

	/**
	 * Check whether a worker could be stopped. A worker could be stopped if the remaining
	 * task managers are more than the required minimal number. If a worker is judged to
	 * be stoppable, the runningNodesNum will be decreased in advance and the worker will
	 * be stopped later.
	 *
	 * @return true if the worker could be stopped, or false if the worker could not be stopped.
	 */
	private synchronized boolean canStopWorker() {
		if (runningNodesNum.get() > minimalNodesNum) {
			runningNodesNum.decrementAndGet();
			return true;
		}
		// runningNodesNum may greater than minimalNodesNum here as Pod watching is asynchronous,
		// but it is okay as we can stop it at the next turn.
		return false;
	}
}
