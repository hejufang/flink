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

package org.apache.flink.yarn.smartresources;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SmartResourceOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.messages.webmonitor.SmartResourcesStats;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.yarn.Utils;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.flink.yarn.YarnResourceManager;
import org.apache.flink.yarn.YarnWorkerNode;

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

import com.bytedance.sr.estimater.client.EstimateException;
import com.bytedance.sr.estimater.client.EstimaterClient;
import com.bytedance.sr.estimater.client.ResourcesUsage;
import org.apache.hadoop.yarn.api.protocolrecords.RuntimeConfiguration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.RtQoSLevel;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SmartResource Manager.
 */
public class SmartResourceManager {

	private final Logger log = LoggerFactory.getLogger(SmartResourceManager.class);

	private Map<ContainerId, Long/* expired time ms */> pendingUpdating;
	private ContainerResources targetResources;
	private YarnResourceManager yarnResourceManager;
	private long resourcesUpdateTimeoutMS = 2 * 60 * 1000;
	private EstimaterClient estimaterClient;
	private String region;
	private String cluster;
	private String applicationID;
	private String applicationName;
	private int durationMinutes;
	private double cpuReserveRatio;
	private double memReserveRatio;
	private SmartResourcesStats smartResourcesStats;
	private boolean disableMemAdjust = false;
	private boolean srCpuAdjustDoubleEnable = false;
	private boolean smartResourcesEnable;
	private int srMemMaxMB;
	private String srCpuEstimateMode;
	private String srAdjustCheckApi;
	private int srAdjustCheckBackoffMS;
	private int srAdjustCheckTimeoutMS;
	private long srNextCheckTimeMS;

	/**
	 * Resource Manager has already updated, wait NodeManager update success.
	 */
	private Map<ContainerId, Container> waitNMUpdate;
	/**
	 * The failed times to update container.
	 */
	private Map<ContainerId, Integer> containerUpdateFailedTimesMap;
	/**
	 * Before this timestamp, the container will be skipped for updating resource.
	 */
	private Map<ContainerId, Long> skipUpdateContainersMap;
	/**
	 * If the container retry times exceed this, it will be add to nmUpdateFailedTimes to skip update for some times.
	 */
	private static final int CONTAINER_MAX_RETRY_TIMES = 3;
	/**
	 * If the container retry times exceed maxRetryTimes, it will be skipping to update for this times.
	 */
	public static final long CONTAINER_SKIP_UPDATE_TIME_MS = 30 * 60 * 1000;

	/**
	 * Init the config of smart-resource.
	 */
	public SmartResourceManager(Configuration flinkConfig, RuntimeConfiguration yarnRuntimeConf, Map<String, String> env) {
		smartResourcesStats = new SmartResourcesStats();
		smartResourcesEnable = flinkConfig.getBoolean(SmartResourceOptions.SMART_RESOURCES_ENABLE);
		if (!smartResourcesEnable) {
			smartResourcesEnable = flinkConfig.getBoolean(SmartResourceOptions.SMART_RESOURCES_ENABLE_OLD);
		}

		// The current yarn does not support the smart resources after turning on the CPU to bind the core
		if (yarnRuntimeConf.getQoSLevel() != RtQoSLevel.QOS_SHARE) {
			smartResourcesEnable = false;
		}

		Map<String, Object> smartResourcesConfig = new HashMap<>();
		smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_ENABLE.key(), smartResourcesEnable);

		if (smartResourcesEnable) {
			this.pendingUpdating = new HashMap<>();
			this.waitNMUpdate = new HashMap<>();
			this.containerUpdateFailedTimesMap = new ConcurrentHashMap<>();
			this.skipUpdateContainersMap = new ConcurrentHashMap<>();
			String smartResourcesServiceName = flinkConfig.getString(SmartResourceOptions.SMART_RESOURCES_SERVICE_NAME, null);
			Preconditions.checkNotNull(smartResourcesServiceName, "SmartResources enabled and service name not set");

			this.region = flinkConfig.getString(ConfigConstants.DC_KEY, null);
			Preconditions.checkNotNull(this.region, "SmartResources enabled and get region failed");
			this.cluster = flinkConfig.getString(ConfigConstants.CLUSTER_NAME_KEY, null);
			Preconditions.checkNotNull(this.cluster, "SmartResources enabled and get cluster failed");
			this.applicationID = System.getenv(YarnConfigKeys.ENV_APP_ID);
			if (StringUtils.isNullOrWhitespaceOnly(applicationID)) {
				this.applicationID = env.get(YarnConfigKeys.ENV_APP_ID);
			}
			Preconditions.checkNotNull(this.applicationID, "SmartResources enabled and get applicationID failed");
			String applicationNameWithUser = flinkConfig.getString("applicationName", null);
			Preconditions.checkNotNull(applicationNameWithUser, "SmartResources enabled and get applicationName failed");
			int idx = applicationNameWithUser.lastIndexOf("_");
			Preconditions.checkState(idx != -1, "SmartResources enabled and applicationName illegal, " + applicationNameWithUser);

			this.applicationName = applicationNameWithUser.substring(0, idx);
			this.estimaterClient = new EstimaterClient(smartResourcesServiceName);
			this.durationMinutes = flinkConfig.getInteger(SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES);
			if (this.durationMinutes == SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES.defaultValue()) {
				this.durationMinutes = flinkConfig.getInteger(SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES_OLD);
			}
			if (this.durationMinutes < SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES_MIN) {
				log.info("adjust smart-resources.duration.minutes from {} to {}", this.durationMinutes, SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES_MIN);
				this.durationMinutes = SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES_MIN;
			}

			this.cpuReserveRatio = flinkConfig.getDouble(SmartResourceOptions.SMART_RESOURCES_CPU_RESERVE_RATIO);
			this.memReserveRatio = flinkConfig.getDouble(SmartResourceOptions.SMART_RESOURCES_MEM_RESERVE_RATIO);
			this.disableMemAdjust = flinkConfig.getBoolean(SmartResourceOptions.SMART_RESOURCES_DISABLE_MEM_ADJUST);
			this.srCpuAdjustDoubleEnable = flinkConfig.getBoolean(SmartResourceOptions.SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE);
			this.srMemMaxMB = flinkConfig.getInteger(SmartResourceOptions.SMART_RESOURCES_MEM_MAX_MB);
			this.srCpuEstimateMode = flinkConfig.getString(SmartResourceOptions.SMART_RESOURCES_CPU_ESTIMATE_MODE);
			this.srAdjustCheckApi = flinkConfig.getString(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_API);
			Preconditions.checkState(validateSrAdjustCheckApi(srAdjustCheckApi), "Invalid sr check api, " + srAdjustCheckApi);
			this.srAdjustCheckBackoffMS = flinkConfig.getInteger(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_BACKOFF_MS);
			this.srAdjustCheckTimeoutMS = flinkConfig.getInteger(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_TIMEOUT_MS);
			this.srNextCheckTimeMS = System.currentTimeMillis();
			log.info("SmartResources initialized, region: {}, cluster: {}, applicationID: {}, "
					+ "applicationName: {}, smartResourcesServiceName: {}, durationMinutes: {}, "
					+ "cpuReserveRatio: {}, memReserveRatio: {}, disableMemAdjust: {}, memMaxMB: {}, "
					+ "cpuEstimateMode: {}, adjustCheckApi: {}, adjustCheckInterval: {}, "
					+ "srCpuAdjustDoubleEnable: {}.",
				this.region, this.cluster, this.applicationID, this.applicationName,
				smartResourcesServiceName, this.durationMinutes, this.cpuReserveRatio,
				this.memReserveRatio, this.disableMemAdjust, this.srMemMaxMB,
				this.srCpuEstimateMode, this.srAdjustCheckApi, this.srAdjustCheckBackoffMS,
				this.srCpuAdjustDoubleEnable);

			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE.key(), srCpuAdjustDoubleEnable);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_SERVICE_NAME.key(), smartResourcesServiceName);
			smartResourcesConfig.put(ConfigConstants.DC_KEY, this.region);
			smartResourcesConfig.put(ConfigConstants.CLUSTER_NAME_KEY, this.cluster);
			smartResourcesConfig.put(ConfigConstants.APPLICATION_ID_KEY, this.applicationID);
			smartResourcesConfig.put(ConfigConstants.APPLICATION_NAME_KEY, this.applicationName);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_DURATION_MINUTES.key(), this.durationMinutes);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_CPU_RESERVE_RATIO.key(), this.cpuReserveRatio);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_MEM_RESERVE_RATIO.key(), this.memReserveRatio);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_DISABLE_MEM_ADJUST.key(), this.disableMemAdjust);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_MEM_MAX_MB.key(), this.srMemMaxMB);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_CPU_ESTIMATE_MODE.key(), this.srCpuEstimateMode);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_API.key(), this.srAdjustCheckApi);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_BACKOFF_MS.key(), this.srAdjustCheckBackoffMS);
			smartResourcesConfig.put(SmartResourceOptions.SMART_RESOURCES_ADJUST_CHECK_TIMEOUT_MS.key(), this.srAdjustCheckTimeoutMS);
		}
		smartResourcesStats.setConfig(smartResourcesConfig);
	}

	/**
	 * Validate the smart-resource adjust check api is valid.
	 */
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

	public boolean getSmartResourcesEnable() {
		return smartResourcesEnable;
	}

	public void setYarnResourceManager(YarnResourceManager yarnResourceManager) {
		this.yarnResourceManager = yarnResourceManager;
	}

	public SmartResourcesStats getSmartResourcesStats() {
		return smartResourcesStats;
	}

	public void setTargetResources(ContainerResources targetResources) {
		this.targetResources = targetResources;
	}

	public Map<ContainerId, Container> getWaitNMUpdate() {
		return waitNMUpdate;
	}

	/**
	 * Calculate the container update resource through the history resource info.
	 */
	public UpdateContainersResources calculateContainerResource()
		throws EstimateException, InterruptedException {
		ResourcesUsage containerMaxResources = estimaterClient.estimateContainerMaxResources(applicationID, durationMinutes);
		ResourcesUsage applicationTotalResources = estimaterClient.estimateApplicationResources(region, cluster, applicationName, durationMinutes);

		int newMemoryMB = new Double(Math.ceil(containerMaxResources.getMemTotalMB() * (1 + memReserveRatio) / 1024)).intValue() * 1024;
		newMemoryMB = newMemoryMB > srMemMaxMB ? srMemMaxMB : newMemoryMB;

		double newVcores = Double.parseDouble(String.format("%.3f", applicationTotalResources.getCpuTotalVcores() * (1 + cpuReserveRatio) / yarnResourceManager.getWorkerNodeMap().size()));
		if (!srCpuAdjustDoubleEnable) {
			if (srCpuEstimateMode.equals(SmartResourceOptions.SMART_RESOURCES_CPU_ESTIMATE_MODE_FLOOR)) {
				newVcores = new Double(Math.floor(newVcores));
			} else if (srCpuEstimateMode.equals(SmartResourceOptions.SMART_RESOURCES_CPU_ESTIMATE_MODE_ROUND)) {
				newVcores = new Long(Math.round(newVcores));
			} else if (srCpuEstimateMode.equals(SmartResourceOptions.SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL)) {
				newVcores = new Double(Math.ceil(newVcores));
			}
		}
		log.info("newVcores: {}, newMemoryMB: {}.", newVcores, newMemoryMB);

		newVcores = newVcores > 8 ? 8 : newVcores;
		newVcores = newVcores < 1 ? 1 : newVcores;
		return new UpdateContainersResources(newMemoryMB, newVcores, new Long(containerMaxResources.getDurtion()).intValue());
	}

	/**
	 * If container can update resource, should request resource update to yarn.
	 */
	public void containersUpdated(List<UpdatedContainer> updatedContainers) {
		for (UpdatedContainer updatedContainer : updatedContainers) {
			// remove containerId from skipUpdateContainersMap while receiving update success response from yarn RM
			if (skipUpdateContainersMap.containsKey(updatedContainer.getContainer().getId())) {
				skipUpdateContainersMap.remove(updatedContainer.getContainer().getId());
			}

			if (!yarnResourceManager.getWorkerNodeMap().containsKey(yarnResourceManager.getResourceID(updatedContainer.getContainer()))) {
				log.info("Container {} resources was updated but container has completed.", updatedContainer.getContainer().getId());
				continue;
			}

			try {
				Container old = yarnResourceManager.getWorkerNodeMap().get(yarnResourceManager.getResourceID(updatedContainer.getContainer())).getContainer();
				log.info("[RM] succeed update {} resources from ({} MB, {} vcores) to {({} MB, {} vcores)}",
					updatedContainer.getContainer().getId(), old.getResource().getMemorySize(), old.getResource().getVirtualCores(),
					updatedContainer.getContainer().getResource().getMemorySize(), updatedContainer.getContainer().getResource().getVirtualCores());
				yarnResourceManager.updateContainerResourceAsync(updatedContainer.getContainer());
				waitNMUpdate.put(updatedContainer.getContainer().getId(), updatedContainer.getContainer());
			} catch (Throwable t) {
				log.error("update container resources error, " + updatedContainer.getContainer().getId(), t);
			} finally {
				pendingUpdating.remove(updatedContainer.getContainer().getId());
			}
		}
	}

	/**
	 * If update container resource failed, need to update metadata.
	 */
	public void containersUpdateError(List<UpdateContainerError> updateContainerErrors) {
		for (UpdateContainerError updateContainerError : updateContainerErrors) {
			log.error("Container {} resources update failed, reason: {}", updateContainerError.getUpdateContainerRequest().getContainerId(), updateContainerError.getReason());
			pendingUpdating.remove(updateContainerError.getUpdateContainerRequest().getContainerId());

			if (updateContainerError.getReason().equals("INCORRECT_CONTAINER_VERSION_ERROR")) {
				ResourceID resourceID = new ResourceID(updateContainerError.getUpdateContainerRequest().getContainerId().toString());
				Container currentContainer = yarnResourceManager.getWorkerNodeMap().get(resourceID).getContainer();
				if (currentContainer != null) {
					log.info("Container {} version updated, {} -> {}",
						currentContainer.getId(), currentContainer.getVersion(), updateContainerError.getCurrentContainerVersion());
					currentContainer.setVersion(updateContainerError.getCurrentContainerVersion());
				} else {
					log.info("Container {} has been released", updateContainerError.getUpdateContainerRequest().getContainerId());
				}
			} else {
				int containerUpdateFailedTimes = containerUpdateFailedTimesMap.getOrDefault(updateContainerError.getUpdateContainerRequest().getContainerId(), 0) + 1;
				if (containerUpdateFailedTimes >= CONTAINER_MAX_RETRY_TIMES) {
					// add container to updateSkipList
					skipUpdateContainersMap.put(updateContainerError.getUpdateContainerRequest().getContainerId(), System.currentTimeMillis() + CONTAINER_SKIP_UPDATE_TIME_MS);
					containerUpdateFailedTimesMap.remove(updateContainerError.getUpdateContainerRequest().getContainerId());
				} else {
					// update container update failed times
					containerUpdateFailedTimesMap.put(updateContainerError.getUpdateContainerRequest().getContainerId(), containerUpdateFailedTimes);
				}
			}
		}
	}

	/**
	 * Update container resource.
	 */
	public void updateContainersResources(UpdateContainersResources updateContainersResources) {
		log.debug("Receive update resources req: {}", updateContainersResources);
		ContainerResources newResources = new ContainerResources(updateContainersResources.getMemoryMB(), updateContainersResources.getVcores());
		if (updateContainersResources.getDurtionMinutes() < this.durationMinutes) {
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
						if (!checkIfCouldUpdateResources(
							new ContainerResources(targetResources.getMemoryMB(), newResources.getVcores()))) {
							log.warn("Container vcores update was rejected by sr check api, original: {} vcore, target: {} vcore",
								targetResources.getVcores(), newResources.getVcores());
						} else {
							log.info("Container vcores updated from {} vcore to {} vcore", targetResources.getVcores(), newResources.getVcores());
							targetResources.setVcores(newResources.getVcores());
							updated = true;
						}
					}

					if (updated) {
						smartResourcesStats.updateCurrentResources(new SmartResourcesStats.Resources(targetResources.getMemoryMB(), targetResources.getVcores()));
					} else {
						srNextCheckTimeMS = System.currentTimeMillis() + srAdjustCheckBackoffMS;
					}
				}
			} else {
				log.info("Container resources updated from {} to {}", targetResources, newResources);
				targetResources = newResources;
				smartResourcesStats.updateCurrentResources(new SmartResourcesStats.Resources(targetResources.getMemoryMB(), targetResources.getVcores()));
			}

		}

		for (Map.Entry<ResourceID, YarnWorkerNode> entry : yarnResourceManager.getWorkerNodeMap().entrySet()) {
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
			if (currentResource.getMemorySize() == targetResources.getMemoryMB() && currentResource.getVirtualCoresMilli() == targetVCoresMilli) {
				continue;
			}

			UpdateContainerRequest request = null;
			if (targetResources.getMemoryMB() >= currentResource.getMemory() &&
				// compare double with int
				targetVCoresMilli >= currentResource.getVirtualCoresMilli()) {
				// increase all
				request = UpdateContainerRequest.newInstance(container.getVersion(), containerId, ContainerUpdateType.INCREASE_RESOURCE, Resource.newInstance(targetResources.getMemoryMB(), 0, targetVCoresMilli));
			} else if (targetResources.getMemoryMB() <= currentResource.getMemorySize() &&
				// compare double with int
				targetVCoresMilli <= currentResource.getVirtualCoresMilli()) {
				// decrease all
				request = UpdateContainerRequest.newInstance(container.getVersion(), containerId, ContainerUpdateType.DECREASE_RESOURCE, Resource.newInstance(targetResources.getMemoryMB(), 0, targetVCoresMilli));
			} else if (targetResources.getMemoryMB() != currentResource.getMemorySize()) {
				// increase | decrease memory
				request = UpdateContainerRequest.newInstance(container.getVersion(), containerId,
					targetResources.getMemoryMB() > currentResource.getMemorySize() ? ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE,
					Resource.newInstance(targetResources.getMemoryMB(), 0, currentResource.getVirtualCoresMilli()));
			} else if (targetVCoresMilli != currentResource.getVirtualCoresMilli()) {
				// increase | decrease vcores
				request = UpdateContainerRequest.newInstance(container.getVersion(), containerId, targetVCoresMilli > currentResource.getVirtualCoresMilli() ? ContainerUpdateType.INCREASE_RESOURCE : ContainerUpdateType.DECREASE_RESOURCE, Resource.newInstance(currentResource.getMemorySize(), 0, targetVCoresMilli));
			}
			try {
				yarnResourceManager.requestContainerUpdate(container, request);
				pendingUpdating.put(containerId, System.currentTimeMillis() + resourcesUpdateTimeoutMS);
				log.info("request update {} resources from ({} MB, {} vcores) to ({} MB, {} vcores), request: {}.",
					containerId, currentResource.getMemorySize(), currentResource.getVirtualCoresDecimal(),
					targetResources.getMemoryMB(), targetResources.getVcores(), request);
			} catch (Throwable t) {
				log.error("update container resources error", t);
			}
		}
	}

	/**
	 * Check to whether need to update container resource.
	 */
	private boolean checkIfCouldUpdateResources(ContainerResources newResources) {
		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpGet checkGet = buildSrCheckGet(newResources);

			try (CloseableHttpResponse response = client.execute(checkGet)) {
				if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
					log.warn("Call sr check api failed, status code: {}", response.getStatusLine().getStatusCode());
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

	/**
	 * Build smart-resource check request.
	 */
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
			uriBuilder.setHost(node.getHost()).setPort(node.getPort());
		} else if (uriPieces[2].indexOf(":") != -1) {
			// port is configured
			String[] hostPieces = uriPieces[2].split(":");
			uriBuilder.setHost(hostPieces[0]).setPort(Integer.parseInt(hostPieces[1]));
		} else {
			// use default port 80
			uriBuilder.setHost(uriPieces[2]).setPort(80);
		}

		uriBuilder.setPath("/" + uriPieces[3]);

		// set param
		uriBuilder.addParameter("region", region)
			.addParameter("cluster", cluster)
			.addParameter("queue", System.getenv("_FLINK_YARN_QUEUE"))
			.addParameter("jobname", applicationName)
			.addParameter("original_tm_num", "" + yarnResourceManager.getWorkerNodeMap().size())
			.addParameter("original_tm_memory", "" + targetResources.getMemoryMB())
			.addParameter("original_tm_core", "" + targetResources.getVcores())
			.addParameter("target_tm_num", "" + yarnResourceManager.getWorkerNodeMap().size())
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
}
