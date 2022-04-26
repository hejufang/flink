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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_TASK_MANAGER_POST_START_HANDLER_COMMANDS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters
 * that are used for constructing the TaskManager Pod.
 */
public class KubernetesTaskManagerParameters extends AbstractKubernetesParameters {
	private final Logger log = LoggerFactory.getLogger(getClass());

	public static final String TASK_MANAGER_MAIN_CONTAINER_NAME = "flink-task-manager";

	private final String podName;

	private final String dynamicProperties;

	private final ContaineredTaskManagerParameters containeredTaskManagerParameters;

	/**
	 * The minimum number of task manager. This field will only be used in application mode to help to
	 * initialize th pod group. Otherwise this will be set to 1.
	 * */
	private final int minNumTaskManager;

	private final Map<String, Long> taskManagerExternalResources;

	public KubernetesTaskManagerParameters(
			Configuration flinkConfig,
			String podName,
			String dynamicProperties,
			ContaineredTaskManagerParameters containeredTaskManagerParameters,
			int minNumTaskManager,
			Map<String, Long> taskManagerExternalResources) {
		super(flinkConfig);
		this.podName = checkNotNull(podName);
		this.dynamicProperties = checkNotNull(dynamicProperties);
		this.containeredTaskManagerParameters = checkNotNull(containeredTaskManagerParameters);
		this.minNumTaskManager = minNumTaskManager;
		this.taskManagerExternalResources = checkNotNull(taskManagerExternalResources);
	}

	@Override
	public Map<String, String> getLabels() {
		final Map<String, String> labels = new HashMap<>();
		labels.putAll(flinkConfig.getOptional(KubernetesConfigOptions.TASK_MANAGER_LABELS).orElse(Collections.emptyMap()));
		labels.putAll(KubernetesUtils.getTaskManagerLabels(getClusterId()));
		return Collections.unmodifiableMap(labels);
	}

	@Override
	public Map<String, String> getNodeSelector() {
		return Collections.unmodifiableMap(
			flinkConfig.getOptional(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR).orElse(Collections.emptyMap()));
	}

	@Override
	public Map<String, String> getEnvironments() {
		return this.containeredTaskManagerParameters.taskManagerEnv();
	}

	@Override
	public Map<String, String> getAnnotations() {
		Map<String, String> taskManagerAnnotations =  KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS);
		if (isPodGroupEffective() && minNumTaskManager > 0) {
			// only add pod group annotation when minNumTaskManager > 0
			taskManagerAnnotations.put(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY, String.valueOf(minNumTaskManager));
		}
		return taskManagerAnnotations;
	}

	@Override
	public List<Map<String, String>> getTolerations() {
		return flinkConfig.getOptional(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS).orElse(Collections.emptyList());
	}

	public String getTaskManagerMainContainerName() {
		return TASK_MANAGER_MAIN_CONTAINER_NAME;
	}

	public String getPodName() {
		return podName;
	}

	public int getTaskManagerMemoryMB() {
		return containeredTaskManagerParameters.getTaskExecutorProcessSpec().getTotalProcessMemorySize().getMebiBytes();
	}

	public double getTaskManagerCPU() {
		return containeredTaskManagerParameters.getTaskExecutorProcessSpec().getCpuCores().getValue().doubleValue();
	}

	public String getServiceAccount() {
		return flinkConfig.getOptional(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT)
			.orElse(flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
	}

	public Map<String, Long> getTaskManagerExternalResources() {
		return taskManagerExternalResources;
	}

	public int getRPCPort() {
		final int taskManagerRpcPort = KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
		checkArgument(taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());
		return taskManagerRpcPort;
	}

	public String getDynamicProperties() {
		return dynamicProperties;
	}

	public ContaineredTaskManagerParameters getContaineredTaskManagerParameters() {
		return containeredTaskManagerParameters;
	}

	/**
	 * Check that the provided POD meets the current requirements.
	 * This method is to find some available previous pods.
	 * @param pod KubernetesPod already exist.
	 * @return Whether this pod is match current requirements.
	 */
	public boolean matchKubernetesPod(KubernetesPod pod) {
		// check main container exists
		Container mainContainer = null;
		for (Container c : pod.getInternalResource().getSpec().getContainers()) {
			if (TASK_MANAGER_MAIN_CONTAINER_NAME.equals(c.getName())) {
				mainContainer = c;
				break;
			}
		}
		if (mainContainer == null) {
			log.error("pod {} not match request because of no main container", pod.getInternalResource());
			return false;
		}

		// check main container resource requirements.
		final ResourceRequirements requestResourceRequirements = KubernetesUtils.getResourceRequirements(
				getTaskManagerMemoryMB(),
				getTaskManagerCPU(),
				getTaskManagerExternalResources());
		final ResourceRequirements podResourceRequirements = mainContainer.getResources();
		if (!requestResourceRequirements.equals(podResourceRequirements)) {
			log.info("pod {} not match request because of resource not match. pod resource: {}, request resource {}",
					pod.getName(),
					podResourceRequirements,
					requestResourceRequirements);
			return false;
		}

		// check node selectors.
		Map<String, String> podNodeSelectors = pod.getInternalResource().getSpec().getNodeSelector();
		Map<String, String> requestNodeSelector = getNodeSelector();
		if (!podNodeSelectors.entrySet().containsAll(requestNodeSelector.entrySet())) {
			log.info("pod {} not match request because of node selector. pod selector: {}, request selector {}",
					pod.getName(),
					podNodeSelectors,
					requestNodeSelector);
			return false;
		}

		// all check passed.
		return true;
	}

	@Override
	public String getPostStartHandlerCommand() {
		List<String> postStartCommands = flinkConfig.get(KUBERNETES_TASK_MANAGER_POST_START_HANDLER_COMMANDS);

		if (postStartCommands == null) {
			return null;
		} else {
			return String.join(" && ", postStartCommands);
		}
	}
}
