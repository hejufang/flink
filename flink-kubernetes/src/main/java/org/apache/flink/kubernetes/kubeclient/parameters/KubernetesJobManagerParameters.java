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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.KUBERNETES_JOB_MANAGER_POST_START_HANDLER_COMMANDS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility class helps parse, verify, and manage the Kubernetes side parameters
 * that are used for constructing the JobManager Pod and all accompanying resources
 * connected to it.
 */
public class KubernetesJobManagerParameters extends AbstractKubernetesParameters {

	public static final String JOB_MANAGER_MAIN_CONTAINER_NAME = "flink-job-manager";

	private final ClusterSpecification clusterSpecification;

	public KubernetesJobManagerParameters(Configuration flinkConfig, ClusterSpecification clusterSpecification) {
		super(flinkConfig);
		this.clusterSpecification = checkNotNull(clusterSpecification);
	}

	@Override
	public Map<String, String> getLabels() {
		final Map<String, String> labels = new HashMap<>();
		labels.putAll(flinkConfig.getOptional(KubernetesConfigOptions.JOB_MANAGER_LABELS).orElse(Collections.emptyMap()));
		labels.putAll(getCommonLabels());
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		return Collections.unmodifiableMap(labels);
	}

	@Override
	public Map<String, String> getNodeSelector() {
		return Collections.unmodifiableMap(
			flinkConfig.getOptional(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR).orElse(Collections.emptyMap()));
	}

	@Override
	public Map<String, String> getEnvironments() {
		return ConfigurationUtils.getPrefixedKeyValuePairs(ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX, flinkConfig);
	}

	@Override
	public Map<String, String> getAnnotations() {
		Map<String, String> jobManagerAnnotations = KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS);
		if (isPodGroupEffective()) {
			// we need one job manager, todo, what if multiple Job Manager when enabling HA.
			jobManagerAnnotations.put(Constants.POD_GROUP_MINMEMBER_ANNOTATION_KEY, "1");
		}
		return jobManagerAnnotations;
	}

	@Override
	public List<Map<String, String>> getTolerations() {
		return flinkConfig.getOptional(KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS).orElse(Collections.emptyList());
	}

	public Map<String, String> getRestServiceAnnotations() {
		return KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS);
	}

	public String getJobManagerMainContainerName() {
		return JOB_MANAGER_MAIN_CONTAINER_NAME;
	}

	public int getJobManagerMemoryMB() {
		return clusterSpecification.getMasterMemoryMB();
	}

	public double getJobManagerCPU() {
		return flinkConfig.getDouble(KubernetesConfigOptions.JOB_MANAGER_CPU);
	}

	public int getRestPort() {
		return flinkConfig.getInteger(RestOptions.PORT);
	}

	public int getSocketPort() {
		return flinkConfig.getInteger(RestOptions.SOCKET_PORT);
	}

	public int getRestBindPort() {
		return Integer.valueOf(flinkConfig.getString(RestOptions.BIND_PORT));
	}

	public int getRPCPort() {
		return flinkConfig.getInteger(JobManagerOptions.PORT);
	}

	public Map<String, String> getDeploymentAnnotations() {
		return KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.KUBERNETES_DEPLOYMENT_ANNOTATIONS);
	}

	public Map<String, String> getIngressAnnotations() {
		return KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.KUBERNETES_INGRESS_ANNOTATIONS);
	}

	public String getIngressHost() {
		final String host = flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_INGRESS_HOST);
		if (StringUtils.isNullOrWhitespaceOnly(host)) {
			throw new IllegalArgumentException(KubernetesConfigOptions.CLUSTER_ID.key() + " must not be blank.");
		}
		return host;
	}

	public int getBlobServerPort() {
		final int blobServerPort = KubernetesUtils.parsePort(flinkConfig, BlobServerOptions.PORT);
		checkArgument(blobServerPort > 0, "%s should not be 0.", BlobServerOptions.PORT.key());
		return blobServerPort;
	}

	public String getServiceAccount() {
		return flinkConfig.getOptional(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT)
			.orElse(flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT));
	}

	public String getEntrypointClass() {
		final String entrypointClass = flinkConfig.getString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS);
		checkNotNull(entrypointClass, KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS + " must be specified!");

		return entrypointClass;
	}

	public KubernetesConfigOptions.ServiceExposedType getRestServiceExposedType() {
		return flinkConfig.get(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
	}

	public boolean enableIngress() {
		return flinkConfig.get(KubernetesConfigOptions.KUBERNETES_INGRESS_ENABLE);
	}

	public boolean isInternalServiceEnabled() {
		return !HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig);
	}

	@Override
	public String getPostStartHandlerCommand() {
		List<String> postStartCommands = flinkConfig.get(KUBERNETES_JOB_MANAGER_POST_START_HANDLER_COMMANDS);

		if (postStartCommands == null) {
			return null;
		} else {
			return String.join(" && ", postStartCommands);
		}
	}

	// Arcee Parameter
	public boolean isArceeEnabled() {
		return flinkConfig.get(KubernetesConfigOptions.ARCEE_ENABLED);
	}

	public Map<String, String> getArceeApplicationAnnotations() {
		return KubernetesUtils.getAnnotations(flinkConfig, KubernetesConfigOptions.ARCEE_APP_ANNOTATIONS);
	}

	public String getArceeApplicationName() {
		String appName = flinkConfig.getString(KubernetesConfigOptions.ARCEE_APP_NAME);
		if (appName == null) {
			appName = flinkConfig.getString(PipelineOptions.NAME, "");
			flinkConfig.setString(KubernetesConfigOptions.ARCEE_APP_NAME, appName);
		}
		return appName;
	}

	public String getArceeAdmissionConfigAccount() {
		return flinkConfig.getString(KubernetesConfigOptions.ARCEE_ADMISSION_CONFIG_ACCOUNT);
	}

	public String getArceeAdmissionConfigUser() {
		String owner = flinkConfig.getString(KubernetesConfigOptions.ARCEE_ADMISSION_CONFIG_USER);
		if (owner == null) {
			owner = flinkConfig.getString(ConfigConstants.OWNER_KEY, ConfigConstants.OWNER_DEFAULT);
			flinkConfig.setString(KubernetesConfigOptions.ARCEE_ADMISSION_CONFIG_USER, owner);
		}
		return owner;
	}

	public String getArceeAdmissionConfigGroup() {
		return flinkConfig.getString(KubernetesConfigOptions.ARCEE_ADMISSION_CONFIG_GROUP);
	}

	public String getArceeSchedulingConfigQueue() {
		String queue = flinkConfig.getString(KubernetesConfigOptions.ARCEE_SCHEDULING_CONFIG_QUEUE);
		if (queue == null) {
			queue = flinkConfig.getString(ConfigConstants.QUEUE_KEY, ConfigConstants.QUEUE_DEFAULT);
			flinkConfig.setString(KubernetesConfigOptions.ARCEE_SCHEDULING_CONFIG_QUEUE, queue);
		}
		return queue;
	}

	public int getArceeSchedulingConfigScheduleTimeoutSec() {
		return flinkConfig.getInteger(KubernetesConfigOptions.ARCEE_SCHEDULING_CONFIG_SCHEDULE_TIMEOUT_SECONDS);
	}

	public String getArceeSchedulingConfigPriorityClassName() {
		return flinkConfig.getString(KubernetesConfigOptions.ARCEE_SCHEDULING_CONFIG_PRIORITY_CLASS_NAME);
	}

	public String getArceeRestartPolicyType() {
		return flinkConfig.getString(KubernetesConfigOptions.ARCEE_RESTART_POLICY_TYPE);
	}

	public int getArceeRestartPolicyMaxRetries() {
		return flinkConfig.getInteger(KubernetesConfigOptions.ARCEE_RESTART_POLICY_MAX_RETRIES);
	}

	public long getArceeRestartPolicyInterval() {
		return flinkConfig.getLong(KubernetesConfigOptions.ARCEE_RESTART_POLICY_INTERVAL);
	}
}
