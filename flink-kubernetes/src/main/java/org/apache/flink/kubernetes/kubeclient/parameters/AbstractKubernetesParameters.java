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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.util.docker.DockerConfigOptions;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Quantity;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS;
import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.FLINK_EXTERNAL_JAR_DEPENDENCIES;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.DNS_POLICY_DEFAULT;
import static org.apache.flink.kubernetes.utils.Constants.DNS_POLICY_HOSTNETWORK;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract class for the {@link KubernetesParameters}.
 */
public abstract class AbstractKubernetesParameters implements KubernetesParameters {

	protected final Configuration flinkConfig;

	public AbstractKubernetesParameters(Configuration flinkConfig) {
		this.flinkConfig = checkNotNull(flinkConfig);
	}

	public Configuration getFlinkConfiguration() {
		return flinkConfig;
	}

	@Override
	public String getConfigDirectory() {
		final String configDir = flinkConfig.getOptional(DeploymentOptionsInternal.CONF_DIR).orElse(
			flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));

		checkNotNull(configDir);
		return configDir;
	}

	@Override
	public String getClusterId() {
		final String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);

		if (StringUtils.isBlank(clusterId)) {
			throw new IllegalArgumentException(KubernetesConfigOptions.CLUSTER_ID.key() + " must not be blank.");
		} else if (clusterId.length() > Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID) {
			throw new IllegalArgumentException(KubernetesConfigOptions.CLUSTER_ID.key() + " must be no more than " +
				Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + " characters.");
		}

		return clusterId;
	}

	@Override
	public String getNamespace() {
		final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		checkArgument(!namespace.trim().isEmpty(), "Invalid " + KubernetesConfigOptions.NAMESPACE + ".");

		return namespace;
	}

	@Override
	public String getImage() {
		final String containerImage = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE);
		checkArgument(!containerImage.trim().isEmpty(),
			"Invalid " + KubernetesConfigOptions.CONTAINER_IMAGE + ".");
		return containerImage;
	}

	@Override
	public KubernetesConfigOptions.ImagePullPolicy getImagePullPolicy() {
		return flinkConfig.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);
	}

	@Override
	public LocalObjectReference[] getImagePullSecrets() {
		final List<String> imagePullSecrets = flinkConfig.get(CONTAINER_IMAGE_PULL_SECRETS);

		if (imagePullSecrets == null) {
			return new LocalObjectReference[0];
		} else {
			return imagePullSecrets.stream()
				.map(String::trim)
				.filter(secret -> !secret.isEmpty())
				.map(LocalObjectReference::new)
				.toArray(LocalObjectReference[]::new);
		}
	}

	@Override
	public Map<String, String> getCommonLabels() {
		return Collections.unmodifiableMap(KubernetesUtils.getCommonLabels(getClusterId()));
	}

	/**
	 * Return map of host path that container needs.
	 * volume name -> (path in host, path to be mounted in container)
	 * @return
	 */
	public Map<String, Tuple2<String, String>> getMountedHostPath() {
		Map<String, Tuple2<String, String>> mountedHostPathMap = new HashMap<>();
		List<String> hostPathList = flinkConfig.getOptional(KubernetesConfigOptions.FLINK_MOUNTED_HOST_PATH)
			.orElse(Collections.emptyList());
		for (String mountedHostPath: hostPathList){
			String[] nameAndPaths = mountedHostPath.split(",");
			String volumeName = nameAndPaths[0].trim();
			checkArgument(StringUtils.isNotBlank(volumeName), "volume name is blank");
			checkArgument(nameAndPaths.length == 3 && !mountedHostPathMap.containsKey(volumeName),
				"Duplicated volume name or illegal formats");
			String pathInHost = nameAndPaths[1].trim();
			String pathInContainer = nameAndPaths[2].trim();
			checkArgument(StringUtils.isNotBlank(pathInHost), "path in host is blank");
			checkArgument(StringUtils.isNotBlank(pathInContainer), "path in container is blank");
			mountedHostPathMap.put(volumeName, Tuple2.of(pathInHost, pathInContainer));
		}
		return mountedHostPathMap;
	}

	public Map<String, Integer> getJobManagerUserDefinedPorts() {
		return getUserDefinedPorts(KubernetesConfigOptions.FLINK_JOBMANAGER_USER_PORTS);
	}

	public Map<String, Integer> getTaskManagerUserDefinedPorts() {
		return getUserDefinedPorts(KubernetesConfigOptions.FLINK_TASKMANAGER_USER_PORTS);
	}

	public Map<String, Integer> getUserDefinedPorts(ConfigOption<List<String>> key) {
		List<String> portList = flinkConfig.getOptional(key).orElse(Collections.emptyList());
		Map<String, Integer> ports = new LinkedHashMap<>();
		for (String portItem : portList) {
			String[] nameAndPort = portItem.split(":");
			checkArgument(nameAndPort.length == 2, "Config of " + portItem + " format error.");
			String name = nameAndPort[0].trim();
			int port = Integer.parseInt(nameAndPort[1]);
			if (ports.containsKey(name)) {
				throw new IllegalArgumentException("Duplicate port name of " + name);
			}
			if (ports.containsValue(port)) {
				throw new IllegalArgumentException("Duplicate port of " + port);
			}
			ports.put(name, port);
		}
		return ports;
	}

	public String getSecretName(){
		String nameTemplate = flinkConfig.getString(KubernetesConfigOptions.GDPR_SECRETE_NAME_TEMPLATE);
		return nameTemplate.replace("%cluster-id%", this.getClusterId());
	}

	@Override
	public String getFlinkConfDirInPod() {
		return flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
	}

	@Override
	public String getFlinkLogDirInPod() {
		return flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);
	}

	@Override
	public String getWorkingDir() {
		return flinkConfig.getString(KubernetesConfigOptions.CONTAINER_WORK_DIR);
	}

	@Override
	public String getContainerEntrypoint() {
		return flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH);
	}

	@Override
	public String getSchedulerName() {
		return flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_SCHEDULER_NAME);
	}

	@Override
	public boolean hasLogback() {
		final String confDir = getConfigDirectory();
		final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
		return logbackFile.exists();
	}

	@Override
	public boolean hasLog4j() {
		final String confDir = getConfigDirectory();
		final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);
		return log4jFile.exists();
	}

	@Override
	public Optional<String> getExistingHadoopConfigurationConfigMap() {
		final String existingHadoopConfigMap = flinkConfig.getString(KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP);
		if (StringUtils.isBlank(existingHadoopConfigMap)) {
			return Optional.empty();
		} else {
			return Optional.of(existingHadoopConfigMap.trim());
		}
	}

	public Optional<String> getExistingHadoopConfigurationHostPathVolume() {
		final String existingHadoopConfigVolume = flinkConfig.getString(KubernetesConfigOptions.HADOOP_CONF_MOUNTED_HOST_PATH_VOLUME);
		if (StringUtils.isBlank(existingHadoopConfigVolume)) {
			return Optional.empty();
		} else {
			return Optional.of(existingHadoopConfigVolume.trim());
		}
	}

	@Override
	public Optional<String> getLocalHadoopConfigurationDirectory() {
		final String[] possibleHadoopConfPaths = new String[] {
			System.getenv(Constants.ENV_HADOOP_CONF_DIR),
			System.getenv(Constants.ENV_HADOOP_HOME) + "/etc/hadoop", // hadoop 2.2
			System.getenv(Constants.ENV_HADOOP_HOME) + "/conf"
		};

		for (String hadoopConfPath: possibleHadoopConfPaths) {
			if (StringUtils.isNotBlank(hadoopConfPath)) {
				return Optional.of(hadoopConfPath);
			}
		}

		return Optional.empty();
	}

	public Map<String, String> getSecretNamesToMountPaths() {
		return flinkConfig.getOptional(KubernetesConfigOptions.KUBERNETES_SECRETS).orElse(Collections.emptyMap());
	}

	public boolean isHostNetworkEnabled() {
		return flinkConfig.get(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED);
	}

	public boolean isReplaceS3SchemaByTos() {
		return flinkConfig.getBoolean(KubernetesConfigOptions.KUBERNETES_REPLACE_S3_SCHEMA_BY_TOS_ENABLED);
	}

	public String getDnsPolicy() {
		// only set to DNS_POLICY_HOSTNETWORK when its necessary because the coreDNS may be unavailable
		if (flinkConfig.get(KubernetesConfigOptions.KUBERNETES_HOST_NETWORK_ENABLED)) {
			if (HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
				return DNS_POLICY_DEFAULT;
			}
			// When in Non-HA mode, the TM connect to JM via address svc.namespace.
			// This address can only be resolved with DNS policy: ClusterFirstWithHostNet after host network enabled
			return DNS_POLICY_HOSTNETWORK;
		} else {
			return DNS_POLICY_DEFAULT;
		}
	}

	@Override
	public List<Map<String, String>> getEnvironmentsFromSecrets() {
		return flinkConfig.getOptional(KubernetesConfigOptions.KUBERNETES_ENV_SECRET_KEY_REF).orElse(Collections.emptyList());
	}

	public boolean isPodGroupEffective() {
		boolean podGroupEnable = flinkConfig.getBoolean(KubernetesConfigOptions.KUBERNETES_PODGROUP_ENABLE);
		if (!podGroupEnable) {
			return false;
		}
		// Pod group is only supported in application mode.
		final String entrypointClass = flinkConfig.getString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS);
		return KubernetesApplicationClusterEntrypoint.class.getName().equals(entrypointClass);
	}

	public String getCsiDriverName() {
		return this.flinkConfig.getString(KubernetesConfigOptions.CSI_DRIVER);
	}

	public Map<String, Quantity> getCsiDiskResourceRequirement() {
		Optional<String> diskResourceValue = flinkConfig.getOptional(KubernetesConfigOptions.CSI_DISK_RESOURCE_VALUE);
		// Only if this disk resource value parameter is set, we create the disk resource requirement map.

		// Noted for AML scenarios, the kubernetes cluster haven't deployed the device plugin but the webhook allocate
		// a disk to Flink, so AML job should not set this parameter otherwise the pod can not be scheduled.
		// Other scenarios (bigdata) need to set this parameter to 1 means enable using device plugin to allocate disk.
		if (diskResourceValue.isPresent()) {
			Map<String, Quantity> diskResource = new HashMap<>();
			String diskResourceKey = flinkConfig.getString(KubernetesConfigOptions.CSI_DISK_RESOURCE_KEY);
			Quantity diskQuantity = Quantity.parse(diskResourceValue.get());
			diskResource.put(diskResourceKey, diskQuantity);
			return diskResource;
		}
		return Collections.emptyMap();
	}


	// databus sidecar
	public boolean isDatabusSideCarEnabled() {
		return flinkConfig.getBoolean(KubernetesConfigOptions.SIDECAR_DATABUS_ENABLED);
	}

	public String getDatabusSidecarImage() {
		if (flinkConfig.contains(KubernetesConfigOptions.SIDECAR_DATABUS_IMAGE)) {
			return flinkConfig.getString(KubernetesConfigOptions.SIDECAR_DATABUS_IMAGE);
		} else {
			throw new IllegalArgumentException(KubernetesConfigOptions.SIDECAR_DATABUS_IMAGE.key() + " must be configured.");
		}
	}

	public double getDatabusSidecarCpu() {
		if (flinkConfig.contains(KubernetesConfigOptions.SIDECAR_DATABUS_CPU)) {
			return flinkConfig.getDouble(KubernetesConfigOptions.SIDECAR_DATABUS_CPU);
		} else {
			throw new IllegalArgumentException(KubernetesConfigOptions.SIDECAR_DATABUS_CPU.key() + " must be configured.");
		}
	}

	public int getDatabusSidecarMemoryInMB() {
		if (flinkConfig.contains(KubernetesConfigOptions.SIDECAR_DATABUS_MEMORY)) {
			return flinkConfig.get(KubernetesConfigOptions.SIDECAR_DATABUS_MEMORY).getMebiBytes();
		} else {
			throw new IllegalArgumentException(KubernetesConfigOptions.SIDECAR_DATABUS_MEMORY.key() + " must be configured.");
		}
	}

	public int getDatabusSidecarShmSizeInMB() {
		int shmSizeImMB = flinkConfig
				.getOptional(KubernetesConfigOptions.SIDECAR_DATABUS_SHARED_MEMORY_SIZE)
				.map(MemorySize::getMebiBytes)
				.orElse(getDatabusSidecarMemoryInMB() * 2 / 3);
		checkArgument(shmSizeImMB < getDatabusSidecarMemoryInMB(), "shared memory size must less than databus memory.");
		return shmSizeImMB;
	}

	public long getTerminationGracePeriodSeconds() {
		return flinkConfig.getLong(KubernetesConfigOptions.TERMINATION_GRACE_PERIOD_SECONDS);
	}

	public boolean isServiceLinkEnable() {
		return flinkConfig.getBoolean(KubernetesConfigOptions.SERVICE_LINK_ENABLE);
	}

	public String getKafkaPartitionList() {
		return flinkConfig.getString(
				ConfigConstants.PARTITION_LIST_KEY,
				flinkConfig.getString(ConfigConstants.PARTITION_OLD_LIST_KEY, null));
	}

	public String getRMQPartitionList() {
		return flinkConfig.getString(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY, null);
	}

	public boolean keepUserClasspathCompatible() {
		return flinkConfig.getBoolean(PipelineOptions.USER_CLASSPATH_COMPATIBLE);
	}

	/**
	 * Find the flink home path by analyzing the path of container entrypoint script. Noted this flink home path will not
	 * be ended with "/".
	 *
	 * @return Flink Home directory
	 */
	public String findFlinkHomeInContainer() {
		String kubeEntryPath = getContainerEntrypoint();
		return new File(kubeEntryPath).getParentFile().getParent();
	}

	public String getExternalJarDependencies() {
		return flinkConfig.get(FLINK_EXTERNAL_JAR_DEPENDENCIES);
	}

	/**
	 * Determines if custom image compatibility mode is allowed. Requires the user to set custom-image-compatible to true
	 * as well as input an image for docker.image.
	 *
	 * @return true, if custom-image-compatibility is true and docker.image is specified. false, otherwise.
	 */
	public boolean getCustomDockerCompatibility() {
		boolean customImageOption = flinkConfig.getBoolean(KubernetesConfigOptions.CUSTOM_IMAGE_COMPATIBLE);
		String customImage = flinkConfig.getString(DockerConfigOptions.DOCKER_IMAGE);
		return customImageOption && StringUtils.isNotBlank(customImage);
	}
}
