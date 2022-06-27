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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.API_VERSION;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_COMPONENT;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NAME;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_COMPONENT_JOB_MANAGER;
import static org.apache.flink.kubernetes.utils.Constants.POD_IP_FIELD_PATH;
import static org.apache.flink.kubernetes.utils.Constants.POD_NAME_FIELD_PATH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the JobManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}.
 */
public class InitJobManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesJobManagerParameters kubernetesJobManagerParameters;
	private final Map<String, Tuple2<String, String>> mountedHostPaths;

	public InitJobManagerDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		this.kubernetesJobManagerParameters = checkNotNull(kubernetesJobManagerParameters);
		this.mountedHostPaths = kubernetesJobManagerParameters.getMountedHostPath();
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		List<Volume> volumeList = new ArrayList<>(mountedHostPaths.size());
		for  (Map.Entry<String,  Tuple2<String, String>> entry: mountedHostPaths.entrySet()) {
			final Volume volume = new VolumeBuilder()
				.withName(entry.getKey())
				.withNewHostPath(entry.getValue().f0, Constants.DEFAULT_HOST_PATH_TYPE)
				.build();
			volumeList.add(volume);
		}
		final Pod basicPod = new PodBuilder(flinkPod.getPod())
			.withApiVersion(API_VERSION)
			.editOrNewMetadata()
				.withLabels(kubernetesJobManagerParameters.getLabels())
				.withAnnotations(kubernetesJobManagerParameters.getAnnotations())
				.endMetadata()
			.editOrNewSpec()
				.withServiceAccountName(kubernetesJobManagerParameters.getServiceAccount())
				.withImagePullSecrets(kubernetesJobManagerParameters.getImagePullSecrets())
				.withNodeSelector(kubernetesJobManagerParameters.getNodeSelector())
				.withHostNetwork(kubernetesJobManagerParameters.isHostNetworkEnabled())
				.withDnsPolicy(kubernetesJobManagerParameters.getDnsPolicy())
				.addAllToVolumes(volumeList)
				.withTolerations(kubernetesJobManagerParameters.getTolerations().stream()
					.map(e -> KubernetesToleration.fromMap(e).getInternalResource())
					.collect(Collectors.toList()))
				.endSpec()
			.build();

		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

		return new FlinkPod.Builder(flinkPod)
			.withPod(basicPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private Container decorateMainContainer(Container container) {
		final ResourceRequirements requirements = KubernetesUtils.getResourceRequirements(
				kubernetesJobManagerParameters.getJobManagerMemoryMB(),
				kubernetesJobManagerParameters.getJobManagerCPU(),
				Collections.emptyMap());
		List<VolumeMount> volumeMountList = new ArrayList<>(mountedHostPaths.size());
		for  (Map.Entry<String,  Tuple2<String, String>> entry: mountedHostPaths.entrySet()) {
			final VolumeMount volumeMount = new VolumeMountBuilder()
				.withName(entry.getKey())
				.withMountPath(entry.getValue().f1)
				.withReadOnly(true)
				.build();
			volumeMountList.add(volumeMount);
		}
		Configuration flinkConfiguration = kubernetesJobManagerParameters.getFlinkConfiguration();
		Map<String, String> kubernetesAnnotations = KubernetesUtils.getAnnotations(flinkConfiguration, KubernetesConfigOptions.KUBERNETES_DEPLOYMENT_ANNOTATIONS);
		return new ContainerBuilder(container)
				.withName(kubernetesJobManagerParameters.getJobManagerMainContainerName())
				.withImage(kubernetesJobManagerParameters.getImage())
				.withImagePullPolicy(kubernetesJobManagerParameters.getImagePullPolicy().name())
				.withWorkingDir(kubernetesJobManagerParameters.getWorkingDir())
				.withResources(requirements)
				.withPorts(getContainerPorts())
				.withEnv(getCustomizedEnvs())
				.addAllToVolumeMounts(volumeMountList)
				.addNewEnv()
					.withName(ENV_FLINK_POD_IP_ADDRESS)
					.withValueFrom(new EnvVarSourceBuilder()
						.withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
						.build())
					.endEnv()
				.addNewEnv()
					.withName(ENV_FLINK_COMPONENT)
					.withValue(LABEL_COMPONENT_JOB_MANAGER)
					.endEnv()
				.addNewEnv()
					.withName(ENV_FLINK_POD_NAME)
					.withValueFrom(new EnvVarSourceBuilder()
						.withNewFieldRef(API_VERSION, POD_NAME_FIELD_PATH)
						.build())
					.endEnv()
				.addNewEnv()
					.withName(ConfigConstants.FLINK_JOB_NAME_KEY)
					.withValue(flinkConfiguration.getString(PipelineOptions.NAME))
					.endEnv()
				.addNewEnv()
					.withName(ConfigConstants.FLINK_JOB_OWNER_KEY)
					.withValue(flinkConfiguration.getString(PipelineOptions.FLINK_JOB_OWNER))
					.endEnv()
				.addNewEnv()
					.withName(ConfigConstants.FLINK_APPLICATION_ID_KEY)
					.withValue(flinkConfiguration.getString(KubernetesConfigOptions.CLUSTER_ID))
					.endEnv()
				.addNewEnv()
					.withName(ConfigConstants.FLINK_QUEUE_KEY)
					.withValue(kubernetesAnnotations.getOrDefault(Constants.KUBERNETES_ANNOTATION_QUEUE_KEY, ConfigConstants.FLINK_QUEUE_DEFAULT))
					.endEnv()
				.addNewEnv()
					.withName(ConfigConstants.FLINK_ENV_TYPE_KEY)
					.withValue(ConfigConstants.FLINK_ENV_TYPE_KUBERNETES)
					.endEnv()
				.build();
	}

	private List<ContainerPort> getContainerPorts() {
		if (kubernetesJobManagerParameters.isHostNetworkEnabled()) {
			// set container port to zero so k8s will allocate one
			// the order is guaranteed as defined in {@link Constants#JOB_MANAGER_CONTAINER_PORT_LIST}
			return Constants.JOB_MANAGER_CONTAINER_PORT_LIST.stream().map(
				portName -> new ContainerPortBuilder()
					.withName(portName)
					.withContainerPort(0)
					.build()
			).collect(Collectors.toList());
		}

		Map<String, Integer> ports = new LinkedHashMap<>();
		ports.put(Constants.REST_PORT_NAME, kubernetesJobManagerParameters.getRestPort());
		ports.put(Constants.JOB_MANAGER_RPC_PORT_NAME, kubernetesJobManagerParameters.getRPCPort());
		ports.put(Constants.BLOB_SERVER_PORT_NAME, kubernetesJobManagerParameters.getBlobServerPort());
		ports.put(Constants.SOCKET_PORT_NAME, kubernetesJobManagerParameters.getSocketPort());

		return KubernetesUtils.getContainerPortsWithUserPorts(ports, kubernetesJobManagerParameters.getJobManagerUserDefinedPorts());
	}

	private List<EnvVar> getCustomizedEnvs() {
		return kubernetesJobManagerParameters.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVarBuilder()
					.withName(kv.getKey())
					.withValue(kv.getValue())
					.build())
			.collect(Collectors.toList());
	}
}
