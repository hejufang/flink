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
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesToleration;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An initializer for the TaskManager {@link org.apache.flink.kubernetes.kubeclient.FlinkPod}.
 */
public class InitTaskManagerDecorator extends AbstractKubernetesStepDecorator {

	private final KubernetesTaskManagerParameters kubernetesTaskManagerParameters;
	private final Map<String, Tuple2<String, String>> mountedHostPaths;

	public InitTaskManagerDecorator(KubernetesTaskManagerParameters kubernetesTaskManagerParameters) {
		this.kubernetesTaskManagerParameters = checkNotNull(kubernetesTaskManagerParameters);
		this.mountedHostPaths = kubernetesTaskManagerParameters.getMountedHostPath();
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
			.withApiVersion(Constants.API_VERSION)
			.editOrNewMetadata()
				.withName(kubernetesTaskManagerParameters.getPodName())
				.withLabels(kubernetesTaskManagerParameters.getLabels())
				.withAnnotations(kubernetesTaskManagerParameters.getAnnotations())
				.endMetadata()
			.editOrNewSpec()
				.withServiceAccountName(kubernetesTaskManagerParameters.getServiceAccount())
				.withRestartPolicy(Constants.RESTART_POLICY_OF_NEVER)
				.withImagePullSecrets(kubernetesTaskManagerParameters.getImagePullSecrets())
				.withNodeSelector(kubernetesTaskManagerParameters.getNodeSelector())
				.withTolerations(kubernetesTaskManagerParameters.getTolerations().stream()
					.map(e -> KubernetesToleration.fromMap(e).getInternalResource())
					.collect(Collectors.toList()))
				.addAllToVolumes(volumeList)
				.endSpec()
			.build();

		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());

		return new FlinkPod.Builder(flinkPod)
			.withPod(basicPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private Container decorateMainContainer(Container container) {
		final ResourceRequirements resourceRequirements = KubernetesUtils.getResourceRequirements(
				kubernetesTaskManagerParameters.getTaskManagerMemoryMB(),
				kubernetesTaskManagerParameters.getTaskManagerCPU(),
				kubernetesTaskManagerParameters.getTaskManagerExternalResources());
		List<VolumeMount> volumeMountList = new ArrayList<>(mountedHostPaths.size());
		for  (Map.Entry<String,  Tuple2<String, String>> entry: mountedHostPaths.entrySet()) {
			final VolumeMount volumeMount = new VolumeMountBuilder()
				.withName(entry.getKey())
				.withMountPath(entry.getValue().f1)
				.withReadOnly(true)
				.build();
			volumeMountList.add(volumeMount);
		}
		return new ContainerBuilder(container)
				.withName(kubernetesTaskManagerParameters.getTaskManagerMainContainerName())
				.withImage(kubernetesTaskManagerParameters.getImage())
				.withImagePullPolicy(kubernetesTaskManagerParameters.getImagePullPolicy().name())
				.withWorkingDir(kubernetesTaskManagerParameters.getWorkingDir())
				.withResources(resourceRequirements)
				.addAllToVolumeMounts(volumeMountList)
				.withPorts(getContainerPorts())
				.withEnv(getCustomizedEnvs())
				.addNewEnv()
					.withName(ENV_FLINK_POD_NAME)
					.withValue(kubernetesTaskManagerParameters.getPodName())
					.endEnv()
				.build();
	}

	private List<ContainerPort> getContainerPorts() {
		Map<String, Integer> systemPorts = new LinkedHashMap<>();
		systemPorts.put(Constants.TASK_MANAGER_RPC_PORT_NAME, kubernetesTaskManagerParameters.getRPCPort());

		return KubernetesUtils.getContainerPortsWithUserPorts(systemPorts, kubernetesTaskManagerParameters.getTaskManagerUserDefinedPorts());
	}

	private List<EnvVar> getCustomizedEnvs() {
		return kubernetesTaskManagerParameters.getEnvironments()
			.entrySet()
			.stream()
			.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
			.collect(Collectors.toList());
	}
}
