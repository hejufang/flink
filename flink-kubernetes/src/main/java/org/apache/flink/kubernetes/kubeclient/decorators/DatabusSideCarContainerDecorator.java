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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This decorator decorate pod by databus side-car container which will be used to isolate with databus agent in host.
 */
public class DatabusSideCarContainerDecorator extends AbstractKubernetesStepDecorator {

	public static final String CONTAINER_NAME = "databus";
	private static final String CONTAINER_COMMAND = "/bin/bash";
	private static final List<String> CONTAINER_ARGS = Arrays.asList(
			"-c",
			"python /opt/tiger/databus_deploy/databus2/svc/run");
	private static final String ENV_DATABUS_SIDECAR_PATH_KEY = "DATABUS_PATH";
	private static final String ENV_DATABUS_SIDECAR_PATH_VALUE = "/opt/tiger/databus_deploy";
	private static final String ENV_DATABUS_SIDECAR_ENABLE_KEY = "DATABUS_SIDECAR_ENABLE";
	private static final String ENV_DATABUS_SIDECAR_ENABLE_VALUE = "TRUE";
	private static final String ENV_IS_READY_CHECK_KEY = "IS_READY_CHECK";
	private static final String ENV_IS_READY_CHECK_VALUE = "1";
	// For Godel collect metrics for sidecar container.
	private static final String ENV_TCE_IS_INJECTED_KEY = "TCE_IS_INJECTED";
	private static final String ENV_TCE_IS_INJECTED_VALUE = "1";
	// fixed psm that is managed by databus.
	private static final String ENV_DATABUS_PSM_KEY = "TCE_PSM";
	private static final String ENV_DATABUS_PSM_VALUE = "bytedance.inf.databus_sidecar";

	private static final String VOLUME_RUN_NAME = "run";
	private static final String VOLUME_RUN_DATABUS_NAME = "run-databus";
	static final String VOLUME_DATABUS_SOCK_NAME = "sharevolume-databus-sock";
	static final String VOLUME_DATABUS_SHM_NAME = "sharevolume-databus-shm";

	private static final String VOLUME_RUN_PATH = "/run";
	private static final String VOLUME_RUN_DATABUS_PATH = "/run";
	private static final String VOLUME_DATABUS_SOCK_PATH = "/opt/tiger/databus_deploy/sock";
	private static final String VOLUME_DATABUS_SHM_PATH = "/dev/shm";
	private static final int DEFAULT_MEMORY_VOLUME_SIZE = 64;

	private final double cpu;
	private final int memoryInMB;
	private final int volumeRunSize = DEFAULT_MEMORY_VOLUME_SIZE;
	private final int volumeRunDatabusSize = DEFAULT_MEMORY_VOLUME_SIZE;
	private final int volumeDatabusSockSize = DEFAULT_MEMORY_VOLUME_SIZE;
	private final int volumeDatabusShmSize;
	private final String containerImage;

	public DatabusSideCarContainerDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.cpu = kubernetesParameters.getDatabusSidecarCpu();
		this.memoryInMB = kubernetesParameters.getDatabusSidecarMemoryInMB();
		this.volumeDatabusShmSize = kubernetesParameters.getDatabusSidecarShmSizeInMB();
		this.containerImage = kubernetesParameters.getDatabusSidecarImage();
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		// volume
		final Volume runVolume = new VolumeBuilder()
				.withName(VOLUME_RUN_NAME)
				.withNewEmptyDir()
					.withMedium("Memory")
					.withSizeLimit(new Quantity(volumeRunSize + Constants.RESOURCE_UNIT_MB))
				.endEmptyDir()
				.build();
		final Volume runDatabusVolume = new VolumeBuilder()
				.withName(VOLUME_RUN_DATABUS_NAME)
				.withNewEmptyDir()
					.withMedium("Memory")
					.withSizeLimit(new Quantity(volumeRunDatabusSize + Constants.RESOURCE_UNIT_MB))
				.endEmptyDir()
				.build();
		final Volume databus0Volume = new VolumeBuilder()
				.withName(VOLUME_DATABUS_SOCK_NAME)
				.withNewEmptyDir()
					.withMedium("Memory")
					.withSizeLimit(new Quantity(volumeDatabusSockSize + Constants.RESOURCE_UNIT_MB))
				.endEmptyDir()
				.build();
		final Volume databus1Volume = new VolumeBuilder()
				.withName(VOLUME_DATABUS_SHM_NAME)
				.withNewEmptyDir()
					.withMedium("Memory")
					.withSizeLimit(new Quantity(volumeDatabusShmSize + Constants.RESOURCE_UNIT_MB))
				.endEmptyDir()
				.build();

		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());
		final Container databusContainer = createDatabusContainer(basicMainContainer);

		final Pod basicPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
				.addToContainers(databusContainer)
				.addToVolumes(runVolume)
				.addToVolumes(runDatabusVolume)
				.addToVolumes(databus0Volume)
				.addToVolumes(databus1Volume)
			.endSpec()
			.build();
		return new FlinkPod.Builder(flinkPod)
			.withPod(basicPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private Container createDatabusContainer(Container basicMainContainer) {
		return new ContainerBuilder()
				.withName(CONTAINER_NAME)
				.withImage(containerImage)
				.withImagePullPolicy(basicMainContainer.getImagePullPolicy())
				.withResources(KubernetesUtils.getResourceRequirements(memoryInMB, cpu, Collections.emptyMap()))
				.withCommand(CONTAINER_COMMAND)
				.withArgs(CONTAINER_ARGS)
				.addNewEnv()
					.withName(ENV_DATABUS_SIDECAR_PATH_KEY)
					.withValue(ENV_DATABUS_SIDECAR_PATH_VALUE)
				.endEnv()
				.addNewEnv()
					.withName(ENV_DATABUS_SIDECAR_ENABLE_KEY)
					.withValue(ENV_DATABUS_SIDECAR_ENABLE_VALUE)
				.endEnv()
				.addNewEnv()
					.withName(ENV_IS_READY_CHECK_KEY)
					.withValue(ENV_IS_READY_CHECK_VALUE)
				.endEnv()
				.addNewEnv()
					.withName(ENV_TCE_IS_INJECTED_KEY)
					.withValue(ENV_TCE_IS_INJECTED_VALUE)
				.endEnv()
				.addNewEnv()
					.withName(ENV_DATABUS_PSM_KEY)
					.withValue(ENV_DATABUS_PSM_VALUE)
				.endEnv()
				.addNewVolumeMount()
					.withName(VOLUME_RUN_DATABUS_NAME)
					.withMountPath(VOLUME_RUN_DATABUS_PATH)
				.endVolumeMount()
				.addNewVolumeMount()
					.withName(VOLUME_DATABUS_SOCK_NAME)
					.withMountPath(VOLUME_DATABUS_SOCK_PATH)
				.endVolumeMount()
				.addNewVolumeMount()
					.withName(VOLUME_DATABUS_SHM_NAME)
					.withMountPath(VOLUME_DATABUS_SHM_PATH)
				.endVolumeMount()
				.build();
	}

	private Container decorateMainContainer(Container mainContainer) {
		return new ContainerBuilder(mainContainer)
			.addNewEnv()
				.withName(ENV_DATABUS_SIDECAR_PATH_KEY)
				.withValue(ENV_DATABUS_SIDECAR_PATH_VALUE)
			.endEnv()
			.addNewVolumeMount()
				.withName(VOLUME_RUN_NAME)
				.withMountPath(VOLUME_RUN_PATH)
			.endVolumeMount()
			.addNewVolumeMount()
				.withName(VOLUME_DATABUS_SOCK_NAME)
				.withMountPath(VOLUME_DATABUS_SOCK_PATH)
			.endVolumeMount()
			.addNewVolumeMount()
				.withName(VOLUME_DATABUS_SHM_NAME)
				.withMountPath(VOLUME_DATABUS_SHM_PATH)
			.endVolumeMount()
			.build();
	}
}
