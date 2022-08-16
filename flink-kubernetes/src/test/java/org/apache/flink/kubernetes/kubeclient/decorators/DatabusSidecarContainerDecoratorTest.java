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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.Test;

import java.util.Map;

import static org.apache.flink.kubernetes.kubeclient.decorators.DatabusSideCarContainerDecorator.CONTAINER_NAME;
import static org.apache.flink.kubernetes.kubeclient.decorators.DatabusSideCarContainerDecorator.VOLUME_DATABUS_SHM_NAME;
import static org.apache.flink.kubernetes.kubeclient.decorators.DatabusSideCarContainerDecorator.VOLUME_DATABUS_SOCK_NAME;
import static org.junit.Assert.assertEquals;

/**
 * Test for DatabusSideCarContainerDecorator.
 */
public class DatabusSidecarContainerDecoratorTest extends KubernetesTaskManagerTestBase {

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();
	}

	@Test
	public void testEnabledSidecar() {
		final double sidecarCpu = 0.5;
		final int sidecarMemory = 1024;

		this.flinkConfig.setBoolean(KubernetesConfigOptions.SIDECAR_DATABUS_ENABLED, true);
		this.flinkConfig.setDouble(KubernetesConfigOptions.SIDECAR_DATABUS_CPU, sidecarCpu);
		this.flinkConfig.setString(KubernetesConfigOptions.SIDECAR_DATABUS_MEMORY.key(), sidecarMemory + "m");
		this.flinkConfig.setString(KubernetesConfigOptions.SIDECAR_DATABUS_IMAGE, "test-sidecar-image");
		DatabusSideCarContainerDecorator databusSideCarContainerDecorator = new DatabusSideCarContainerDecorator(kubernetesTaskManagerParameters);
		final FlinkPod resultFlinkPod = databusSideCarContainerDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(1, resultFlinkPod.getPod().getSpec().getContainers().size());
		Container sidecarContainer = resultFlinkPod.getPod().getSpec().getContainers().get(0);
		assertEquals(CONTAINER_NAME, sidecarContainer.getName());

		// verify databus container share volumes with main container.
		VolumeMount databusDeploy = sidecarContainer.getVolumeMounts().get(1);
		VolumeMount databusShm = sidecarContainer.getVolumeMounts().get(2);
		assertEquals(VOLUME_DATABUS_SOCK_NAME, databusDeploy.getName());
		assertEquals(VOLUME_DATABUS_SHM_NAME, databusShm.getName());

		Container mainContainer = resultFlinkPod.getMainContainer();
		assertEquals(VOLUME_DATABUS_SOCK_NAME, mainContainer.getVolumeMounts().get(1).getName());
		assertEquals(VOLUME_DATABUS_SHM_NAME, mainContainer.getVolumeMounts().get(2).getName());

		// verify databus container resource
		ResourceRequirements resourceRequirements = sidecarContainer.getResources();
		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(sidecarCpu), requests.get("cpu").getAmount());
		assertEquals(String.valueOf(sidecarMemory), requests.get("memory").getAmount());
		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(sidecarCpu), limits.get("cpu").getAmount());
		assertEquals(String.valueOf(sidecarMemory), limits.get("memory").getAmount());

		// verify databus volume size
		Volume databusShmVolume = resultFlinkPod.getPod().getSpec().getVolumes().get(3);
		assertEquals(VOLUME_DATABUS_SHM_NAME, databusShmVolume.getName());
		int volumeSize = sidecarMemory * 2 / 3;
		assertEquals(String.valueOf(volumeSize), databusShmVolume.getEmptyDir().getSizeLimit().getAmount());
	}

	@Test
	public void testSidecarShmSize() {
		final double sidecarCpu = 0.5;
		final int sidecarMemory = 1024;
		final int sidecarShmSize = 512;

		this.flinkConfig.setBoolean(KubernetesConfigOptions.SIDECAR_DATABUS_ENABLED, true);
		this.flinkConfig.setDouble(KubernetesConfigOptions.SIDECAR_DATABUS_CPU, sidecarCpu);
		this.flinkConfig.setString(KubernetesConfigOptions.SIDECAR_DATABUS_MEMORY.key(), sidecarMemory + "m");
		this.flinkConfig.setString(KubernetesConfigOptions.SIDECAR_DATABUS_SHARED_MEMORY_SIZE.key(), sidecarShmSize + "m");
		this.flinkConfig.setString(KubernetesConfigOptions.SIDECAR_DATABUS_IMAGE, "test-sidecar-image");
		DatabusSideCarContainerDecorator databusSideCarContainerDecorator = new DatabusSideCarContainerDecorator(kubernetesTaskManagerParameters);
		final FlinkPod resultFlinkPod = databusSideCarContainerDecorator.decorateFlinkPod(baseFlinkPod);

		// verify databus volume size
		Volume databusShmVolume = resultFlinkPod.getPod().getSpec().getVolumes().get(3);
		assertEquals(VOLUME_DATABUS_SHM_NAME, databusShmVolume.getName());
		assertEquals(String.valueOf(sidecarShmSize), databusShmVolume.getEmptyDir().getSizeLimit().getAmount());
	}
}
