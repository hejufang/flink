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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.runtime.util.docker.DockerConfigOptions;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for DockerImageDecorator.
 */
public class DockerImageDecoratorTest extends KubernetesJobManagerTestBase {

	private DockerImageDecorator dockerImageDecorator;

	private final String customImageLatestWithHub = "hub.byted.org/base/flink_image_base:latest";

	private final String customImageLatestWithoutHub = "flink_image_base:latest";
	private final String customImageHashtagWithHub = "hub.byted.org/base/flink_image_base:ff7dc2449a16eb027598fca59d86ddc8";
	private final String customImageHashtagWithoutHub = "flink_image_base:ff7dc2449a16eb027598fca59d86ddc8";
	private final String customNamespace = "customBase";

	private static final String DEFAULT_FLINK_CONF_DIR_DOCKER_MODE =
			KubernetesConfigOptions.CUSTOM_IMAGE_COMPATIBLE_FLINK_CONF_MOUNTING_PATH_DEFAULT.defaultValue();

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();
		flinkConfig.setBoolean(KubernetesConfigOptions.CUSTOM_IMAGE_COMPATIBLE, true);
		flinkConfig.setString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, "/opt/tiger/flink_deploy/bin/kube-entry.sh");
		flinkConfig.setString(DockerConfigOptions.DOCKER_IMAGE, customImageHashtagWithHub);
		flinkConfig.setString(DockerConfigOptions.DOCKER_NAMESPACE, customNamespace);
		flinkConfig.setString(DockerConfigOptions.DOCKER_SERVER, "image-manager.byted.org");
		flinkConfig.setString(DockerConfigOptions.DOCKER_REGION, "China-North-LF");
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
	}

	@Test
	public void testCustomNoConflictFlinkConfDir() {
		String expectedFlinkConfDir = "/opt/tiger/flink_dummy";
		String userInputFlinkConfPath = "/opt/tiger/flink_dummy";
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, userInputFlinkConfPath);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		assertTrue(resultFlinkPod.getMainContainer().getEnv().stream()
				.anyMatch(envVar -> envVar.getValue().equals(expectedFlinkConfDir)
						&& envVar.getName().equals(ConfigConstants.ENV_FLINK_CONF_DIR)));
	}

	@Test
	public void testCustomConflictedFlinkConfDir() {
		String userInputFlinkConfPath = "/opt/tiger/flink_deploy/deploy";
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, userInputFlinkConfPath);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		assertTrue(resultFlinkPod.getMainContainer().getEnv().stream()
				.anyMatch(envVar -> envVar.getValue().equals(DEFAULT_FLINK_CONF_DIR_DOCKER_MODE)
						&& envVar.getName().equals(ConfigConstants.ENV_FLINK_CONF_DIR)));
	}

	@Test
	public void testMainContainerVolumeMountPath() {
		String expectedPath = "/opt/tiger/flink_deploy";
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		assertTrue(resultFlinkPod.getMainContainer().getVolumeMounts().stream()
				.anyMatch(volumeMount -> volumeMount.getMountPath().equals(expectedPath)));
	}

	@Test
	public void testInitContainerMountPath() {
		String expectedPath = "/opt/tiger/flink_deploy_copy";
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		assertTrue(resultFlinkPod.getPod().getSpec().getInitContainers().stream()
				.anyMatch(initContainer -> initContainer.getVolumeMounts().stream()
						.anyMatch(volumeMount -> volumeMount.getMountPath().equals(expectedPath))));
	}

	@Ignore
	@Test
	public void testCustomImageLatestHashtagWithoutHubInput() {
		String expectedDockerPsm = "hub.byted.org/base/flink_image_base";

		flinkConfig.setString(DockerConfigOptions.DOCKER_IMAGE, customImageLatestWithoutHub);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		String[] splitImage = resultFlinkPod.getMainContainer().getImage().split(":");
		String dockerPsm = splitImage[0];
		String dockerVersion = splitImage[1];
		assertEquals(expectedDockerPsm, dockerPsm);
		assertNotEquals("latest", dockerVersion);
	}

	@Ignore
	@Test
	public void testCustomImageLatestHashTagWithHubInput() {
		String expectedDockerPsm = "hub.byted.org/base/flink_image_base";

		flinkConfig.setString(DockerConfigOptions.DOCKER_IMAGE, customImageLatestWithHub);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);

		String[] splitImage = resultFlinkPod.getMainContainer().getImage().split(":");
		String dockerPsm = splitImage[0];
		String dockerVersion = splitImage[1];
		assertEquals(expectedDockerPsm, dockerPsm);
		assertNotEquals("latest", dockerVersion);
	}

	@Test
	public void testCustomImageHashtagWithHubInput() {
		flinkConfig.setString(DockerConfigOptions.DOCKER_IMAGE, customImageHashtagWithHub);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);
		String image = resultFlinkPod.getMainContainer().getImage();
		assertEquals(customImageHashtagWithHub, image);
	}

	@Test
	public void testCustomImageHashtagWithoutHubInput() {
		String expectedDockerPsm = "hub.byted.org/customBase/flink_image_base";
		String expectedDockerVersion = "ff7dc2449a16eb027598fca59d86ddc8";

		flinkConfig.setString(DockerConfigOptions.DOCKER_IMAGE, customImageHashtagWithoutHub);
		dockerImageDecorator = new DockerImageDecorator(kubernetesJobManagerParameters);
		final FlinkPod resultFlinkPod = dockerImageDecorator.decorateFlinkPod(baseFlinkPod);

		String[] splitImage = resultFlinkPod.getMainContainer().getImage().split(":");
		String dockerPsm = splitImage[0];
		String dockerVersion = splitImage[1];
		assertEquals(expectedDockerPsm, dockerPsm);
		assertEquals(expectedDockerVersion, dockerVersion);
	}

	@Test
	public void testConfDirInParentPathOfFlinkLib() {
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, "/opt/tiger");
		new DockerImageDecorator(kubernetesJobManagerParameters);
		// FLINK_CONF_DIR is set to the parent path of flink library (/opt/tiger/flink_deploy),
		// should change it to default conf path of docker mode.
		assertEquals(DEFAULT_FLINK_CONF_DIR_DOCKER_MODE, flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));
	}

	@Test
	public void testConfDirInChildrenPathOfFlinkLib() {
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, "/opt/tiger/flink_deploy/dummy_conf");
		new DockerImageDecorator(kubernetesJobManagerParameters);
		// FLINK_CONF_DIR is set to the children path of flink library (/opt/tiger/flink_deploy),
		// should change it to default conf path of docker mode.
		assertEquals(DEFAULT_FLINK_CONF_DIR_DOCKER_MODE, flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));
	}

	@Test
	public void testConfDirSameAsFlinkLib() {
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, "/opt/tiger/flink_deploy");
		new DockerImageDecorator(kubernetesJobManagerParameters);
		// FLINK_CONF_DIR is set to the same path of flink library (/opt/tiger/flink_deploy),
		// should change it to default conf path of docker mode.
		assertEquals(DEFAULT_FLINK_CONF_DIR_DOCKER_MODE, flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));
	}

	@Test
	public void testEffectiveConfDir() {
		flinkConfig.setString(KubernetesConfigOptions.FLINK_CONF_DIR, "/opt/tiger/flink_dummy_conf");
		new DockerImageDecorator(kubernetesJobManagerParameters);
		// FLINK_CONF_DIR is set to a effective path, should not change it second times.
		assertEquals("/opt/tiger/flink_dummy_conf", flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));
	}

}
