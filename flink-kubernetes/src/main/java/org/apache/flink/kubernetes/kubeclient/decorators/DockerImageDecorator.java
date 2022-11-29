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
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.runtime.util.docker.DockerConfigOptions;
import org.apache.flink.runtime.util.docker.DockerUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * To keep the compatibility to use docker.image as same as yarn, this decorator will set up some mechanism to download
 * flink library to a shared empty dir with main container.
 */
public class DockerImageDecorator extends AbstractKubernetesStepDecorator {

	private static final Logger LOG = LoggerFactory.getLogger(DockerImageDecorator.class);
	private static final String FLINK_LIB_VOLUME_NAME = "flink-lib-volume";
	private static final String FLINK_DEPLOY_COPIER_NAME = "flink-deploy-copier";

	private final AbstractKubernetesParameters kubernetesParameters;
	private final boolean isDockerCompatibleMode;
	private final String libVolumeMountingPathInitContainer;

	private String customDockerImage = "";
	private String flinkHome;

	public DockerImageDecorator(AbstractKubernetesParameters kubernetesParameters) {
		try {
			this.kubernetesParameters = checkNotNull(kubernetesParameters);
			this.isDockerCompatibleMode = kubernetesParameters.getCustomDockerCompatibility();
			this.libVolumeMountingPathInitContainer = kubernetesParameters.getFlinkConfiguration()
					.getString(KubernetesConfigOptions.CUSTOM_IMAGE_COMPATIBLE_FLINK_LIB_MOUNTING_PATH_FOR_INIT_CONTAINER);
			if (this.isDockerCompatibleMode) {
				this.flinkHome = kubernetesParameters.findFlinkHomeInContainer();
				// flink-lib-volume is mounted to the parent or children path of the default mounting path of flink-conf-volume.
				// As conflicting mount paths are not allowed, we set a custom flink config directory in the case when
				// the config (either default or by the user) is conflicting with the flink-lib-volume mounting path.
				Path flinkConfPath = Paths.get(kubernetesParameters.getFlinkConfDirInPod());
				Path flinkLibPath = Paths.get(flinkHome);
				if (flinkConfPath.startsWith(flinkLibPath) || flinkLibPath.startsWith(flinkConfPath)) {
					String defaultFlinkConfVolumeMountingPath = kubernetesParameters.getFlinkConfiguration()
							.getString(KubernetesConfigOptions.CUSTOM_IMAGE_COMPATIBLE_FLINK_CONF_MOUNTING_PATH_DEFAULT);
					kubernetesParameters.getFlinkConfiguration().setString(KubernetesConfigOptions.FLINK_CONF_DIR, defaultFlinkConfVolumeMountingPath);
					LOG.error("{} given has conflicts with path of flink library, setting it as {}.", KubernetesConfigOptions.FLINK_CONF_DIR.key(), defaultFlinkConfVolumeMountingPath);
				}
				// check docker image
				String customDockerImage = kubernetesParameters.getFlinkConfiguration().getString(DockerConfigOptions.DOCKER_IMAGE).trim();
				this.customDockerImage = DockerUtils.getDockerImage(customDockerImage, kubernetesParameters.getFlinkConfiguration());
				kubernetesParameters.getFlinkConfiguration().setString(DockerConfigOptions.DOCKER_IMAGE, this.customDockerImage);
			}
		} catch (IOException e) {
			LOG.error("ERROR occurred while get real docker image", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		if (!isDockerCompatibleMode) {
			return flinkPod;
		}
		// flink lib volume
		final Volume flinkLibVolume = new VolumeBuilder()
				.withName(FLINK_LIB_VOLUME_NAME)
				.withNewEmptyDir()
				.endEmptyDir()
				.build();
		// init container to download remote files
		Container flinkLibCopier = createInitContainerForCopyingFlinkLib();
		final Pod basicPod = new PodBuilder(flinkPod.getPod())
				.editOrNewSpec()
				.addToInitContainers(flinkLibCopier)
				.addToVolumes(flinkLibVolume)
				.endSpec()
				.build();
		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());
		return new FlinkPod.Builder(flinkPod)
				.withPod(basicPod)
				.withMainContainer(basicMainContainer)
				.build();
	}

	private Container decorateMainContainer(Container mainContainer) {
		// update image of main container to customer docker image and set flink conf dir env
		return new ContainerBuilder(mainContainer)
				.withImage(customDockerImage)
				.addNewVolumeMount()
					.withName(FLINK_LIB_VOLUME_NAME)
					.withMountPath(flinkHome)
				.endVolumeMount()
				.addNewEnv()
					.withName(ConfigConstants.ENV_FLINK_CONF_DIR)
					.withValue(kubernetesParameters.getFlinkConfDirInPod())
				.endEnv()
				.build();
	}

	private Container createInitContainerForCopyingFlinkLib() {
		//copy flink deploy to backup folder
		String copyCommand = String.format("cp -r %s/* %s", flinkHome, libVolumeMountingPathInitContainer);
		return new ContainerBuilder()
				.withName(FLINK_DEPLOY_COPIER_NAME)
				.withImage(kubernetesParameters.getImage())
				.withImagePullPolicy(kubernetesParameters.getImagePullPolicy().name())
				.withWorkingDir(kubernetesParameters.getWorkingDir())
				.withCommand("/bin/bash")
				.withArgs(Arrays.asList("-c", copyCommand))
				.addNewVolumeMount()
					.withName(FLINK_LIB_VOLUME_NAME)
					.withMountPath(libVolumeMountingPathInitContainer)
				.endVolumeMount()
				.build();
	}

}
