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

import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This decorator decorate pod by InitContainer which will be used to download remote file from HTTP/HDFS/S3 etc.
 */
public class FileDownloadDecorator extends AbstractKubernetesStepDecorator {

	private final String fileMountedPath;
	private final AbstractKubernetesParameters kubernetesParameters;
	private final List<URI> remoteFiles = new ArrayList<>();

	public FileDownloadDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = checkNotNull(kubernetesParameters);
		this.fileMountedPath = kubernetesParameters.getFlinkConfiguration().getString(PipelineOptions.FILE_MOUNTED_PATH);
		List<URI> userJars = getRemoteFilesForApplicationMode(kubernetesParameters.getFlinkConfiguration(), PipelineOptions.JARS);
		Preconditions.checkArgument(userJars.size() <= 1, "Should only have one jar");
		List<URI> uris = getRemoteFilesForApplicationMode(kubernetesParameters.getFlinkConfiguration(), PipelineOptions.EXTERNAL_RESOURCES);
		this.remoteFiles.addAll(userJars);
		this.remoteFiles.addAll(uris);
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		if (remoteFiles.isEmpty()) {
			return flinkPod;
		}
		// emptyDir type volume
		final Volume flinkConfVolume = new VolumeBuilder()
			.withName(Constants.FILE_DOWNLOAD_VOLUME)
			.withNewEmptyDir()
			.endEmptyDir()
			.build();
		final Container basicMainContainer = decorateMainContainer(flinkPod.getMainContainer());
		// init container to download remote files
		final Container initContainer = createInitContainer(basicMainContainer);
		final Pod basicPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
			.addToInitContainers(initContainer)
			.addToVolumes(flinkConfVolume)
			.endSpec()
			.build();
		return new FlinkPod.Builder(flinkPod)
			.withPod(basicPod)
			.withMainContainer(basicMainContainer)
			.build();
	}

	private static List<URI> getRemoteFilesForApplicationMode(Configuration configuration, ConfigOption<List<String>> fileOption) {
		if (!configuration.contains(fileOption)) {
			return Collections.emptyList();
		}
		return configuration.get(fileOption).stream()
			.map(
				FunctionUtils.uncheckedFunction(
					PackagedProgramUtils::resolveURI))
			.filter(uri -> !uri.getScheme().equals("local") && !uri.getScheme().equals("file"))
			.collect(Collectors.toList());
	}

	private Container createInitContainer(Container basicMainContainer) {
		String remoteFiles = this.remoteFiles.stream()
			.map(URI::toString)
			.collect(Collectors.joining(";"));
		// By default, use command `bin/flink download [source file list] [target directory]`
		String downloadTemplate = kubernetesParameters.getFlinkConfiguration().getString(PipelineOptions.DOWNLOAD_TEMPLATE);
		String downloadCommand = downloadTemplate.replace("%files%", remoteFiles)
			.replace("%target%", fileMountedPath);
		// we need to mount flink conf volume, hadoop conf volume, and downloader volume
		List<VolumeMount> volumeMounts = basicMainContainer.getVolumeMounts();
		List<EnvVar> envVarList = basicMainContainer.getEnv();
		return new ContainerBuilder()
			.withName("downloader")
			.withImage(kubernetesParameters.getImage())
			.withCommand(kubernetesParameters.getContainerEntrypoint())
			.withArgs(Arrays.asList("/bin/bash", "-c", downloadCommand))
			.addAllToVolumeMounts(volumeMounts)
			.addAllToEnv(envVarList)
			.build();
	}

	private Container decorateMainContainer(Container mainContainer) {
		return new ContainerBuilder(mainContainer)
			.addNewVolumeMount()
			.withName(Constants.FILE_DOWNLOAD_VOLUME)
			.withMountPath(fileMountedPath)
			.endVolumeMount()
			.build();
	}

	/**
	 * The save path of remote file downloaded by init container should be obtained from this method.
	 */
	public static String getDownloadedPath(URI uri, Configuration configuration) {
		String fileMountedPath = configuration.getString(PipelineOptions.FILE_MOUNTED_PATH);
		return String.join("/", fileMountedPath, new Path(uri).getName());
	}

}
