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

import org.apache.flink.client.deployment.application.classpath.UserClasspathConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.classpath.KubernetesUserClasspathConstructor;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.stream.Collectors;

/**
 * This decorator is used to add flink user classpath environment variables to container.
 * Then in the config.sh script, that env variable will be added to the head of the whole flink classpath.
 */
public class UserClasspathDecorator extends AbstractKubernetesStepDecorator {

	private static final Logger LOG = LoggerFactory.getLogger(UserClasspathDecorator.class);

	private final AbstractKubernetesParameters kubernetesParameters;

	public UserClasspathDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = kubernetesParameters;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		// whether to make the user classpath setting as same as yarn per job mode
		if (!kubernetesParameters.keepUserClasspathCompatible()) {
			return flinkPod;
		}
		Container mainContainer = decorateMainContainer(flinkPod.getMainContainer());
		return new FlinkPod.Builder(flinkPod)
				.withMainContainer(mainContainer)
				.build();
	}

	private Container decorateMainContainer(Container container) {
		String userClasspath = getFlinkUserClasspath(kubernetesParameters.getFlinkConfiguration());
		if (StringUtils.isNullOrWhitespaceOnly(userClasspath)) {
			return container;
		}
		return new ContainerBuilder(container)
				.addNewEnv()
				.withName(Constants.FLINK_USER_CLASSPATH_ENV_KEY)
				.withValue(userClasspath)
				.endEnv()
				.build();
	}

	protected String getFlinkUserClasspath(Configuration flinkConfiguration) {
		if (System.getenv(Constants.FLINK_USER_CLASSPATH_ENV_KEY) != null) {
			// if this is in JobManager container, the user classpath environment variable has been set, so return
			// that environment variable directly for task managers.
			return System.getenv(Constants.FLINK_USER_CLASSPATH_ENV_KEY);
		}
		String flinkHome = findFlinkHomeInContainer();
		try {
			return UserClasspathConstructor
					.getFlinkUserClasspath(KubernetesUserClasspathConstructor.INSTANCE, flinkConfiguration, null, flinkHome)
					.stream()
					.map(URL::getPath)
					.collect(Collectors.joining(":"));
		} catch (IOException e) {
			LOG.error("get user classpath error", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Find the flink home path by analyzing the path of container entrypoint script. Noted this flink home path will not
	 * be ended with "/".
	 *
	 * @return Flink Home directory
	 */
	public String findFlinkHomeInContainer() {
		String kubeEntryPath = kubernetesParameters.getContainerEntrypoint();
		return new File(kubeEntryPath).getParentFile().getParent();
	}

}
