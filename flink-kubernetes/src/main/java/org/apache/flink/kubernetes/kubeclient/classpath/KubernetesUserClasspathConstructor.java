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

package org.apache.flink.kubernetes.kubeclient.classpath;

import org.apache.flink.client.deployment.application.classpath.UserClasspathConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.function.FunctionUtils;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The user classpath constructor in kubernetes application mode.
 */
public enum KubernetesUserClasspathConstructor implements UserClasspathConstructor {

	INSTANCE;

	@Override
	public List<URL> getUserJar(Configuration flinkConfiguration) {
		// get user jar
		if (flinkConfiguration.contains(PipelineOptions.JARS)) {
			return KubernetesUtils.checkJarFileForApplicationMode(flinkConfiguration)
					.stream()
					.map(FunctionUtils.uncheckedFunction(file -> file.toURI().toURL()))
					.collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	@Override
	public List<URL> getExternalJars(Configuration flinkConfiguration) {
		// get external files
		return KubernetesUtils.getExternalFiles(flinkConfiguration);
	}
}
