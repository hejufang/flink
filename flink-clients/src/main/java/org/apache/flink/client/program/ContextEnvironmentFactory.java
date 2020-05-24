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

package org.apache.flink.client.program;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.URL;
import java.util.List;

/**
 * The factory that instantiates the environment to be used when running jobs that are
 * submitted through a pre-configured client connection.
 * This happens for example when a job is submitted from the command line.
 */
public class ContextEnvironmentFactory implements ExecutionEnvironmentFactory {

	private final ClusterClient<?> client;

	private final List<URL> jarFilesToAttach;

	private final List<URL> classpathsToAttach;

	private final ClassLoader userCodeClassLoader;

	private final int defaultParallelism;

	private final boolean isDetached;

	private ExecutionEnvironment lastEnvCreated;

	private SavepointRestoreSettings savepointSettings;

	private Configuration configuration;

	public ContextEnvironmentFactory(ClusterClient<?> client, List<URL> jarFilesToAttach,
			List<URL> classpathsToAttach, ClassLoader userCodeClassLoader, int defaultParallelism,
			boolean isDetached, SavepointRestoreSettings savepointSettings, Configuration configuration) {
		this.client = client;
		this.jarFilesToAttach = jarFilesToAttach;
		this.classpathsToAttach = classpathsToAttach;
		this.userCodeClassLoader = userCodeClassLoader;
		this.defaultParallelism = defaultParallelism;
		this.isDetached = isDetached;
		this.savepointSettings = savepointSettings;
		this.configuration = configuration;
	}

	@Override
	public ExecutionEnvironment createExecutionEnvironment() {
		if (isDetached && lastEnvCreated != null) {
			throw new InvalidProgramException("Multiple environments cannot be created in detached mode");
		}

		lastEnvCreated = isDetached
			? new DetachedEnvironment(client, jarFilesToAttach, classpathsToAttach, userCodeClassLoader, savepointSettings, configuration)
			: new ContextEnvironment(client, jarFilesToAttach, classpathsToAttach, userCodeClassLoader, savepointSettings, configuration);
		if (defaultParallelism > 0) {
			lastEnvCreated.setParallelism(defaultParallelism);
		}
		lastEnvCreated.getConfig().setDefaultPartitioner(
				configuration.getEnum(ExecutionConfig.DefaultPartitioner.class, CoreOptions.DEFAULT_STREAM_PARTITIONER));
		return lastEnvCreated;
	}

	public ExecutionEnvironment getLastEnvCreated() {
		return lastEnvCreated;
	}
}
