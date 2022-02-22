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

package org.apache.flink.kubernetes.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;

/**
 * This class is the executable entry point for running a TaskExecutor in a Kubernetes pod.
 */
public class KubernetesTaskExecutorRunner {

	protected static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskExecutorRunner.class);

	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "Kubernetes TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		final String resourceID = System.getenv().get(Constants.ENV_FLINK_POD_NAME);
		Preconditions.checkArgument(resourceID != null,
			"Pod name variable %s not set", Constants.ENV_FLINK_POD_NAME);

		LOG.info("Kubernetes TM PodName is {} in host {}", System.getenv(Constants.ENV_POD_NAME), System.getenv(Constants.ENV_POD_HOST_IP));
		runTaskManagerSecurely(args, new ResourceID(resourceID));
	}

	/**
	 * The instance entry point for the Kubernetes task executor.
	 * Some Kubernetes specific action should be done in this method.
	 *
	 * @param args The command line arguments.
	 * @param resourceID The resource id for current task manager
	 */
	private static void runTaskManagerSecurely(String[] args, ResourceID resourceID) {
		try {
			final Configuration configuration = TaskManagerRunner.loadConfiguration(args);
			final PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
			FileSystem.initialize(configuration, pluginManager);

			SecurityUtils.install(new SecurityConfiguration(configuration));
			if (KubernetesUtils.isHostNetworkEnabled(configuration)) {
				String rpcPort = KubernetesUtils.getHostPortNumberFromEnv(Constants.TASK_MANAGER_RPC_PORT_NAME);
				String nettyPort = KubernetesUtils.getHostPortNumberFromEnv(Constants.TASK_MANAGER_NETTY_SERVER_NAME);
				String metricsPort = KubernetesUtils.getHostPortNumberFromEnv(Constants.FLINK_METRICS_PORT_NAME);

				configuration.setString(TaskManagerOptions.RPC_PORT, rpcPort);
				configuration.setInteger(NettyShuffleEnvironmentOptions.DATA_BIND_PORT, Integer.parseInt(nettyPort));
				configuration.setString(MetricOptions.QUERY_SERVICE_PORT, metricsPort);
			}
			SecurityUtils.getInstalledContext().runSecured(() -> {
				TaskManagerRunner.runTaskManager(configuration, resourceID, pluginManager);
				return null;
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			LOG.error("TaskManager initialization failed.", strippedThrowable);
			System.exit(TaskManagerRunner.STARTUP_FAILURE_RETURN_CODE);
		}
	}

}
