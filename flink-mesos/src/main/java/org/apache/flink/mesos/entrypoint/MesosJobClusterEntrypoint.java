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

package org.apache.flink.mesos.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.mesos.runtime.clusterframework.MesosResourceManagerFactory;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServices;
import org.apache.flink.mesos.runtime.clusterframework.services.MesosServicesUtils;
import org.apache.flink.mesos.util.MesosConfiguration;
import org.apache.flink.mesos.util.MesosUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.ClusterEntrypointUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.apache.flink.shaded.org.apache.commons.cli.CommandLine;
import org.apache.flink.shaded.org.apache.commons.cli.CommandLineParser;
import org.apache.flink.shaded.org.apache.commons.cli.Options;
import org.apache.flink.shaded.org.apache.commons.cli.PosixParser;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point for Mesos per-job clusters.
 */
public class MesosJobClusterEntrypoint extends JobClusterEntrypoint {

	// ------------------------------------------------------------------------
	//  Command-line options
	// ------------------------------------------------------------------------

	private static final Options ALL_OPTIONS;

	static {
		ALL_OPTIONS =
			new Options()
				.addOption(BootstrapTools.newDynamicPropertiesOption());
	}

	private MesosConfiguration schedulerConfiguration;

	private MesosServices mesosServices;

	public MesosJobClusterEntrypoint(Configuration config) {
		super(config);
	}

	@Override
	protected void initializeServices(Configuration config, PluginManager pluginManager) throws Exception {
		super.initializeServices(config, pluginManager);

		final String hostname = config.getString(JobManagerOptions.ADDRESS);

		// Mesos configuration
		schedulerConfiguration = MesosUtils.createMesosSchedulerConfiguration(config, hostname);

		// services
		mesosServices = MesosServicesUtils.createMesosServices(config, hostname);
	}

	@Override
	protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
		final CompletableFuture<Void> serviceShutDownFuture = super.stopClusterServices(cleanupHaData);

		return FutureUtils.runAfterwards(
			serviceShutDownFuture,
			() -> {
				if (mesosServices != null) {
					mesosServices.close(cleanupHaData);
				}
			});
	}

	@Override
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			new MesosResourceManagerFactory(
				mesosServices,
				schedulerConfiguration),
			FileJobGraphRetriever.createFrom(configuration, ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null)));
	}

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, MesosJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// load configuration incl. dynamic properties
		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(ALL_OPTIONS, args);
		}
		catch (Exception e){
			LOG.error("Could not parse the command-line options.", e);
			System.exit(STARTUP_FAILURE_RETURN_CODE);
			return;
		}

		Configuration dynamicProperties = BootstrapTools.parseDynamicProperties(cmd);
		Configuration configuration = MesosUtils.loadConfiguration(dynamicProperties, LOG);

		MesosJobClusterEntrypoint clusterEntrypoint = new MesosJobClusterEntrypoint(configuration);

		ClusterEntrypoint.runClusterEntrypoint(clusterEntrypoint);
	}
}