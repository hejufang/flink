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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.entrypoint.component.JobDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.FlinkVersionReporter;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.flink.yarn.ZkUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import com.bytedance.btrace.ByteTrace;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.yarn.Utils.require;

/**
 * Entry point for Yarn per-job clusters.
 */
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

	private final String workingDirectory;

	public YarnJobClusterEntrypoint(
			Configuration configuration,
			String workingDirectory) {

		super(configuration);
		this.workingDirectory = Preconditions.checkNotNull(workingDirectory);
	}

	@Override
	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		return YarnEntrypointUtils.installSecurityContext(configuration, workingDirectory);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory<?> createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return new JobDispatcherResourceManagerComponentFactory(
			YarnResourceManagerFactory.getInstance(),
			FileJobGraphRetriever.createFrom(configuration));
	}

	/**
	 * Start a flink version reporter.
	 * @param flinkConfig flink configuration.
	 * */
	private static void startVersionReporter(Configuration flinkConfig) {
		try {
			FlinkVersionReporter flinkVersionReporter = new FlinkVersionReporter(flinkConfig);
			new Thread(flinkVersionReporter).start();
		} catch (Throwable t) {
			LOG.warn("Failed to start the flink version reporter.", t);
		}
	}

	private static void checkJobUnique(CuratorFramework client, Configuration config) throws Exception {
		boolean checkJobUnique = config.getBoolean("check.job.unique",  true);
		if (!checkJobUnique) {
			return;
		}
		client.start();
		String dc = config.getString("dc", null);
		require(dc != null && !dc.isEmpty(), "Dc not set.");
		String cluster = config.getString(ConfigConstants.CLUSTER_NAME_KEY, null);
		require(cluster != null && !cluster.isEmpty(), "Cluster not set.");
		String appName = System.getenv().get(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		require(appName != null && !appName.isEmpty(), "AppName not set.");
		String jobName = appName;
		if (appName.lastIndexOf("_") > 0) {
			jobName = appName.substring(0, appName.lastIndexOf("_"));
		}
		require(jobName != null && !jobName.isEmpty(), "JobName not set.");
		String zkRoot = config.getString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);
		require(zkRoot != null && !zkRoot.isEmpty(), "Zookeeper root path not set.");

		String zkPath = String.format("%s/uniqueness/%s/%s/%s", zkRoot, dc, cluster, jobName);
		if (client.checkExists().forPath(zkPath) != null) {
			LOG.error("Job {} already exists on {} {}", jobName, dc, cluster);
			throw new Exception("Job already exists Exception");
		} else {
			LOG.info("Job {} does not exist on {} {}, continue...", jobName, dc, cluster);
			LOG.info("Create zk node {}", zkPath);
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkPath);
		}
	}


	// ------------------------------------------------------------------------
	//  The executable entry point for the Yarn Application Master Process
	//  for a single Flink job.
	// ------------------------------------------------------------------------

	public static void main(String[] args) {
		// startup checks and logging
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();

		ByteTrace.initialize(env, null, null, null);

		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}

		Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env, LOG);

		YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(
			configuration,
			workingDirectory);

		CuratorFramework client = ZkUtils.newZkClient(configuration);
		try {
			checkJobUnique(client, configuration);
		} catch (Exception e) {
			LOG.error("error while check job unique", e);
			System.exit(-1);
		}
		yarnJobClusterEntrypoint.getTerminationFuture().whenComplete(((applicationStatus, throwable) -> {
			try {
				client.close();
			} catch (Throwable t) {
				LOG.error("Error shutting down CuratorFramework client", t);
			}
		}));

		// start a Flink version reporter.
		startVersionReporter(configuration);

		ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
	}
}
