/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.flink.topology;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.FlinkMiniCluster;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.parser.ConfigParser;
import com.bytedance.flink.pojo.JobConfig;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Flink submitter, the main entrance class.
 */
public class FlinkSubmitter {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkSubmitter.class);
	private String flinkConfDir;
	private Configuration flinkConfig;
	private String userJar;
	private String yamlConf;
	private String clusterName;
	private boolean isDetached;
	/**
	 * The Flink mini cluster on which to execute the programs.
	 */
	private FlinkMiniCluster flinkMiniCluster;
	private FlinkTopologyBuilder flinkTopologyBuilder;

	public static void main(String[] args) throws Exception {
		String[] newArgs = new String[args.length];
		for (int i = 0; i < args.length; i++) {
			newArgs[i] = args[i].replace('#', ' ');
		}
		FlinkSubmitter flinkSubmitter = new FlinkSubmitter();
		flinkSubmitter.submit(newArgs);
	}

	public void submit(String[] args) throws Exception {
		parseConfig(args);
		JobConfig jobConfig = flinkTopologyBuilder.getJobConfig();
		RuntimeConfig runtimeConfig = parseRuntimeConfig(jobConfig);
		flinkTopologyBuilder.getEnv().getConfig().setGlobalJobParameters(runtimeConfig);
		if (jobConfig.getRunMode() == 0) {
			LOG.info("Run job on cluster mode");
			jobConfig.setDetached(isDetached);
			submitClusterJob();
		} else {
			LOG.info("Run job on local mode");
			jobConfig.setDetached(false);
			submitLocalJob();
			int runTimeMs = jobConfig.getRunSeconds() * 1000;
			LOG.info("The local cluster will shut down after {} seconds.", jobConfig.getRunSeconds());
			Thread.sleep(runTimeMs);
			if (flinkMiniCluster != null) {
				flinkMiniCluster.stop();
				flinkMiniCluster = null;
			}
		}
	}

	private void parseConfig(String[] args) throws Exception {
		// 1.load system property
		loadSystemProperty();
		// 2.load configuration in flink-conf.yaml
		flinkConfig = GlobalConfiguration.loadConfiguration();
		// 3.load job configuration in user yaml
		ConfigParser configParser = new ConfigParser(yamlConf);
		JobConfig jobConfig = configParser.parse();

		LOG.info("jobConfig = {}", jobConfig);
		flinkTopologyBuilder = new FlinkTopologyBuilder(jobConfig);
		String[] flinkYarnArgs = Arrays.copyOfRange(args, 0, args.length);
		flinkTopologyBuilder.setFlinkYarnArgs(flinkYarnArgs);
		flinkTopologyBuilder.build();
	}

	private void loadSystemProperty() {
		flinkConfDir = System.getProperty(Constants.FLINK_CONF_DIR_PROPERTY);
		assert flinkConfDir != null;
		yamlConf = System.getProperty(Constants.YAML_CONF_PROPERTY);
		assert yamlConf != null;
		clusterName = System.getProperty(Constants.CLUSTER_NAME);
		assert yamlConf != null;

		userJar = System.getProperty(Constants.USER_JAR);
		assert userJar != null;
		isDetached = Boolean.valueOf(System.getProperty(Constants.DETACHED_PROPERTY, "true"));
	}

	private void submitClusterJob() {
		final URI uploadedJarUri;
		final URL uploadedJarUrl;
		try {
			uploadedJarUri = new File(userJar).getAbsoluteFile().toURI();
			uploadedJarUrl = uploadedJarUri.toURL();
			JobWithJars.checkJarFile(uploadedJarUrl);
		} catch (final IOException e) {
			throw new RuntimeException("Problem with jar file " + userJar, e);
		}

		StreamGraph streamGraph = flinkTopologyBuilder.getEnv().getStreamGraph();
		streamGraph.setJobName(flinkTopologyBuilder.getJobName());

		JobGraph jobGraph = streamGraph.getJobGraph();
		jobGraph.addJar(new Path(uploadedJarUri));

		ClusterClient client;
		try {
			client = getClusterClient();
		} catch (Exception e) {
			throw new RuntimeException("Could not establish a connection to the job manager", e);
		}
		try {
			ClassLoader classLoader = JobWithJars.buildUserCodeClassLoader(
				Collections.singletonList(uploadedJarUrl),
				Collections.emptyList(),
				this.getClass().getClassLoader());
			boolean isDetached = flinkTopologyBuilder.getJobConfig().isDetached();
			if (isDetached) {
				client.runDetached(jobGraph, classLoader);
			} else {
				client.run(jobGraph, classLoader);
			}
		} catch (final ProgramInvocationException e) {
			throw new RuntimeException("Cannot execute job due to ProgramInvocationException", e);
		}
	}

	private void submitLocalJob() throws JobExecutionException {
		StreamGraph streamGraph = flinkTopologyBuilder.getEnv().getStreamGraph();
		streamGraph.setJobName(flinkTopologyBuilder.getJobName());

		JobGraph jobGraph = streamGraph.getJobGraph();
		if (flinkMiniCluster == null) {
			Configuration configuration = new Configuration();
			configuration.addAll(jobGraph.getJobConfiguration());

			configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, -1L);
			configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

			this.flinkMiniCluster = new LocalFlinkMiniCluster(configuration, true);
			this.flinkMiniCluster.start();
			this.flinkMiniCluster.submitJobAndWait(jobGraph, false);
		}
	}

	private ClusterClient getClusterClient() throws Exception {
		YarnCommandLine yarnCommandLine = new YarnCommandLine(flinkConfDir);
		return yarnCommandLine.createClusterClient(flinkTopologyBuilder.getFlinkYarnArgs());
	}

	private RuntimeConfig parseRuntimeConfig(JobConfig jobConfig) {
		RuntimeConfig runtimeConfig = new RuntimeConfig();
		runtimeConfig.setJobName(jobConfig.getJobName());
		runtimeConfig.setOwners(jobConfig.getOwners());
		runtimeConfig.setClusterName(clusterName);
		runtimeConfig.setIgnoreMismatchedMsg(jobConfig.isIgnoreMismatchedMsg());

		// Infer that we are in new mode, user can access through 'conf' in callee
		runtimeConfig.put(Constants.USE_NEW_MODE, true);
		List<String> resources = EnvironmentInitUtils.getResourceFileList(userJar);
		if (resources == null || resources.isEmpty()) {
			throw new RuntimeException("Resource list cannot be null or empty.");
		}
		runtimeConfig.setResourceFiles(EnvironmentInitUtils.getResourceFileList(userJar));

		String tempDir;
		if (jobConfig.getRunMode() == 0) {
			// cluster mode
			tempDir = Constants.TEMP_DIR_VAL;
		} else {
			// local mode
			tempDir = flinkConfig.getString(CoreOptions.TMP_DIRS, "");
		}
		runtimeConfig.setCodeDir(Paths.get(tempDir, Constants.CODE_DIR_VAL).toString());
		runtimeConfig.setPidDir(Constants.PID_DIR_VAL);
		runtimeConfig.setEnvironment(jobConfig.getEnvironment());
		runtimeConfig.put(Constants.TOPOLOGY_YAML, jobConfig.getTopologyYaml());
		runtimeConfig.put(Constants.LOCAL_FAILOVER, jobConfig.isLocalFailover());
		return runtimeConfig;
	}
}
