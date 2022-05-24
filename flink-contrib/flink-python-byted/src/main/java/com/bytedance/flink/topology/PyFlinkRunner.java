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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.StringUtils;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.parser.ConfigParser;
import com.bytedance.flink.pojo.JobConfig;
import com.bytedance.flink.pojo.RuntimeConfig;
import com.bytedance.flink.utils.EnvironmentInitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * PyFlink entry class.
 */
public class PyFlinkRunner {
	private static final Logger LOG = LoggerFactory.getLogger(PyFlinkRunner.class);
	private static final String CLUSTER_NAME_KEY = "clusterName";
	private static final String USER_YAML_FILE_NAME = "userYamlFileName";
	private static final String FLINK_CONFIG_DIR = "flinkConfigDir";
	private static final String USER_JAR_KEY = "userJar";
	private static final String RESOURCE_FILES_KEY = "resourceFiles";
	@Nullable
	private Configuration flinkConfig;
	private String userYamlFile;
	private String flinkConfigDir;
	private String clusterName;
	@Nullable
	private String userJar;
	@Nullable
	private String resourceFiles;

	public PyFlinkRunner(String userYamlFile, @Nullable String flinkConfigDir, String clusterName,
						@Nullable String userJar, @Nullable String resourceFiles) {
		this.userYamlFile = userYamlFile;
		this.flinkConfigDir = flinkConfigDir;
		this.clusterName = clusterName;
		this.userJar = userJar;
		this.resourceFiles = resourceFiles;
	}

	public static void main(String[] args) throws IOException {
		LOG.info("args = {}", Arrays.asList(args));
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String userYamlFile = parameterTool.get(USER_YAML_FILE_NAME);
		String flinkConfigDir = parameterTool.get(FLINK_CONFIG_DIR);
		String clusterName = parameterTool.get(CLUSTER_NAME_KEY);
		String userJar = parameterTool.get(USER_JAR_KEY);
		String resourceFiles = parameterTool.get(RESOURCE_FILES_KEY);
		LOG.info("userYamlFile = {}", userYamlFile);
		LOG.info("userJar = {}", userJar);
		LOG.info("flinkConfigDir = {}", flinkConfigDir);
		LOG.info("clusterName = {}", clusterName);
		LOG.info("resourceFiles = {}", resourceFiles);
		PyFlinkRunner pyFlinkRunner =
			new PyFlinkRunner(userYamlFile, flinkConfigDir, clusterName, userJar, resourceFiles);
		pyFlinkRunner.run();
	}

	private void run() throws IOException {
		ConfigParser configParser = new ConfigParser(userYamlFile);
		JobConfig jobConfig = configParser.parse();

		if (jobConfig.getRunMode() == Constants.RUN_MODE_STANDLONE) {
			if (StringUtils.isNullOrWhitespaceOnly(flinkConfigDir)) {
				throw new IllegalArgumentException("flink config dir could not be empty in standalone mode");
			}
			this.flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);
		}

		LOG.info("jobConfig = {}", jobConfig);
		FlinkTopologyBuilder flinkTopologyBuilder = new FlinkTopologyBuilder(jobConfig, flinkConfig);
		flinkTopologyBuilder.getEnv().getConfig()
			.setGlobalJobParameters(parseRuntimeConfig(jobConfig));
		flinkTopologyBuilder.run(jobConfig.getJobName());
	}

	private RuntimeConfig parseRuntimeConfig(JobConfig jobConfig) {
		RuntimeConfig runtimeConfig = new RuntimeConfig();
		int runMode = jobConfig.getRunMode();
		if (runMode == Constants.RUN_MODE_STANDLONE) {
			// Overwrite job name in local mode, to avoid working directory conflicts.
			String newJobName = jobConfig.getJobName() + "-" + System.currentTimeMillis();
			LOG.info("Overwrite job name in local mode, from {} to {}",
				jobConfig.getJobName(), newJobName);
			jobConfig.setJobName(newJobName);
		}
		runtimeConfig.setJobName(jobConfig.getJobName());
		runtimeConfig.setOwners(jobConfig.getOwners());
		runtimeConfig.setClusterName(clusterName);
		runtimeConfig.setIgnoreMismatchedMsg(jobConfig.isIgnoreMismatchedMsg());
		runtimeConfig.put(Constants.TOPOLOGY_NAME, jobConfig.getJobName());
		// Infer that we are in new mode, user can access through 'conf' in callee
		runtimeConfig.put(Constants.USE_NEW_MODE, true);
		List<String> resources = null;
		if (resourceFiles != null) {
			resources = EnvironmentInitUtils.getResourceFileListFromParameter(resourceFiles);
		} else {
			resources = EnvironmentInitUtils.getResourceFileList(userJar);
		}
		if (resources == null || resources.isEmpty()) {
			throw new RuntimeException("Resource list cannot be null or empty.");
		}
		runtimeConfig.setResourceFiles(resources);

		String tempDir;
		if (jobConfig.getRunMode() == 0) {
			// cluster mode
			tempDir = Constants.TEMP_DIR_VAL;
		} else {
			// local mode
			if (jobConfig.getCommonArgs().containsKey(CoreOptions.TMP_DIRS)) {
				tempDir = (String) jobConfig.getCommonArgs().get(CoreOptions.TMP_DIRS);
			} else {
				tempDir = flinkConfig.getString(CoreOptions.TMP_DIRS, "/tmp");
			}
		}
		runtimeConfig.setCodeDir(Paths.get(tempDir, Constants.CODE_DIR_VAL).toString());
		runtimeConfig.setLocalLogDir(Paths.get(tempDir, Constants.LOG).toString());
		runtimeConfig.setPidDir(Constants.PID_DIR_VAL);
		runtimeConfig.setEnvironment(jobConfig.getEnvironment());
		runtimeConfig.setTmpDir(tempDir);
		runtimeConfig.setRunMode(runMode);
		runtimeConfig.put(Constants.TOPOLOGY_YAML, jobConfig.getTopologyYaml());
		runtimeConfig.put(Constants.LOCAL_FAILOVER, jobConfig.isLocalFailover());
		return runtimeConfig;
	}
}
