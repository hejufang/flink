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

package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import com.bytedance.metrics.UdpMetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.yarn.Utils.require;

/**
 * Flink version reporter which periodically reports Flink version messages.
 */
public class FlinkVersionReporter implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkVersionReporter.class);
	private static final String FLINK_VERSION_METRICS_PREFIX = "inf.flink";
	private static final String FLINK_VERSION_METRICS_NAME = "version";
	private static final String FLINK_VERSION_RESOURCE_CORE_NAME = "resource.core";
	private static final String FLINK_VERSION_RESOURCE_MEMORY_NAME = "resource.memory";

	private String tags;
	private Configuration flinkConfig;
	private UdpMetricsClient udpMetricsClient;
	private boolean isRunning = true;
	private double totalCores;
	private long totalMemory;

	public FlinkVersionReporter(Configuration flinkConfig) {
		this.flinkConfig = flinkConfig;
		init();
	}

	public void init() {
		udpMetricsClient = new UdpMetricsClient(FLINK_VERSION_METRICS_PREFIX);
		String subVersion = this.flinkConfig.getString(ConfigConstants.FLINK_SUBVERSION_KEY, null);
		String flinkJobType = this.flinkConfig.getString(ConfigConstants.FLINK_JOB_TYPE_KEY,
			ConfigConstants.FLINK_JOB_TYPE_DEFAULT);
		String isInDockerMode = this.flinkConfig.getString(YarnConfigKeys.IS_IN_DOCKER_MODE_KEY, null);
		String dockerImage = this.flinkConfig.getString(YarnConfigKeys.DOCKER_IMAGE_KEY, null);
		String dc = this.flinkConfig.getString(ConfigConstants.DC_KEY, null);
		String flinkApi = this.flinkConfig.getString(ConfigConstants.FLINK_JOB_API_KEY, "DataSet");
		String cluster = this.flinkConfig.getString(ConfigConstants.CLUSTER_NAME_KEY, null);

		// calculate resources
		double jmCore = ConfigurationUtils.getJobManagerVcore(flinkConfig);
		long jmMemory = ConfigurationUtils.getJobManagerHeapMemory(flinkConfig).getGibiBytes();
		long tmMemory = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getGibiBytes();
		double tmCore = flinkConfig.getDouble(YarnConfigOptions.VCORES);
		int tmCount = flinkConfig.getInteger(JobManagerOptions.TASK_MANAGER_COUNT);
		this.totalCores = jmCore + tmCore * tmCount;
		this.totalMemory = jmMemory + tmMemory * tmCount;

		EnvironmentInformation.RevisionInformation rev =
			EnvironmentInformation.getRevisionInformation();
		String commitId = rev.commitId;
		String commitDate = rev.commitDate;
		if (rev.commitDate != null) {
			commitDate = rev.commitDate.replace(" ", "_");
		}
		String appName = System.getenv().get(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		require(appName != null && !appName.isEmpty(), "AppName not set.");
		// we assume that the format of appName is {jobName}_{owner}
		String jobName = appName;
		String owner = null;
		int index = appName.lastIndexOf("_");
		if (index > 0) {
			jobName = appName.substring(0, index);
			if (index + 1 < appName.length()) {
				owner = appName.substring(index + 1);
			}
		}

		String version = EnvironmentInformation.getVersion();
		tags = String.format("version=%s|commitId=%s|commitDate=%s|jobName=%s",
			version, commitId, commitDate, jobName);
		if (flinkJobType != null && !flinkJobType.isEmpty()) {
			tags = tags + "|flinkJobType=" + flinkJobType;
		}
		if (subVersion != null && !subVersion.isEmpty()) {
			tags = tags + "|subVersion=" + subVersion;
		}
		if (owner != null && !owner.isEmpty()) {
			tags = tags + "|owner=" + owner;
		}
		if (isInDockerMode != null && !isInDockerMode.isEmpty()) {
			tags = tags + "|isInDockerMode=" + isInDockerMode;
		} else {
			tags = tags + "|isInDockerMode=" + false;
		}
		if (dockerImage != null && !dockerImage.isEmpty()) {
			tags = tags + "|dockerImage=" + dockerImage;
		}
		if (dc != null && !dc.isEmpty()) {
			tags = tags + "|region=" + dc;
		}
		if (flinkApi != null && !flinkApi.isEmpty()) {
			tags = tags + "|flinkApi=" + flinkApi;
		}
		if (cluster != null && !cluster.isEmpty()) {
			tags = tags + "|cluster=" + cluster;
		}
	}

	@Override
	public void run() {
		while (isRunning) {
			try {
				LOG.debug("Emit flink version counter.");

				// send basic information tagged by version
				udpMetricsClient.emitStoreWithTag(FLINK_VERSION_METRICS_NAME, 1, tags);

				// send resouce tagged by version
				udpMetricsClient.emitStoreWithTag(FLINK_VERSION_RESOURCE_CORE_NAME, this.totalCores, tags);
				udpMetricsClient.emitStoreWithTag(FLINK_VERSION_RESOURCE_MEMORY_NAME, this.totalMemory, tags);

				Thread.sleep(10 * 1000);
			} catch (IOException | InterruptedException e) {
				LOG.warn("Failed to emit flink version counter.", e);
			}
		}
	}

	public void stop() {
		isRunning = false;
	}
}
