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
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
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

	private String tags;
	private final Configuration flinkConfig;
	private UdpMetricsClient udpMetricsClient;
	private boolean isRunning = true;

	public FlinkVersionReporter(Configuration flinkConfig) {
		this.flinkConfig = flinkConfig;
		init();
	}

	public void init() {
		udpMetricsClient = new UdpMetricsClient(FLINK_VERSION_METRICS_PREFIX);
		String subVersion = this.flinkConfig.getString(ConfigConstants.FLINK_SUBVERSION_KEY, null);
		String flinkJobType = this.flinkConfig.getString(ConfigConstants.FLINK_JOB_TYPE_KEY, ConfigConstants.FLINK_JOB_TYPE_DEFAULT);
		boolean isInDockerMode = this.flinkConfig.getBoolean(YarnConfigOptions.DOCKER_ENABLED, false);
		String dockerImage = this.flinkConfig.getString(YarnConfigOptions.DOCKER_IMAGE);
		String dc = this.flinkConfig.getString(ConfigConstants.DC_KEY, null);
		String flinkApi = this.flinkConfig.getString(ConfigConstants.FLINK_JOB_API_KEY, "DataSet");
		String applicationType = this.flinkConfig.getString(YarnConfigOptions.APPLICATION_TYPE);
		String cluster = this.flinkConfig.getString(ConfigConstants.CLUSTER_NAME_KEY, null);
		String shuffleServiceType = this.flinkConfig.getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) ? ShuffleServiceOptions.CLOUD_SHUFFLE : ShuffleServiceOptions.NETTY_SHUFFLE;

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
		tags = tags + "|isInDockerMode=" + isInDockerMode;
		if (dockerImage != null && !dockerImage.isEmpty()) {
			tags = tags + "|dockerImage=" + dockerImage;
		}
		if (shuffleServiceType != null && !shuffleServiceType.isEmpty()) {
			tags = tags + "|shuffleServiceType=" + shuffleServiceType;
		}
		if (dc != null && !dc.isEmpty()) {
			tags = tags + "|region=" + dc;
		}
		if (flinkApi != null && !flinkApi.isEmpty()) {
			tags = tags + "|flinkApi=" + flinkApi;
		}
		if (applicationType != null && !applicationType.isEmpty()) {
			tags = tags + "|appType=" + formatTag(applicationType);
		}
		if (cluster != null && !cluster.isEmpty()) {
			tags = tags + "|cluster=" + cluster;
		}
	}

	String formatTag(String name) {
		return name.replaceAll(" ", "-");
	}

	@Override
	public void run() {
		while (isRunning) {
			try {
				LOG.debug("Emit flink version counter.");
				udpMetricsClient.emitStoreWithTag(FLINK_VERSION_METRICS_NAME, 1, tags);
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
