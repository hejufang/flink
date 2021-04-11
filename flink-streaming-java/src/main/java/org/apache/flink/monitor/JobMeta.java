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

package org.apache.flink.monitor;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.monitor.utils.Utils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class for saving job meta to database.
 */
public class JobMeta {
	private final String dc;
	private final String cluster;
	private final String applicationName;
	private final String jobName;
	private final String tasks;
	private final String operators;
	private final String operatorsButSources;
	private final String sources;
	private final String sinks;
	private final String user;
	private final String jobType;
	private final String version = "1.11";
	private final String modifyTime;
	private final String topics;
	private final String dataSource;

	public JobMeta(StreamGraph streamGraph, JobGraph jobGraph) {
		dc = System.getProperty(ConfigConstants.DC_KEY,
			ConfigConstants.DC_DEFAULT);
		cluster = System.getProperty(ConfigConstants.CLUSTER_NAME_KEY,
			ConfigConstants.CLUSTER_NAME_DEFAULT);
		applicationName = System.getProperty(ConfigConstants.APPLICATION_NAME_KEY,
			ConfigConstants.APPLICATION_NAME_DEFAULT);
		jobName = streamGraph.getJobName();
		tasks = Utils.list2JSONArray(Utils.getTasks(jobGraph)).toJSONString();
		operators = Utils.list2JSONArray(Utils.getOperators(streamGraph)).toJSONString();
		user = System.getProperty(ConfigConstants.FLINK_OWNER_KEY,
			ConfigConstants.FLINK_OWNER_DEFAULT);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		modifyTime = df.format(new Date());
		topics = Utils.getKafkaTopics().toJSONString();
		dataSource = System.getProperty(ConfigConstants.DASHBOARD_DATA_SOURCE_KEY,
				ConfigConstants.DASHBOARD_DATA_SOURCE_DEFAULT);
		operatorsButSources = Utils.list2JSONArray(Utils.getOperatorsExceptSources(streamGraph)).toJSONString();
		sources = Utils.list2JSONArray(Utils.getSources(streamGraph)).toJSONString();
		sinks = Utils.list2JSONArray(Utils.getSinks(streamGraph)).toJSONString();
		jobType = System.getProperty(ConfigConstants.FLINK_JOB_TYPE_KEY,
			ConfigConstants.FLINK_JOB_TYPE_DEFAULT);
	}

	public String getDc() {
		return dc;
	}

	public String getCluster() {
		return cluster;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public String getJobName() {
		return jobName;
	}

	public String getTasks() {
		return tasks;
	}

	public String getOperators() {
		return operators;
	}

	public String getOperatorsButSources() {
		return operatorsButSources;
	}

	public String getSources() {
		return sources;
	}

	public String getSinks() {
		return sinks;
	}

	public String getUser() {
		return user;
	}

	public String getJobType() {
		return jobType;
	}

	public String getVersion() {
		return version;
	}

	public String getModifyTime() {
		return modifyTime;
	}

	public String getTopics() {
		return topics;
	}

	public String getDataSource() {
		return dataSource;
	}
}
