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

package org.apache.flink.monitor.utils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.monitor.Dashboard;
import org.apache.flink.monitor.JobMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Common utils.
 */
public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 40;
	private static final int METRICS_TASK_NAME_MAX_LENGTH = 100;
	private static final Pattern CLUSTER_WITH_DC_PATTERN = Pattern.compile("(.*)\\.service\\.(\\w+)");

	public static String replaceSpecialCharacter(String name) {
		if (name != null) {
			return name.replaceAll("[^\\w.]", "_")
					.replaceAll("\\.+", ".")
					.replaceAll("_+", "_");
		} else {
			return name;
		}
	}

	public static List<String> getTasks(JobGraph jobGraph) {
		List<String> tasks = new ArrayList<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			String name = vertex.getName();
			name = formatTask(name);
			tasks.add(name);
		}
		return tasks;
	}

	public static List<String> getOperaters(StreamGraph streamGraph) {
		List<String> operators = new ArrayList<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			String name = node.getOperatorName();
			name = formatOperater(name);
			operators.add(name);
		}
		return operators;
	}

	public static List<String> filterLookupOperators(List<String> operators) {
		return operators.stream()
			.filter(s -> s.startsWith("LookupJoin"))
			.collect(Collectors.toList());
	}

	public static List<String> getOperatersExceptSources(StreamGraph streamGraph) {
		List<String> result = new ArrayList<>();
		List<String> operators = getOperaters(streamGraph);
		List<String> sources = getSources(streamGraph);
		for (String operator: operators) {
			if (!sources.contains(operator)) {
				result.add(operator);
			}
		}
		return result;
	}

	public static List<String> getSources(StreamGraph streamGraph) {
		List<String> sourceList = new ArrayList<>();
		for (int soureId : streamGraph.getSourceIDs()) {
			StreamNode sourceNode = streamGraph.getStreamNode(soureId);
			String sourceName = sourceNode.getOperatorName();
			sourceName = Utils.replaceSpecialCharacter(sourceName);
			sourceList.add(sourceName);
		}
		return sourceList;
	}

	public static List<String> getSinks(StreamGraph streamGraph) {
		List<String> sinkList = new ArrayList<>();
		for (int sinkId : streamGraph.getSinkIDs()) {
			StreamNode sinkNode = streamGraph.getStreamNode(sinkId);
			String sinkName = sinkNode.getOperatorName();
			sinkName = Utils.replaceSpecialCharacter(sinkName);
			sinkList.add(sinkName);
		}
		return sinkList;
	}

	public static String formatOperater(String name) {
		if (name != null && name.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
			LOG.warn("The operator name {} exceeded the {} characters length limit and was truncated.",
				name, METRICS_OPERATOR_NAME_MAX_LENGTH);
			name = name.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
		}
		return Utils.replaceSpecialCharacter(name);
	}

	public static String formatTask(String name) {
		if (name != null && name.length() > METRICS_TASK_NAME_MAX_LENGTH) {
			LOG.warn("The Task name {} exceeded the {} characters length limit and was truncated.",
					name, METRICS_TASK_NAME_MAX_LENGTH);
			name = name.substring(0, METRICS_TASK_NAME_MAX_LENGTH);
		}
		return Utils.replaceSpecialCharacter(name);
	}

	public static List<String> getKafkaLagSizeMetrics(String kafkaServerUrl) {
		List<String> kafkaMetricsList = new ArrayList<>();
		JSONArray jsonArray = getKafkaTopics();
		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject) object;
			String kafkaCluster = (String) jsonObject.get("cluster");
			String kafkaTopicPrefix = KafkaUtil.getKafkaTopicPrefix(kafkaCluster, kafkaServerUrl);
			String topic = (String) jsonObject.get("topic");
			String consumer = (String) jsonObject.get("consumer");
			String metric = String.format("%s.%s.%s.lag.size", kafkaTopicPrefix, topic, consumer);
			kafkaMetricsList.add(metric);
		}
		return kafkaMetricsList;
	}

	public static JSONArray getKafkaTopics() {
		String kafkaMetricsStr = System.getProperty("flink_kafka_metrics", "[]");
		JSONParser parser = new JSONParser();
		try {
			JSONArray jsonArray = (JSONArray) parser.parse(kafkaMetricsStr);
			return jsonArray;
		} catch (ParseException e) {
			LOG.error("Failed to render lag size metrics", e);
		}
		return new JSONArray();
	}

	/**
	 * Get RocketMQ configurations which we have saved in org.apache.flink.streaming.connectors.rocketmq.RocketMQSource.
	 * */
	public static JSONArray getRocketMQConfigurations() {
		String kafkaMetricsStr = System.getProperty("flink_rocketmq_metrics", "[]");
		JSONParser parser = new JSONParser();
		try {
			JSONArray jsonArray = (JSONArray) parser.parse(kafkaMetricsStr);
			return jsonArray;
		} catch (ParseException e) {
			LOG.error("Failed to parse RocketMQ configurations.", e);
		}
		return new JSONArray();
	}

	/**
	 * Parse cluster and dc from domain.
	 * Parse "{cluster}.service.{dc}" to [{cluster}, {dc}].
	 * */
	public static String[] parseClusterAndDc(String clusterWithDc) {
		String clusterName = clusterWithDc;
		String dc = "";
		if (clusterWithDc == null) {
			return new String[] {"", ""};
		}
		Matcher matcher = CLUSTER_WITH_DC_PATTERN.matcher(clusterWithDc);
		if (matcher.matches()) {
			clusterName = matcher.group(1);
			dc = matcher.group(2);
		}
		return new String[] {clusterName, dc};
	}

	/**
	 *  Parse input to write metric. Replace the charset which didn't
	 *  in [A-Za-z0-9_] to '_'. Because some characters metrics doesn't support.
	 *  Example "flink-test$job" to "flink_test_job".
	 */
	public static String formatMetricsName(String input) {
		String result = input.replaceAll("[^\\w.]", "_")
				.replaceAll("\\.+", ".")
				.replaceAll("_+", "_");
		return result;
	}

	public static JSONArray list2JSONArray(List list) {
		JSONArray jsonArray = new JSONArray();
		for (Object o : list) {
			jsonArray.add(o);
		}
		return jsonArray;
	}

	private static void saveMeta(StreamGraph streamGraph, JobGraph jobGraph) {
		String jobType = System.getProperty(ConfigConstants.FLINK_JOB_TYPE_KEY,
			ConfigConstants.FLINK_JOB_TYPE_DEFAULT);
		if ("pyFlink".equals(jobType)) {
			return;
		}
		try {
			JobMeta jobMeta = new JobMeta(streamGraph, jobGraph);
			if (jobMeta.saveToDB()) {
				LOG.info("Succeed in save job meta to database.");
			} else {
				LOG.warn("Failed to save job meta to database.");
			}
		} catch (Throwable e) {
			LOG.warn("Failed to save job meta to database.", e);
		}

	}

	private static void registerDashboard(StreamGraph streamGraph, JobGraph jobGraph) {
		String clusterName = System.getProperty(ConfigConstants.CLUSTER_NAME_KEY,
				ConfigConstants.CLUSTER_NAME_DEFAULT);
		LOG.info("clusterName = {}", clusterName);
		String dataSource = System.getProperty(ConfigConstants.DASHBOARD_DATA_SOURCE_KEY,
				ConfigConstants.DASHBOARD_DATA_SOURCE_DEFAULT);
		LOG.info("dataSource = {}", dataSource);
		String grafanaDomainUrl = System.getProperty(ConfigConstants.GRAFANA_DOMAIN_URL_KEY,
				ConfigConstants.GRAFANA_DOMAIN_URL_VALUE);
		String url = String.format(ConfigConstants.METRIC_REGISTER_URL_TEMPLATE, grafanaDomainUrl);
		String token = System.getProperty(ConfigConstants.REGISTER_DASHBOARD_TOKEN);
		if (url == null || token == null) {
			throw new IllegalArgumentException(
					"dashboard url or token not exists, please config by "
							+ ConfigConstants.GRAFANA_DOMAIN_URL_KEY + " and "
							+ ConfigConstants.REGISTER_DASHBOARD_TOKEN);
		}
		int maxRetryTimes = 5;
		int retryTimes = 0;
		boolean registerDashboardSuccessfully = false;
		while (retryTimes++ < maxRetryTimes && !registerDashboardSuccessfully) {
			try {
				Dashboard dashboard = new Dashboard(clusterName, dataSource, streamGraph, jobGraph);
				registerDashboardSuccessfully = dashboard.registerDashboard(url, token);
			} catch (Throwable e){
				registerDashboardSuccessfully = false;
				LOG.info("Failed to registering dashboard, retry", e);
			}
		}
		if (registerDashboardSuccessfully){
			LOG.info("Succeed in registering dashboard.");
		} else {
			LOG.warn("Failed to registering dashboard!");
		}
	}

	public static void saveMetaAndRegisterDashboard(StreamGraph streamGraph) {
		JobGraph jobGraph = streamGraph.getJobGraph();
		if (Boolean.parseBoolean(
				System.getProperty(
						ConfigConstants.SAVE_META_ENABLED, ConfigConstants.SAVE_META_ENABLED_DEFAULT))) {
			saveMeta(streamGraph, jobGraph);
		}
		if (Boolean.parseBoolean(
				System.getProperty(
						ConfigConstants.REGISTER_DASHBOARD_ENABLED, ConfigConstants.REGISTER_DASHBOARD_ENABLED_DEFAULT))) {
			registerDashboard(streamGraph, jobGraph);
		}
	}
}
