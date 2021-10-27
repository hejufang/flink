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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.monitor.Dashboard;
import org.apache.flink.monitor.JobMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.MetricUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Common utils.
 */
public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static final Pattern CLUSTER_WITH_DC_PATTERN = Pattern.compile("(.*)\\.service\\.(\\w+)");
	private static final String RMQ_CONSUMER_DASHBOARD_URL_FORMAT = "%s/dashboard/db/rocketmq-health_diagnosisjian-kong?refresh=1m&orgId=1&from=now-12h&to=now&var-cluster=%s&var-cg=%s&var-dc=*&var-topic=%s&var-data_source=%s";

	public static List<String> getTasks(JobGraph jobGraph) {
		List<String> tasks = new ArrayList<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			String name = MetricUtils.formatTaskMetricName(vertex.getMetricName());
			tasks.add(name);
		}
		return tasks;
	}

	public static List<String> getSortedTasks(JobGraph jobGraph) {
		List<String> tasks = new ArrayList<>();
		for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			String name = MetricUtils.formatTaskMetricName(vertex.getMetricName());
			tasks.add(name);
		}
		return tasks;
	}

	public static List<String> getOperators(StreamGraph streamGraph) {
		List<String> operators = new ArrayList<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			String name = node.getOperatorMetricName();
			operators.add(name);
		}
		return operators;
	}

	public static List<String> getSortedOperators(StreamGraph streamGraph) {
		List<String> operators = new ArrayList<>();
		List<StreamNode> streamNodes = streamGraph.getStreamNodeSortedTopologicallyFromSources();
		for (StreamNode node : streamNodes) {
			String name = node.getOperatorMetricName();
			operators.add(name);
		}
		return operators;
	}

	public static List<String> filterLookupOperators(List<String> operators) {
		return operators.stream()
			.filter(s -> s.startsWith("LookupJoin"))
			.collect(Collectors.toList());
	}

	public static List<String> filterWindowOperators(List<String> operators) {
		return operators.stream()
			.filter(s -> s.contains("Window"))
			.collect(Collectors.toList());
	}

	public static List<String> getOperatorsExceptSources(StreamGraph streamGraph) {
		List<String> result = new ArrayList<>();
		List<String> operators = getOperators(streamGraph);
		List<String> sources = getSources(streamGraph);
		for (String operator: operators) {
			if (!sources.contains(operator)) {
				result.add(operator);
			}
		}
		return result;
	}

	public static List<String> getSortedOperatorsExceptSources(StreamGraph streamGraph) {
		List<String> result = new ArrayList<>();
		List<String> operators = getSortedOperators(streamGraph);
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
		for (int sourceId : streamGraph.getSourceIDs()) {
			StreamNode sourceNode = streamGraph.getStreamNode(sourceId);
			String sourceName = sourceNode.getOperatorMetricName();
			sourceList.add(sourceName);
		}
		return sourceList;
	}

	public static List<String> getSinks(StreamGraph streamGraph) {
		List<String> sinkList = new ArrayList<>();
		for (int sinkId : streamGraph.getSinkIDs()) {
			StreamNode sinkNode = streamGraph.getStreamNode(sinkId);
			String sinkName = sinkNode.getOperatorMetricName();
			sinkList.add(sinkName);
		}
		return sinkList;
	}

	public static List<String> getKafkaLagSizeMetrics(String kafkaServerUrl) {
		List<String> kafkaMetricsList = new ArrayList<>();
		JSONArray jsonArray = getKafkaTopics();
		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject) object;
			String kafkaCluster = (String) jsonObject.get("cluster");
			Tuple2<String, String> metricPrefix = KafkaUtil.getKafkaTopicPrefix(kafkaCluster, kafkaServerUrl);
			String kafkaTopicPrefix = metricPrefix.f0;
			String topic = (String) jsonObject.get("topic");
			String consumer = (String) jsonObject.get("consumer");
			String metric = String.format("%s.%s.%s.lag.size", kafkaTopicPrefix, topic, consumer);
			kafkaMetricsList.add(metric);
		}
		return kafkaMetricsList;
	}

	public static List<Tuple2<String, String>> getKafkaConsumerUrls(String kafkaServerUrl, String dataSource) {
		List<Tuple2<String, String>> kafkaConsumerUrls = new ArrayList<>();
		JSONArray jsonArray = getKafkaTopics();
		for (Object object : jsonArray) {
			JSONObject jsonObject = (JSONObject) object;
			String kafkaCluster = (String) jsonObject.get("cluster");
			Tuple2<String, String> metricPrefix = KafkaUtil.getKafkaTopicPrefix(kafkaCluster, kafkaServerUrl);
			String kafkaTopicPrefix = metricPrefix.f0;
			String kafkaBrokerPrefix = metricPrefix.f1;
			String topic = (String) jsonObject.get("topic");
			String consumer = (String) jsonObject.get("consumer");
			String url = KafkaUtil.getKafkaConsumerDashboardUrl(consumer, topic, kafkaTopicPrefix, kafkaBrokerPrefix, dataSource);
			kafkaConsumerUrls.add(Tuple2.of(topic, url));
		}
		return kafkaConsumerUrls;
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

	public static JSONArray list2JSONArray(List list) {
		JSONArray jsonArray = new JSONArray();
		for (Object o : list) {
			jsonArray.add(o);
		}
		return jsonArray;
	}

	private static void registerDashboard(StreamGraph streamGraph, JobGraph jobGraph, Configuration jobConfig) {
		String clusterName = jobConfig.getString(ConfigConstants.CLUSTER_NAME_KEY, ConfigConstants.CLUSTER_NAME_DEFAULT);
		LOG.info("clusterName = {}", clusterName);
		String dataSource = jobConfig.getString(ConfigConstants.DASHBOARD_DATA_SOURCE_KEY, ConfigConstants.DASHBOARD_DATA_SOURCE_DEFAULT);
		LOG.info("dataSource = {}", dataSource);
		String grafanaDomainUrl = jobConfig.getString(ConfigConstants.GRAFANA_DOMAIN_URL_KEY, ConfigConstants.GRAFANA_DOMAIN_URL_VALUE);
		String url = String.format(ConfigConstants.METRIC_REGISTER_URL_TEMPLATE, grafanaDomainUrl);
		String token = jobConfig.getString(ConfigConstants.REGISTER_DASHBOARD_TOKEN, "");
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

	public static Map<String, String> getMetaData(StreamGraph streamGraph, JobGraph jobGraph) {
		Map<String, String> metaData = new HashMap<>();
		try {
			JobMeta jobMeta = new JobMeta(streamGraph, jobGraph);
			metaData.put("dc", jobMeta.getDc());
			metaData.put("cluster", jobMeta.getCluster());
			metaData.put("job_type", jobMeta.getJobType());
			metaData.put("version", jobMeta.getVersion());
			metaData.put("job_name", jobMeta.getJobName());
			metaData.put("tasks", jobMeta.getTasks());
			metaData.put("operators", jobMeta.getOperators());
			metaData.put("operatorsButSources", jobMeta.getOperatorsButSources());
			metaData.put("sources", jobMeta.getSources());
			metaData.put("sinks", jobMeta.getSinks());
			metaData.put("topics", jobMeta.getTopics());
			metaData.put("modify_time", jobMeta.getModifyTime());
			metaData.put("application_name", jobMeta.getApplicationName());
			metaData.put("owner", jobMeta.getUser());
			metaData.put("data_source", jobMeta.getDataSource());
		} catch (Throwable e) {
			LOG.warn("Failed to save job meta to database.", e);
		}
		return metaData;
	}

	public static void registerDashboard(StreamGraph streamGraph, Configuration jobConfig) {
		JobGraph jobGraph = streamGraph.getJobGraph();
		boolean enableDashboard = jobConfig.getBoolean(ConfigConstants.REGISTER_DASHBOARD_ENABLED, false);
		if (enableDashboard) {
			registerDashboard(streamGraph, jobGraph, jobConfig);
		}
	}

	public static <T> List<List<T>> splitList(List<T> source, int subListSize) {
		List<List<T>> result = new ArrayList<>();
		int sourceSize = source.size();
		int size = (sourceSize % subListSize) == 0 ? (sourceSize / subListSize) : ((sourceSize / subListSize) + 1);
		for (int i = 0; i < size; i++) {
			List<T> subList = new ArrayList<T>();
			for (int j = i * subListSize; j < (i + 1) * subListSize; j++) {
				if (j < sourceSize) {
					subList.add(source.get(j));
				}
			}
			result.add(subList);
		}
		return result;
	}

	public static String getRmqDashboardUrl(String cluster, String consumerGroup, String topic, String dataSource) {
		String grafanaDomainUrl = System.getProperty(ConfigConstants.GRAFANA_DOMAIN_URL_KEY,
			ConfigConstants.GRAFANA_DOMAIN_URL_VALUE);
		return String.format(RMQ_CONSUMER_DASHBOARD_URL_FORMAT, grafanaDomainUrl, cluster, consumerGroup, topic, dataSource);
	}
}
