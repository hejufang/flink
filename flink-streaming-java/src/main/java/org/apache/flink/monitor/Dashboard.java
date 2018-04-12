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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.monitor.utils.HttpUtil;
import org.apache.flink.monitor.utils.KafkaUtil;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods for registring grafana dashboard.
 */
public class Dashboard {
	private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);
	private static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 40;
	private String clusterName;
	private String jobName;
	private String dataSource;
	private JobGraph jobGraph;
	private StreamGraph streamGraph;

	public Dashboard(String clusterName, StreamGraph streamGraph, JobGraph jobGraph) {
		this.clusterName = clusterName;
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
		this.jobName = jobGraph.getName();
		this.dataSource = loadClusterDatasource(clusterName);
	}

	public String loadClusterDatasource(String clusterName){
		String dataSource = "bytetsd";
		InputStream inputStream = Dashboard.class.getClassLoader().getResourceAsStream("cluster_datasource.properties");
		Properties prop = new Properties();
		try {
			prop.load(inputStream);
			for (Object key: prop.keySet()){
				String cluster = (String) key;
				if (cluster.equals(clusterName)){
					dataSource = prop.getProperty(cluster);
					return dataSource;
				}
			}
		} catch (IOException e) {
			LOG.error("Load cluster datasource configuration failed", e);
		}
		return dataSource;
	}

	public String renderString(String content, Map<String, String> map) {
		Set<Map.Entry<String, String>> sets = map.entrySet();
		try {
			for (Map.Entry<String, String> entry : sets) {
				String regex = "${" + entry.getKey() + "}";
				content = content.replace(regex, entry.getValue());
			}
		} catch (Exception e) {
			LOG.error("Failed to render string", e);
		}
		return content;
	}

	public String renderJobInfoRow() {
		String jobInfoTemplate = Template.JOB_INFO;
		Map<String, String> jobInfoValues = new HashMap<>();
		jobInfoValues.put("jobname", jobName);
		jobInfoValues.put("datasource", dataSource);
		String jobInfoRow = renderString(jobInfoTemplate, jobInfoValues);
		return jobInfoRow;
	}

	public String renderLagSizeRow(List<String> lags) {
		String lagSizeTargetTemplate = Template.LAG_SIZE_TARGET;
		List<String> lagsList = new ArrayList<>();
		for (String l : lags) {
			Map<String, String> lagSizeTargetValues = new HashMap<>();
			lagSizeTargetValues.put("lag", l);
			lagsList.add(renderString(lagSizeTargetTemplate, lagSizeTargetValues));
		}
		String targets = String.join(",", lagsList);
		Map<String, String> lagSizeValues = new HashMap<>();
		lagSizeValues.put("targets", targets);
		lagSizeValues.put("datasource", dataSource);
		String lagSizeTemplate = Template.LAG_SIZE;
		String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
		return lagSizeRow;
	}

	public String renderTmSlotRow() {
		String tmSlotTemplate = Template.TM_SLOT;
		Map<String, String> tmSlotValues = new HashMap<>();
		tmSlotValues.put("jobname", jobName);
		tmSlotValues.put("datasource", dataSource);
		String tmSlotRow = renderString(tmSlotTemplate, tmSlotValues);
		return tmSlotRow;
	}

	public String renderMemoryRow() {
		String memoryTemplate = Template.MEMORY;
		Map<String, String> memoryValues = new HashMap<>();
		memoryValues.put("jobname", jobName);
		memoryValues.put("datasource", dataSource);
		String memoryRow = renderString(memoryTemplate, memoryValues);
		return memoryRow;
	}

	public String renderGcRow() {
		String gcTemplate = Template.GC;
		Map<String, String> gcValues = new HashMap<>();
		gcValues.put("jobname", jobName);
		gcValues.put("datasource", dataSource);
		String gcRow = renderString(gcTemplate, gcValues);
		return gcRow;
	}

	public String renderQueueLengthRow(List<String> operators) {
		String queueLengthTargetTemplate = Template.QUEUE_LENGTH_TARGET;
		List<String> queueLengthList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> queueLengthTargetValues = new HashMap<>();
			queueLengthTargetValues.put("operator", o);
			queueLengthTargetValues.put("jobname", jobName);
			queueLengthList.add(renderString(queueLengthTargetTemplate, queueLengthTargetValues));
		}
		String targets = String.join(",", queueLengthList);
		Map<String, String> queueLengthValues = new HashMap<>();
		queueLengthValues.put("targets", targets);
		queueLengthValues.put("datasource", dataSource);
		String queueLengthTemplate = Template.QUEUE_LENGTH;
		String queueLengthRow = renderString(queueLengthTemplate, queueLengthValues);
		return queueLengthRow;
	}

	public String renderPoolUsageRow(List<String> operators) {
		String poolUsageTargetTemplate = Template.POOL_USAGE_TARGET;
		List<String> poolUsageList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> poolUsageTargetValues = new HashMap<>();
			poolUsageTargetValues.put("operator", o);
			poolUsageTargetValues.put("jobname", jobName);
			poolUsageList.add(renderString(poolUsageTargetTemplate, poolUsageTargetValues));
		}
		String targets = String.join(",", poolUsageList);
		Map<String, String> poolUsageValues = new HashMap<>();
		poolUsageValues.put("targets", targets);
		poolUsageValues.put("datasource", dataSource);
		String poolUsageTemplate = Template.POOL_USAGE;
		String poolUsageRow = renderString(poolUsageTemplate, poolUsageValues);
		return poolUsageRow;
	}

	public String renderRecordNumRow(List<String> operators) {
		String recordNumTargetTemplate = Template.RECORD_NUM_TARGET;
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> recordNumTargetValues = new HashMap<>();
			recordNumTargetValues.put("operator", o);
			recordNumTargetValues.put("jobname", jobName);
			recordNumList.add(renderString(recordNumTargetTemplate, recordNumTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> recordNumValues = new HashMap<>();
		recordNumValues.put("targets", targets);
		recordNumValues.put("datasource", dataSource);
		String poolUsageTemplate = Template.RECORD_NUM;
		String poolUsageRow = renderString(poolUsageTemplate, recordNumValues);
		return poolUsageRow;
	}

	public String renderKafkaOffsetRow(List<String> sources) {
		String kafkaOffsetTargetTemplate = Template.KAFKA_OFFSET_TARGET;
		List<String> kafkaOffsetList = new ArrayList<>();
		for (String s : sources) {
			Map<String, String> kafkaOffsetTargetValues = new HashMap<>();
			kafkaOffsetTargetValues.put("kafka_source", s);
			kafkaOffsetTargetValues.put("jobname", jobName);
			kafkaOffsetList.add(renderString(kafkaOffsetTargetTemplate, kafkaOffsetTargetValues));
		}
		String targets = String.join(",", kafkaOffsetList);
		Map<String, String> kafkaOffsetValues = new HashMap<>();
		kafkaOffsetValues.put("targets", targets);
		kafkaOffsetValues.put("datasource", dataSource);
		String kafkaOffsetTemplate = Template.KAFKA_OFFSET;
		String kafkaOffsetRow = renderString(kafkaOffsetTemplate, kafkaOffsetValues);
		return kafkaOffsetRow;
	}

	public List<String> getLagSizeMetrics() {
		List<String> metricsList = new ArrayList<>();
		String kafkaMetricsStr = System.getProperty("flink_kafka_metrics", "[]");
		JSONParser parser = new JSONParser();
		try {
			JSONArray jsonArray = (JSONArray) parser.parse(kafkaMetricsStr);
			for (Object object : jsonArray) {
				JSONObject jsonObject = (JSONObject) object;
				String kafkaCluster = (String) jsonObject.get("cluster");
				String kafkaTopicPrefix = KafkaUtil.getKafkaTopicPrefix(kafkaCluster);
				String topic = (String) jsonObject.get("topic");
				String group = (String) jsonObject.get("group");
				String metric = String.format("%s.%s.%s.lag.size", kafkaTopicPrefix, topic, group);
				metricsList.add(metric);
			}
		} catch (ParseException e) {
			LOG.error("Failed to render lag size metrics", e);
		}
		return metricsList;
	}

	public List<String> getTasks() {
		List<String> tasks = new ArrayList<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			String name = vertex.getName();
			name = formatOperater(name);
			tasks.add(name);
		}
		return tasks;
	}

	public List<String> getOperaters() {
		List<String> operators = new ArrayList<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			String name = node.getOperatorName();
			name = formatOperater(name);
			operators.add(name);
		}
		return operators;
	}

	public List<String> getSources() {
		List<String> sourceList = new ArrayList<>();
		for (int soureId : streamGraph.getSourceIDs()){
			StreamNode sourceNode = streamGraph.getStreamNode(soureId);
			String sourceName = sourceNode.getOperatorName();
			sourceName = replaceSpecialCharacter(sourceName);
			sourceList.add(sourceName);
		}
		return sourceList;
	}

	public String formatOperater(String name){
		if (name != null && name.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
			LOG.warn("The operator name {} exceeded the {} characters length limit and was truncated.",
					name, METRICS_OPERATOR_NAME_MAX_LENGTH);
			name = name.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
		}
		return replaceSpecialCharacter(name);
	}

	public String replaceSpecialCharacter(String name){
		String result = name.replaceAll("\\s*", "")
				.replace("->", "_")
				.replace("{", "_")
				.replace("}", "_")
				.replace(":", "_")
				.replace("..", ".");
		return result;
	}

	public String renderDashboard() {
		List<String> rows = new ArrayList<>();
		rows.add(renderJobInfoRow());
		rows.add(renderLagSizeRow(getLagSizeMetrics()));
		rows.add(renderTmSlotRow());
		rows.add(renderMemoryRow());
		rows.add(renderGcRow());
		rows.add(renderQueueLengthRow(getTasks()));
		rows.add(renderPoolUsageRow(getTasks()));
		rows.add(renderRecordNumRow(getOperaters()));
		rows.add(renderKafkaOffsetRow(getSources()));

		String template = Template.TEMPLATE;
		String rowsStr = String.join(",", rows);
		Map<String, String> map = new HashMap<>();
		map.put("rows", rowsStr);
		map.put("jobname", jobName);
		map.put("cluster", clusterName);
		String dashboardJson = renderString(template, map);
		return dashboardJson;
	}

	public boolean registerDashboard() throws IOException {
		String url = "https://grafana.byted.org/api/dashboards/db";
		String dashboardJson = renderDashboard();

		Map<String, String> headers = new HashMap();
		headers.put("Authorization",
				"Bearer eyJrIjoiYjZMS0hPSXZybVpOOWJMS3pLRHkwaXRoWWI2RW1UT2oiLCJuIjoianN0b3JtIiwiaWQiOjF9");
		headers.put("Accept", "application/json");
		headers.put("Content-Type", "application/json");

		CloseableHttpResponse response = HttpUtil.sendPost(url, dashboardJson, headers);
		int statusCode = response.getStatusLine().getStatusCode();
		boolean success = statusCode == 200;
		return success;
	}
}
