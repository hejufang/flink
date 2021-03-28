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
import org.apache.flink.monitor.utils.HttpUtil;
import org.apache.flink.monitor.utils.Utils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.StringUtils;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.monitor.utils.Utils.formatMetricsName;

/**
 * Provides methods for registring grafana dashboard.
 */
public class Dashboard {
	private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);

	private static final String CHECKPOINT_OVERVIEW_TEMPLATE = "checkpoint_overview_template.txt";
	private static final String CHECKPOINT_TIMER_TEMPLATE = "checkpoint_timer_template.txt";
	private static final String CHECKPOINT_TIMER_LAG_TARGET_TEMPLATE = "checkpoint_timer_lag_target_template.txt";
	private static final String CHECKPOINT_TIMER_RATE_TARGET_TEMPLATE = "checkpoint_timer_rate_target_template.txt";

	/** Templates for checkpoint operator metrics. */
	private static final String CHECKPOINT_OPERATOR_TEMPLATE = "checkpoint_operator_template.txt";
	private static final String CHECKPOINT_BARRIER_ALIGN_DURATION_TEMPLATE = "checkpoint_barrier_align_duration_target_template.txt";
	private static final String CHECKPOINT_CONTENTION_LOCK_DURATION_TEMPLATE = "checkpoint_contention_lock_duration_target_template.txt";

	private String clusterName;
	private String jobName;
	private String formatJobName;
	private String dataSource;
	private JobGraph jobGraph;
	private StreamGraph streamGraph;

	public Dashboard(String clusterName, String dataSource, StreamGraph streamGraph, JobGraph jobGraph) {
		this.clusterName = clusterName;
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
		String jobNameFromProperty = System.getProperty(ConfigConstants.JOB_NAME_KEY);
		if (StringUtils.isNullOrWhitespaceOnly(jobNameFromProperty)) {
			this.jobName = jobGraph.getName();
		} else {
			this.jobName = jobNameFromProperty;
		}
		this.formatJobName = formatMetricsName(jobGraph.getName());
		this.dataSource = dataSource;
	}

	private String renderString(String content, Map<String, String> map) {
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

	private String renderJobInfoRow() {
		String jobInfoTemplate = Template.JOB_INFO;
		Map<String, String> jobInfoValues = new HashMap<>();
		jobInfoValues.put("jobname", formatJobName);
		jobInfoValues.put("datasource", dataSource);
		String jobInfoRow = renderString(jobInfoTemplate, jobInfoValues);
		return jobInfoRow;
	}

	private String renderKafkaLagSizeRow(List<String> lags) {
		String lagSizeTargetTemplate = Template.KAFKA_LAG_SIZE_TARGET;
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
		String lagSizeTemplate = Template.KAFKA_LAG_SIZE;
		String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
		return lagSizeRow;
	}

	private String renderRocketMQLagSizeRow(JSONArray rocketMQConfArray) {
		String lagSizeTargetTemplate = Template.ROCKETMQ_LAG_SIZE_TARGET;
		List<String> lagsList = new ArrayList<>();
		for (Object o : rocketMQConfArray) {
			JSONObject json = (JSONObject) o;
			Map<String, String> lagSizeTargetValues = new HashMap<>();
			String[] clusterAndDcArray = Utils.parseClusterAndDc(json.get("cluster").toString());
			lagSizeTargetValues.put("metric_name", "rocketmq.consumer_group.depth");
			lagSizeTargetValues.put("cluster", clusterAndDcArray[0]);
			lagSizeTargetValues.put("topic", json.get("topic").toString());
			lagSizeTargetValues.put("dc", "*");
			lagSizeTargetValues.put("consumer_group", json.get("consumer_group").toString());
			lagsList.add(renderString(lagSizeTargetTemplate, lagSizeTargetValues));
		}
		String targets = String.join(",", lagsList);
		Map<String, String> lagSizeValues = new HashMap<>();
		lagSizeValues.put("targets", targets);
		lagSizeValues.put("datasource", dataSource);
		String lagSizeTemplate = Template.ROCKETMQ_LAG_SIZE;
		String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
		return lagSizeRow;
	}

	private String renderTmSlotRow() {
		String tmSlotTemplate = Template.TM_SLOT;
		Map<String, String> tmSlotValues = new HashMap<>();
		tmSlotValues.put("jobname", jobName);
		tmSlotValues.put("datasource", dataSource);
		String tmSlotRow = renderString(tmSlotTemplate, tmSlotValues);
		return tmSlotRow;
	}

	private String renderMemoryRow() {
		String memoryTemplate = Template.MEMORY;
		Map<String, String> memoryValues = new HashMap<>();
		memoryValues.put("jobname", jobName);
		memoryValues.put("datasource", dataSource);
		String memoryRow = renderString(memoryTemplate, memoryValues);
		return memoryRow;
	}

	private String renderGcRow() {
		String gcTemplate = Template.GC;
		Map<String, String> gcValues = new HashMap<>();
		gcValues.put("jobname", jobName);
		gcValues.put("datasource", dataSource);
		String gcRow = renderString(gcTemplate, gcValues);
		return gcRow;
	}

	private String renderCheckpointTimerRow(List<String> tasks) {
		String checkpointTemplate = null;
		String checkpointTimerLagTargetTemplate = null;
		String checkpointTimerRateTargetTemplate = null;
		try {
			checkpointTemplate = renderFromResource(CHECKPOINT_TIMER_TEMPLATE);
			checkpointTimerLagTargetTemplate = renderFromResource(CHECKPOINT_TIMER_LAG_TARGET_TEMPLATE);
			checkpointTimerRateTargetTemplate = renderFromResource(CHECKPOINT_TIMER_RATE_TARGET_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		List<String> targets1 = new ArrayList<>();
		List<String> targets2 = new ArrayList<>();
		for (String task : tasks) {
			checkpointValues.put("task", task);
			targets1.add(renderString(checkpointTimerLagTargetTemplate, checkpointValues));
			targets2.add(renderString(checkpointTimerRateTargetTemplate, checkpointValues));
		}
		checkpointValues.put("targets1", String.join(",", targets1));
		checkpointValues.put("targets2", String.join(",", targets2));
		return renderString(checkpointTemplate, checkpointValues);
	}

	private String renderCheckpointOperatorRow(List<String> tasks) {
		String checkpointOperatorTemplate = null;
		String checkpointBarrierAlignDurationTemplate = null;
		String checkpointContentionLockTemplate = null;
		try {
			checkpointOperatorTemplate = renderFromResource(CHECKPOINT_OPERATOR_TEMPLATE);
			checkpointBarrierAlignDurationTemplate = renderFromResource(CHECKPOINT_BARRIER_ALIGN_DURATION_TEMPLATE);
			checkpointContentionLockTemplate = renderFromResource(CHECKPOINT_CONTENTION_LOCK_DURATION_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		List<String> targets1 = new ArrayList<>();
		List<String> targets2 = new ArrayList<>();
		for (String task : tasks) {
			checkpointValues.put("task", task);
			targets1.add(renderString(checkpointContentionLockTemplate, checkpointValues));
			targets2.add(renderString(checkpointBarrierAlignDurationTemplate, checkpointValues));
		}
		checkpointValues.put("targets1", String.join(",", targets1));
		checkpointValues.put("targets2", String.join(",", targets2));
		return renderString(checkpointOperatorTemplate, checkpointValues);
	}

	private String renderCheckpointOverviewRow() {
		String checkpointTemplate = null;
		try {
			checkpointTemplate = renderFromResource(CHECKPOINT_OVERVIEW_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		return renderString(checkpointTemplate, checkpointValues);
	}

	private String renderSlowContainerRow() {
		String slowContainerTemplate = Template.SLOW_CONTAINER;
		Map<String, String> slowContainerValues = new HashMap<>();
		slowContainerValues.put("jobname", jobName);
		slowContainerValues.put("datasource", dataSource);
		String slowContainerRow = renderString(slowContainerTemplate, slowContainerValues);
		return slowContainerRow;
	}

	private String renderCompletedContainerRow() {
		String completedContainerTemplate = Template.COMPLETED_CONTAINER;
		Map<String, String> completedContainerValues = new HashMap<>();
		completedContainerValues.put("jobname", jobName);
		completedContainerValues.put("datasource", dataSource);
		return renderString(completedContainerTemplate, completedContainerValues);
	}

	private String renderPoolUsageRow(List<String> operators) {
		String poolUsageTargetTemplate = Template.POOL_USAGE_TARGET;
		List<String> poolUsageList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> poolUsageTargetValues = new HashMap<>();
			poolUsageTargetValues.put("operator", o);
			poolUsageTargetValues.put("jobname", formatJobName);
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

	private String renderRecordNumRow(List<String> operators) {
		String recordNumTargetTemplate = Template.RECORD_NUM_TARGET;
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> recordNumTargetValues = new HashMap<>();
			recordNumTargetValues.put("operator", o);
			recordNumTargetValues.put("jobname", formatJobName);
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

	private String renderLateRecordsDropped(List<String> operators) {
		String lateRecordsDroppedTargetTemplate = Template.LATE_RECORDS_DROPPED_TARGET;
		List<String> lateRecordsDroppedList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> lateRecordsDroppedTarget = new HashMap<>();
			lateRecordsDroppedTarget.put("operator", o);
			lateRecordsDroppedTarget.put("jobname", formatJobName);
			lateRecordsDroppedList.add(renderString(lateRecordsDroppedTargetTemplate, lateRecordsDroppedTarget));
		}
		String targets = String.join(",", lateRecordsDroppedList);
		Map<String, String> lateRecordsDroppedValues = new HashMap<>();
		lateRecordsDroppedValues.put("targets", targets);
		lateRecordsDroppedValues.put("datasource", dataSource);
		String lateRecordsDroppedTemplate = Template.LATE_RECORDS_DROPPED;
		return renderString(lateRecordsDroppedTemplate, lateRecordsDroppedValues);
	}

	private String renderDirtyRecordsSourceSkippedRow(List<String> sources) {
		String dirtyRecordsSkippedTargetTemplate = Template.DIRTY_RECORDS_SOURCE_SKIPPED_TARGET;
		List<String> dirtyRecordsSkippedList = new ArrayList<>();
		for (String source : sources) {
			Map<String, String> dirtyRecordsSkippedTarget = new HashMap<>();
			dirtyRecordsSkippedTarget.put("source", source);
			dirtyRecordsSkippedTarget.put("jobname", formatJobName);
			dirtyRecordsSkippedList.add(
				renderString(dirtyRecordsSkippedTargetTemplate, dirtyRecordsSkippedTarget));
		}

		String targets = String.join(",", dirtyRecordsSkippedList);
		Map<String, String> dirtyRecordsSkippedValues = new HashMap<>();
		dirtyRecordsSkippedValues.put("targets", targets);
		dirtyRecordsSkippedValues.put("datasource", dataSource);
		String dirtyRecordsSkippedTemplate = Template.DIRTY_RECORDS_SOURCE_SKIPPED;
		return renderString(dirtyRecordsSkippedTemplate, dirtyRecordsSkippedValues);
	}

	private String renderRecordsSinkSkippedRow(List<String> sinks) {
		String recordsSkippedTargetTemplate = Template.RECORDS_SINK_SKIPPED_TARGET;
		List<String> recordsWriteSkippedList = new ArrayList<>();
		for (String sink : sinks) {
			Map<String, String> recordsWriteSkippedTarget = new HashMap<>();
			recordsWriteSkippedTarget.put("sink", sink);
			recordsWriteSkippedTarget.put("jobname", formatJobName);
			recordsWriteSkippedList.add(
				renderString(recordsSkippedTargetTemplate, recordsWriteSkippedTarget));
		}

		String targets = String.join(",", recordsWriteSkippedList);
		Map<String, String> recordsWriteSkippedValues = new HashMap<>();
		recordsWriteSkippedValues.put("targets", targets);
		recordsWriteSkippedValues.put("datasource", dataSource);
		String recordsWriteSkippedTemplate = Template.RECORDS_SINK_SKIPPED;
		return renderString(recordsWriteSkippedTemplate, recordsWriteSkippedValues);
	}

	private String renderLookupRow(List<String> operators, String target, String metric) {
		List<String> metricRows = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> targetValues = new HashMap<>(2);
			targetValues.put("operator", o);
			targetValues.put("jobname", formatJobName);
			metricRows.add(renderString(target, targetValues));
		}
		String targets = String.join(",", metricRows);
		Map<String, String> values = new HashMap<>(2);
		values.put("targets", targets);
		values.put("datasource", dataSource);
		return renderString(metric, values);
	}

	private String renderOperatorLatencyRow(List<String> operators) {
		String operatorLatencyTarget = Template.OPERATOR_LATENCY_TARGET;
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> operatorLatencyTargetValues = new HashMap<>();
			operatorLatencyTargetValues.put("operator", o);
			operatorLatencyTargetValues.put("jobname", formatJobName);
			recordNumList.add(renderString(operatorLatencyTarget, operatorLatencyTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> operatorLatencyValues = new HashMap<>();
		operatorLatencyValues.put("targets", targets);
		operatorLatencyValues.put("datasource", dataSource);
		String operatorLatencyTemplate = Template.OPERATOR_LATENCY;
		String operatorLatencyRow = renderString(operatorLatencyTemplate, operatorLatencyValues);
		return operatorLatencyRow;
	}

	private String renderKafkaOffsetRow(List<String> sources) {
		String kafkaOffsetTargetTemplate = Template.KAFKA_OFFSET_TARGET;
		List<String> kafkaOffsetList = new ArrayList<>();
		for (String s : sources) {
			Map<String, String> kafkaOffsetTargetValues = new HashMap<>();
			kafkaOffsetTargetValues.put("kafka_source", s);
			kafkaOffsetTargetValues.put("jobname", formatJobName);
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

	private String renderKafkaLatencyRow(List<String> sources) {
		String kafkaLatencyTargetTemplate = Template.KAFKA_LATENCY_TARGET;
		List<String> kafkaLatencyList = new ArrayList<>();
		for (String s : sources) {
			Map<String, String> kafkaLatencyTargetValues = new HashMap<>();
			kafkaLatencyTargetValues.put("kafka_source", s);
			kafkaLatencyTargetValues.put("jobname", formatJobName);
			kafkaLatencyList.add(renderString(kafkaLatencyTargetTemplate, kafkaLatencyTargetValues));
		}
		String targets = String.join(",", kafkaLatencyList);
		Map<String, String> kafkaOffsetValues = new HashMap<>();
		kafkaOffsetValues.put("targets", targets);
		kafkaOffsetValues.put("datasource", dataSource);
		String kafkaLatencyTemplate = Template.KAFKA_LATENCY;
		String kafkaLatencyRow = renderString(kafkaLatencyTemplate, kafkaOffsetValues);
		return kafkaLatencyRow;
	}

	private String renderDashboard() {
		List<String> rows = new ArrayList<>();
		List <String> operators = Utils.getOperaters(streamGraph);
		List <String> operatorsButSources = Utils.getOperatersExceptSources(streamGraph);
		List <String> sources = Utils.getSources(streamGraph);
		List <String> sinks = Utils.getSinks(streamGraph);
		List <String> tasks = Utils.getTasks(jobGraph);
		List <String> lookupOperators = Utils.filterLookupOperators(operators);
		String kafkaServerUrl = System.getProperty(ConfigConstants.KAFKA_SERVER_URL_KEY,
			ConfigConstants.KAFKA_SERVER_URL_DEFAUL);
		JSONArray rocketmqConfigArray = Utils.getRocketMQConfigurations();
		List<String> kafkaMetricsList = Utils.getKafkaLagSizeMetrics(kafkaServerUrl);
		if (!kafkaMetricsList.isEmpty()) {
			rows.add(renderKafkaLagSizeRow(kafkaMetricsList));
			rows.add(renderKafkaOffsetRow(sources));
			rows.add(renderKafkaLatencyRow(sources));
		}
		if (!rocketmqConfigArray.isEmpty()) {
			rows.add(renderRocketMQLagSizeRow(rocketmqConfigArray));
		}
		rows.add(renderMemoryRow());
		rows.add(renderRecordNumRow(operators));
		rows.add(renderCheckpointTimerRow(tasks));
		rows.add(renderCheckpointOperatorRow(tasks));
		rows.add(renderCheckpointOverviewRow());
		rows.add(renderLateRecordsDropped(operators));
		rows.add(renderDirtyRecordsSourceSkippedRow(sources));
		rows.add(renderRecordsSinkSkippedRow(sinks));
		rows.add(renderLookupRow(
			lookupOperators,
			Template.LOOKUP_JOIN_HIT_RATE_TARGET,
			Template.LOOKUP_JOIN_HIT_RATE));
		rows.add(renderLookupRow(
			lookupOperators,
			Template.LOOKUP_JOIN_REQUEST_PER_SECOND_TARGET,
			Template.LOOKUP_JOIN_REQUEST_PER_SECOND));
		rows.add(renderLookupRow(
			lookupOperators,
			Template.LOOKUP_JOIN_FAILURE_PER_SECOND_TARGET,
			Template.LOOKUP_JOIN_FAILURE_PER_SECOND));
		rows.add(renderLookupRow(
			lookupOperators,
			Template.LOOKUP_JOIN_REQUEST_DELAY_P99_TARGET,
			Template.LOOKUP_JOIN_REQUEST_DELAY_P99));
		rows.add(renderOperatorLatencyRow(operatorsButSources));
		rows.add(renderPoolUsageRow(tasks));
		rows.add(renderGcRow());
		rows.add(renderJobInfoRow());
		rows.add(renderTmSlotRow());
		rows.add(renderSlowContainerRow());
		rows.add(renderCompletedContainerRow());

		String template = Template.TEMPLATE;
		String rowsStr = String.join(",", rows);
		Map<String, String> map = new HashMap<>();
		map.put("rows", rowsStr);
		map.put("jobname", jobName);
		map.put("cluster", clusterName);
		String dashboardJson = renderString(template, map);
		return dashboardJson;
	}

	private String renderFromResource(String resource) throws IOException {
		StringBuilder contentBuilder = new StringBuilder();
		try (InputStream stream = Dashboard.class.getClassLoader().getResourceAsStream("templates/" + resource)) {
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			String line;
			line = br.readLine();
			while (line != null){
				if (!line.startsWith("#")) {
					contentBuilder.append(line);
					contentBuilder.append("\n");
				}
				line = br.readLine();
			}
		}
		return contentBuilder.toString();
	}

	public boolean registerDashboard(String url, String token) throws IOException {
		String dashboardJson = renderDashboard();

		Map<String, String> headers = new HashMap<>();
		headers.put("Authorization", token);
		headers.put("Accept", "application/json");
		headers.put("Content-Type", "application/json");

		HttpUtil.HttpResponsePojo response = HttpUtil.sendPost(url, dashboardJson, headers);
		int statusCode = response.getStatusCode();
		boolean success = statusCode == 200;
		if (!success) {
			String resStr = response.getContent();
			LOG.warn("Failed to register dashboard, response: {}", resStr);
		}
		return success;
	}
}
