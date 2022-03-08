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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.monitor.utils.HttpUtil;
import org.apache.flink.monitor.utils.Utils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.MetricUtils;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.commons.collections.CollectionUtils;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides methods for registering grafana dashboard.
 */
public class Dashboard {
	private static final Logger LOG = LoggerFactory.getLogger(Dashboard.class);

	private String clusterName;
	private String jobName;
	private String formatJobName;
	private String dataSource;
	private JobGraph jobGraph;
	private StreamGraph streamGraph;
	// not split for now.
	private static final int targetLimit = Integer.MAX_VALUE;

	public Dashboard(String clusterName, String dataSource, StreamGraph streamGraph, JobGraph jobGraph) {
		this.clusterName = clusterName;
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
		this.jobName = jobGraph.getName();
		this.formatJobName = MetricUtils.formatJobMetricNameOrigin(jobGraph.getName());
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
		String jobInfoTemplate;

		try {
			jobInfoTemplate = renderFromResource(DashboardTemplate.JOB_INFO_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> jobInfoValues = new HashMap<>();
		jobInfoValues.put("jobname", formatJobName);
		jobInfoValues.put("datasource", dataSource);
		String jobInfoRow = renderString(jobInfoTemplate, jobInfoValues);
		return jobInfoRow;
	}

	private String renderOverviewRow() {
		String template;

		try {
			template = renderFromResource(DashboardTemplate.OVERVIEW_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> values = new HashMap<>();
		values.put("jobname", jobName);
		values.put("datasource", dataSource);
		String row = renderString(template, values);
		return row;
	}

	private String renderRowTemplate(String rowTemplateName, List<String> panels) {

		return renderRowTemplate(rowTemplateName, panels, true);
	}

	private String renderRowTemplate(String rowTemplateName, List<String> panels, boolean collapse) {

		try {
			String rowTemplate = renderFromResource(rowTemplateName);
			Map<String, String> panelValues = new HashMap<>();
			panelValues.put("collapsed", String.valueOf(collapse));
			if (collapse) {
				if (CollectionUtils.isNotEmpty(panels)) {
					panelValues.put("panels", String.join(",", panels));
				} else {
					panelValues.put("panels", "");
				}
				return renderString(rowTemplate, panelValues);
			} else {
				panelValues.put("panels", "");
				String row = renderString(rowTemplate, panelValues);
				return row + "," + String.join(",", panels);
			}
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
	}

	private String renderKafkaLagSizeRow(List<String> lags, List<Tuple2<String, String>> kafkaConsumerUrls) {
		String lagSizeTargetTemplate;
		String lagSizeTemplate;
		String linkTemplate;

		try {
			lagSizeTargetTemplate = renderFromResource(DashboardTemplate.KAFKA_LAG_SIZE_TARGET_TEMPLATE);
			lagSizeTemplate = renderFromResource(DashboardTemplate.KAFKA_LAG_SIZE_TEMPLATE);
			linkTemplate = renderFromResource(DashboardTemplate.KAFKA_LAG_LINK_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> lagsList = new ArrayList<>();
		for (String l : lags) {
			Map<String, String> lagSizeTargetValues = new HashMap<>();
			lagSizeTargetValues.put("lag", l);
			lagsList.add(renderString(lagSizeTargetTemplate, lagSizeTargetValues));
		}
		List<String> linksList = new ArrayList<>();
		Map<String, String> linkValues = new HashMap<>();
		for (Tuple2<String, String> tuple : kafkaConsumerUrls) {
			linkValues.put("topic", tuple.f0);
			linkValues.put("url", tuple.f1);
			linksList.add(renderString(linkTemplate, linkValues));
		}
		String targets = String.join(",", lagsList);
		String links = String.join(",", linksList);
		Map<String, String> lagSizeValues = new HashMap<>();
		lagSizeValues.put("targets", targets);
		lagSizeValues.put("links", links);
		lagSizeValues.put("datasource", dataSource);
		String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
		return lagSizeRow;
	}

	private String renderRocketMQLagSizeRow(JSONArray rocketMQConfArray) {
		String lagSizeTargetTemplate;
		String lagSizeTemplate;
		String linkTemplate;

		try {
			lagSizeTargetTemplate = renderFromResource(DashboardTemplate.ROCKETMQ_LAG_SIZE_TARGET_TEMPLATE);
			lagSizeTemplate = renderFromResource(DashboardTemplate.ROCKETMQ_LAG_SIZE_TEMPLATE);
			linkTemplate = renderFromResource(DashboardTemplate.ROCKETMQ_LAG_LINK_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> lagsList = new ArrayList<>();
		List<String> linksList = new ArrayList<>();
		Map<String, String> linkValues = new HashMap<>();
		for (Object o : rocketMQConfArray) {
			JSONObject json = (JSONObject) o;
			Map<String, String> lagSizeTargetValues = new HashMap<>();
			String[] clusterAndDcArray = Utils.parseClusterAndDc(json.get("cluster").toString());
			lagSizeTargetValues.put("metric_name", "rocketmq.consumer_group.depth");
			String cluster = clusterAndDcArray[0];
			lagSizeTargetValues.put("cluster", cluster);
			String topic = json.get("topic").toString();
			lagSizeTargetValues.put("topic", topic);
			lagSizeTargetValues.put("dc", "*");
			String consumerGroup = json.get("consumer_group").toString();
			lagSizeTargetValues.put("consumer_group", consumerGroup);
			String url = Utils.getRmqDashboardUrl(cluster, consumerGroup, topic, dataSource);
			linkValues.put("topic", topic);
			linkValues.put("url", url);
			linksList.add(renderString(linkTemplate, linkValues));
			lagsList.add(renderString(lagSizeTargetTemplate, lagSizeTargetValues));
		}
		String targets = String.join(",", lagsList);
		Map<String, String> lagSizeValues = new HashMap<>();
		lagSizeValues.put("targets", targets);
		lagSizeValues.put("datasource", dataSource);
		lagSizeValues.put("links", String.join(",", linksList));
		String lagSizeRow = renderString(lagSizeTemplate, lagSizeValues);
		return lagSizeRow;
	}

	private String renderTmSlotRow() {
		String tmSlotTemplate;

		try {
			tmSlotTemplate = renderFromResource(DashboardTemplate.TASK_MANAGER_SLOT_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> tmSlotValues = new HashMap<>();
		tmSlotValues.put("jobname", jobName);
		tmSlotValues.put("datasource", dataSource);
		String tmSlotRow = renderString(tmSlotTemplate, tmSlotValues);
		return tmSlotRow;
	}

	private String renderJMMemoryRow() {
		String memoryTemplate;

		try {
			memoryTemplate = renderFromResource(DashboardTemplate.JM_MEMORY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> memoryValues = new HashMap<>();
		memoryValues.put("jobname", jobName);
		memoryValues.put("datasource", dataSource);

		String memoryRow = renderString(memoryTemplate, memoryValues);
		return memoryRow;
	}

	private String renderTMMemoryRow() {
		String memoryTemplate;

		try {
			memoryTemplate = renderFromResource(DashboardTemplate.TM_MEMORY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> memoryValues = new HashMap<>();
		memoryValues.put("jobname", jobName);
		memoryValues.put("datasource", dataSource);
		String memoryRow = renderString(memoryTemplate, memoryValues);
		return memoryRow;
	}

	private String renderJMThreadRow() {
		String threadTemplate;

		try {
			threadTemplate = renderFromResource(DashboardTemplate.JM_THREAD_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> memoryValues = new HashMap<>();
		memoryValues.put("jobname", jobName);
		memoryValues.put("datasource", dataSource);

		String memoryRow = renderString(threadTemplate, memoryValues);
		return memoryRow;
	}

	private String renderTMThreadRow() {
		String threadTemplate;

		try {
			threadTemplate = renderFromResource(DashboardTemplate.TM_THREAD_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> values = new HashMap<>();
		values.put("jobname", jobName);
		values.put("datasource", dataSource);
		String row = renderString(threadTemplate, values);
		return row;
	}

	// metric not stable， disable for now.
	private String renderJMCPURow() {
		String cpuTemplate;

		try {
			cpuTemplate = renderFromResource(DashboardTemplate.JM_CORES_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> values = new HashMap<>();
		values.put("jobname", jobName);
		values.put("datasource", dataSource);

		String row = renderString(cpuTemplate, values);
		return row;
	}

	private String renderTMCPURow() {
		String cpuTemplate;

		try {
			cpuTemplate = renderFromResource(DashboardTemplate.TM_CORES_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> values = new HashMap<>();
		values.put("jobname", jobName);
		values.put("datasource", dataSource);
		String row = renderString(cpuTemplate, values);
		return row;
	}

	private String renderJMGcRow() {
		String gcTemplate;

		try {
			gcTemplate = renderFromResource(DashboardTemplate.JM_GC_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> gcValues = new HashMap<>();
		gcValues.put("jobname", jobName);
		gcValues.put("datasource", dataSource);
		String gcRow = renderString(gcTemplate, gcValues);
		return gcRow;
	}

	private String renderTMGcRow() {
		String gcTemplate;

		try {
			gcTemplate = renderFromResource(DashboardTemplate.TM_GC_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
			checkpointTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_TIMER_TEMPLATE);
			checkpointTimerLagTargetTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_TIMER_LAG_TARGET_TEMPLATE);
			checkpointTimerRateTargetTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_TIMER_RATE_TARGET_TEMPLATE);
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

	private String renderCheckpointOperatorRow(List<String> tasks, List<String> operators) {
		String checkpointOperatorTemplate = null;
		String checkpointBarrierAlignDurationTemplate = null;
		String checkpointContentionLockTemplate = null;
		String checkpointSyncTemplate = null;
		String checkpointAsyncTemplate = null;
		try {
			checkpointOperatorTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_OPERATOR_TEMPLATE);
			checkpointBarrierAlignDurationTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_BARRIER_ALIGN_DURATION_TEMPLATE);
			checkpointContentionLockTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_CONTENTION_LOCK_DURATION_TEMPLATE);
			checkpointSyncTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_SYNC_DURATION_TEMPLATE);
			checkpointAsyncTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_ASYNC_DURATION_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		List<String> targets1 = new ArrayList<>();
		List<String> targets2 = new ArrayList<>();
		List<String> targets3 = new ArrayList<>();
		List<String> targets4 = new ArrayList<>();
		for (String task : tasks) {
			checkpointValues.put("task", task);
			targets1.add(renderString(checkpointContentionLockTemplate, checkpointValues));
			targets2.add(renderString(checkpointBarrierAlignDurationTemplate, checkpointValues));
		}
		for (String operator : operators) {
			checkpointValues.put("operator", operator);
			targets3.add(renderString(checkpointSyncTemplate, checkpointValues));
			targets4.add(renderString(checkpointAsyncTemplate, checkpointValues));
		}
		checkpointValues.put("targets1", String.join(",", targets1));
		checkpointValues.put("targets2", String.join(",", targets2));
		checkpointValues.put("targets3", String.join(",", targets3));
		checkpointValues.put("targets4", String.join(",", targets4));
		return renderString(checkpointOperatorTemplate, checkpointValues);
	}

	private String renderCheckpointOverviewRow() {
		String checkpointTemplate = null;
		try {
			checkpointTemplate = renderFromResource(DashboardTemplate.CHECKPOINT_OVERVIEW_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		return renderString(checkpointTemplate, checkpointValues);
	}

	private String renderOperatorStatePerformanceRow(List<String> operators) {
		String operatorStateTemplate = null;
		String keyValueSizeTemplate = null;
		String stateOpLatencyTemplate = null;
		String stateOpRateTemplate = null;
		String stateMemorySizeTemplate = null;
		String stateTotalSizeTemplate = null;
		String stateCompactionFlushTemplate = null;
		String stateWriteStallTemplate = null;
		String stateCacheHitRate = null;
		String stateCacheMemoryUsage = null;
		try {
			operatorStateTemplate = renderFromResource(DashboardTemplate.OPERATOR_STATE_PERFORMANCE_TEMPLATE);
			keyValueSizeTemplate = renderFromResource(DashboardTemplate.STATE_KEY_VALUE_SIZE_TEMPLATE);
			stateOpLatencyTemplate = renderFromResource(DashboardTemplate.STATE_OP_LATENCY_TEMPLATE);
			stateOpRateTemplate = renderFromResource(DashboardTemplate.STATE_OP_RATE_TEMPLATE);
			stateMemorySizeTemplate = renderFromResource(DashboardTemplate.STATE_MEMORY_SIZE_TEMPLATE);
			stateTotalSizeTemplate = renderFromResource(DashboardTemplate.STATE_TOTAL_SIZE_TEMPLATE);
			stateCompactionFlushTemplate = renderFromResource(DashboardTemplate.STATE_COMPACTION_FLUSH_TEMPLATE);
			stateWriteStallTemplate = renderFromResource(DashboardTemplate.STATE_WRITE_STALL_TEMPLATE);
			stateCacheHitRate = renderFromResource(DashboardTemplate.STATE_CACHE_HIT_RATE);
			stateCacheMemoryUsage = renderFromResource(DashboardTemplate.STATE_CACHE_MEMORY_USAGE);
		} catch (IOException e) {
			LOG.error("Fail to render checkpoint metrics.", e);
			return "";
		}

		Map<String, String> checkpointValues = new HashMap<>();
		checkpointValues.put("jobname", formatJobName);
		checkpointValues.put("datasource", dataSource);
		List<String> targets1 = new ArrayList<>();
		List<String> targets2 = new ArrayList<>();
		List<String> targets3 = new ArrayList<>();
		List<String> targets4 = new ArrayList<>();
		List<String> targets5 = new ArrayList<>();
		List<String> targets6 = new ArrayList<>();
		List<String> targets7 = new ArrayList<>();
		List<String> targets8 = new ArrayList<>();
		List<String> targets9 = new ArrayList<>();
		List<String> targets10 = new ArrayList<>();

		for (String operator : operators) {
			checkpointValues.put("operator", operator);
			checkpointValues.put("type", "Key");
			targets1.add(renderString(keyValueSizeTemplate, checkpointValues));
			checkpointValues.put("type", "Value");
			targets2.add(renderString(keyValueSizeTemplate, checkpointValues));
			targets3.add(renderString(stateOpLatencyTemplate, checkpointValues));
			targets4.add(renderString(stateOpRateTemplate, checkpointValues));
			targets5.add(renderString(stateMemorySizeTemplate, checkpointValues));
			targets6.add(renderString(stateTotalSizeTemplate, checkpointValues));
			targets7.add(renderString(stateCompactionFlushTemplate, checkpointValues));
			targets8.add(renderString(stateWriteStallTemplate, checkpointValues));
			targets9.add(renderString(stateCacheHitRate, checkpointValues));
			targets10.add(renderString(stateCacheMemoryUsage, checkpointValues));
		}
		checkpointValues.put("targets1", String.join(",", targets1));
		checkpointValues.put("targets2", String.join(",", targets2));
		checkpointValues.put("targets3", String.join(",", targets3));
		checkpointValues.put("targets4", String.join(",", targets4));
		checkpointValues.put("targets5", String.join(",", targets5));
		checkpointValues.put("targets6", String.join(",", targets6));
		checkpointValues.put("targets7", String.join(",", targets7));
		checkpointValues.put("targets8", String.join(",", targets8));
		checkpointValues.put("targets9", String.join(",", targets9));
		checkpointValues.put("targets10", String.join(",", targets10));
		return renderString(operatorStateTemplate, checkpointValues);
	}

	private String renderSlowContainerRow() {
		String slowContainerTemplate;

		try {
			slowContainerTemplate = renderFromResource(DashboardTemplate.SLOW_CONTAINER_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> slowContainerValues = new HashMap<>();
		slowContainerValues.put("jobname", jobName);
		slowContainerValues.put("datasource", dataSource);
		String slowContainerRow = renderString(slowContainerTemplate, slowContainerValues);
		return slowContainerRow;
	}

	private String renderYarnContainerRow() {
		String yarnContainerTemplate;
		try {
			yarnContainerTemplate = renderFromResource(DashboardTemplate.YARN_CONTAINER_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> yarnContainerValues = new HashMap<>();
		yarnContainerValues.put("jobname", jobName);
		yarnContainerValues.put("datasource", dataSource);
		String yarnContainerRow = renderString(yarnContainerTemplate, yarnContainerValues);
		return yarnContainerRow;
	}

	private String renderCompletedContainerRow() {
		String completedContainerTemplate = null;
		try {
			completedContainerTemplate = renderFromResource(DashboardTemplate.COMPLETED_CONTAINER_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render completed containers metrics.", e);
			return "";
		}
		Map<String, String> completedContainerValues = new HashMap<>();
		completedContainerValues.put("jobname", jobName);
		completedContainerValues.put("datasource", dataSource);
		return renderString(completedContainerTemplate, completedContainerValues);
	}

	private String renderPoolUsageRow(List<String> operators) {
		String inPoolUsageTargetTemplate;
		String outPoolUsageTargetTemplate;
		String poolUsageTemplate;

		try {
			inPoolUsageTargetTemplate = renderFromResource(DashboardTemplate.IN_POOL_USAGE_TARGET_TEMPLATE);
			outPoolUsageTargetTemplate = renderFromResource(DashboardTemplate.OUT_POOL_USAGE_TARGET_TEMPLATE);
			poolUsageTemplate = renderFromResource(DashboardTemplate.POOL_USAGE_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> poolUsageList = new ArrayList<>();
		for (int i = 0; i < operators.size(); i++) {
			Map<String, String> poolUsageTargetValues = new HashMap<>();
			poolUsageTargetValues.put("operator", operators.get(i));
			poolUsageTargetValues.put("jobname", formatJobName);
			poolUsageTargetValues.put("hide", i > targetLimit / 2 ? "true" : "false");
			poolUsageList.add(renderString(inPoolUsageTargetTemplate, poolUsageTargetValues));
			poolUsageList.add(renderString(outPoolUsageTargetTemplate, poolUsageTargetValues));
		}
		String targets = String.join(",", poolUsageList);
		Map<String, String> poolUsageValues = new HashMap<>();
		poolUsageValues.put("targets", targets);
		poolUsageValues.put("datasource", dataSource);
		String poolUsageRow = renderString(poolUsageTemplate, poolUsageValues);
		return poolUsageRow;
	}

	private List<String> renderPoolUsageSplitRow(List<String> operators) {
		if (operators.size() < targetLimit) {
			return Lists.newArrayList(renderInPoolUsageRow(operators, ""), renderOutPoolUsageRow(operators, ""));
		}

		List<List<String>> operatorLists = Utils.splitList(operators, targetLimit);
		List<String> rows = Lists.newArrayList();
		for (int i = 0; i < operatorLists.size(); i++) {
			List<String> operatorList = operatorLists.get(i);
			rows.add(renderInPoolUsageRow(operatorList, "#" + i));
			rows.add(renderOutPoolUsageRow(operatorList, "#" + i));
		}
		return rows;
	}

	private String renderInPoolUsageRow(List<String> operators, String suffix) {
		String inPoolUsageTargetTemplate;
		String poolUsageTemplate;

		try {
			inPoolUsageTargetTemplate = renderFromResource(DashboardTemplate.IN_POOL_USAGE_TARGET_TEMPLATE);
			poolUsageTemplate = renderFromResource(DashboardTemplate.IN_POOL_USAGE_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> poolUsageList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> poolUsageTargetValues = new HashMap<>();
			poolUsageTargetValues.put("operator", o);
			poolUsageTargetValues.put("jobname", formatJobName);
			poolUsageTargetValues.put("hide", "false");
			poolUsageList.add(renderString(inPoolUsageTargetTemplate, poolUsageTargetValues));
		}
		String targets = String.join(",", poolUsageList);
		Map<String, String> poolUsageValues = new HashMap<>();
		poolUsageValues.put("targets", targets);
		poolUsageValues.put("datasource", dataSource);
		poolUsageValues.put("suffix", suffix);
		String poolUsageRow = renderString(poolUsageTemplate, poolUsageValues);
		return poolUsageRow;
	}

	private String renderOutPoolUsageRow(List<String> operators, String suffix) {
		String outPoolUsageTargetTemplate;
		String poolUsageTemplate;

		try {
			outPoolUsageTargetTemplate = renderFromResource(DashboardTemplate.OUT_POOL_USAGE_TARGET_TEMPLATE);
			poolUsageTemplate = renderFromResource(DashboardTemplate.OUT_POOL_USAGE_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> poolUsageList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> poolUsageTargetValues = new HashMap<>();
			poolUsageTargetValues.put("operator", o);
			poolUsageTargetValues.put("jobname", formatJobName);
			poolUsageTargetValues.put("hide", "false");
			poolUsageList.add(renderString(outPoolUsageTargetTemplate, poolUsageTargetValues));
		}
		String targets = String.join(",", poolUsageList);
		Map<String, String> poolUsageValues = new HashMap<>();
		poolUsageValues.put("targets", targets);
		poolUsageValues.put("datasource", dataSource);
		poolUsageValues.put("suffix", suffix);
		String poolUsageRow = renderString(poolUsageTemplate, poolUsageValues);
		return poolUsageRow;
	}

	private String renderRecordNumRow(List<String> operators) {
		String recordInNumTargetTemplate;
		String recordOutNumTargetTemplate;
		String recordNumTemplate;

		try {
			recordInNumTargetTemplate = renderFromResource(DashboardTemplate.RECORD_IN_PER_SECOND_TARGET_TEMPLATE);
			recordOutNumTargetTemplate = renderFromResource(DashboardTemplate.RECORD_OUT_PER_SECOND_TARGET_TEMPLATE);
			recordNumTemplate = renderFromResource(DashboardTemplate.RECORD_NUM_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> recordNumTargetValues = new HashMap<>();
			recordNumTargetValues.put("operator", o);
			recordNumTargetValues.put("jobname", formatJobName);
			recordNumList.add(renderString(recordInNumTargetTemplate, recordNumTargetValues));
			recordNumList.add(renderString(recordOutNumTargetTemplate, recordNumTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> recordNumValues = new HashMap<>();
		recordNumValues.put("targets", targets);
		recordNumValues.put("datasource", dataSource);
		String poolUsageRow = renderString(recordNumTemplate, recordNumValues);
		return poolUsageRow;
	}

	private List<String> renderRecordNumSplitRow(List<String> operators) {
		if (operators.size() < targetLimit) {
			return Lists.newArrayList(renderRecordIndNumRow(operators, ""), renderRecordOutNumRow(operators, ""));
		}

		List<List<String>> operatorLists = Utils.splitList(operators, targetLimit);
		List<String> rows = Lists.newArrayList();
		for (int i = 0; i < operatorLists.size(); i++) {
			List<String> operatorList = operatorLists.get(i);
			rows.add(renderRecordIndNumRow(operatorList, "#" + i));
			rows.add(renderRecordOutNumRow(operatorList, "#" + i));
		}
		return rows;
	}

	private String renderRecordIndNumRow(List<String> operators, String suffix) {
		String recordInNumTargetTemplate;
		String recordNumTemplate;

		try {
			recordInNumTargetTemplate = renderFromResource(DashboardTemplate.RECORD_IN_PER_SECOND_TARGET_TEMPLATE);
			recordNumTemplate = renderFromResource(DashboardTemplate.RECORD_IN_NUM_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> recordNumTargetValues = new HashMap<>();
			recordNumTargetValues.put("operator", o);
			recordNumTargetValues.put("jobname", formatJobName);
			recordNumList.add(renderString(recordInNumTargetTemplate, recordNumTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> recordNumValues = new HashMap<>();
		recordNumValues.put("targets", targets);
		recordNumValues.put("datasource", dataSource);
		recordNumValues.put("suffix", suffix);
		String poolUsageRow = renderString(recordNumTemplate, recordNumValues);
		return poolUsageRow;
	}

	private String renderRecordOutNumRow(List<String> operators, String suffix) {
		String recordOutNumTargetTemplate;
		String recordNumTemplate;

		try {
			recordOutNumTargetTemplate = renderFromResource(DashboardTemplate.RECORD_OUT_PER_SECOND_TARGET_TEMPLATE);
			recordNumTemplate = renderFromResource(DashboardTemplate.RECORD_OUT_NUM_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> recordNumList = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> recordNumTargetValues = new HashMap<>();
			recordNumTargetValues.put("operator", o);
			recordNumTargetValues.put("jobname", formatJobName);
			recordNumList.add(renderString(recordOutNumTargetTemplate, recordNumTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> recordNumValues = new HashMap<>();
		recordNumValues.put("targets", targets);
		recordNumValues.put("datasource", dataSource);
		recordNumValues.put("suffix", suffix);
		String poolUsageRow = renderString(recordNumTemplate, recordNumValues);
		return poolUsageRow;
	}

	private String renderNetworkMemoryRow() {
		String networkMemoryTemplate;

		try {
			networkMemoryTemplate = renderFromResource(DashboardTemplate.NETWORK_MEMORY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		Map<String, String> networkMemoryValues = new HashMap<>();
		networkMemoryValues.put("datasource", dataSource);
		networkMemoryValues.put("jobname", jobName);
		String poolUsageRow = renderString(networkMemoryTemplate, networkMemoryValues);
		return poolUsageRow;
	}

	private String renderLateRecordsDropped(List<String> operators) {
		String lateRecordsDroppedTargetTemplate;
		String lateRecordsDroppedTemplate;

		try {
			lateRecordsDroppedTargetTemplate = renderFromResource(DashboardTemplate.LATE_RECORD_DROPPED_TARGET_TEMPLATE);
			lateRecordsDroppedTemplate = renderFromResource(DashboardTemplate.LATE_RECORDS_DROPPED_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
		return renderString(lateRecordsDroppedTemplate, lateRecordsDroppedValues);
	}

	private String renderDirtyRecordsSourceSkippedRow(List<String> sources) {
		String dirtyRecordsSkippedTargetTemplate;
		String dirtyRecordsSkippedTemplate;

		try {
			dirtyRecordsSkippedTargetTemplate = renderFromResource(DashboardTemplate.DIRTY_RECORDS_SOURCE_SKIPPED_TARGET_TEMPLATE);
			dirtyRecordsSkippedTemplate = renderFromResource(DashboardTemplate.DIRTY_RECORDS_SOURCE_SKIPPED_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
		return renderString(dirtyRecordsSkippedTemplate, dirtyRecordsSkippedValues);
	}

	private String renderRecordsSinkSkippedRow(List<String> sinks) {
		String recordsSkippedTargetTemplate;
		String recordsWriteSkippedTemplate;

		try {
			recordsSkippedTargetTemplate = renderFromResource(DashboardTemplate.RECORDS_SINK_SKIPPED_TARGET_TEMPLATE);
			recordsWriteSkippedTemplate = renderFromResource(DashboardTemplate.RECORDS_SINK_SKIPPED_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
		return renderString(recordsWriteSkippedTemplate, recordsWriteSkippedValues);
	}

	private String renderLookupRow(List<String> operators, String target, String metric) {
		String targetTemplate;
		String metricTemplate;

		try {
			targetTemplate = renderFromResource(target);
			metricTemplate = renderFromResource(metric);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> metricRows = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> targetValues = new HashMap<>(2);
			targetValues.put("operator", o);
			targetValues.put("jobname", formatJobName);
			metricRows.add(renderString(targetTemplate, targetValues));
		}
		String targets = String.join(",", metricRows);
		Map<String, String> values = new HashMap<>(2);
		values.put("targets", targets);
		values.put("datasource", dataSource);
		return renderString(metricTemplate, values);
	}

	private String renderWindowWatermarkLagRow(List<String> operators) {
		String targetTemplate;
		String metricTemplate;

		try {
			targetTemplate = renderFromResource(DashboardTemplate.WATERMARK_LATENCY_TARGET_TEMPLATE);
			metricTemplate = renderFromResource(DashboardTemplate.WATERMARK_LATENCY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> metricRows = new ArrayList<>();
		for (String o : operators) {
			Map<String, String> targetValues = new HashMap<>(2);
			targetValues.put("operator", o);
			targetValues.put("jobname", formatJobName);
			metricRows.add(renderString(targetTemplate, targetValues));
		}
		String targets = String.join(",", metricRows);
		Map<String, String> values = new HashMap<>(2);
		values.put("targets", targets);
		values.put("datasource", dataSource);
		return renderString(metricTemplate, values);
	}

	private String renderOperatorLatencyRow(List<String> operators) {
		return renderOperatorLatencyRow(operators, "");
	}

	private String renderOperatorLatencyRow(List<String> operators, String suffix) {
		String operatorLatencyTarget;
		String operatorLatencyTemplate;

		try {
			operatorLatencyTarget = renderFromResource(DashboardTemplate.OPERATOR_LATENCY_TARGET_TEMPLATE);
			operatorLatencyTemplate = renderFromResource(DashboardTemplate.OPERATOR_LATENCY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> recordNumList = new ArrayList<>();
		for (int i = 0; i < operators.size(); i++) {
			Map<String, String> operatorLatencyTargetValues = new HashMap<>();
			operatorLatencyTargetValues.put("operator", operators.get(i));
			operatorLatencyTargetValues.put("jobname", formatJobName);
			operatorLatencyTargetValues.put("hide", i > targetLimit ? "true" : "false");
			recordNumList.add(renderString(operatorLatencyTarget, operatorLatencyTargetValues));
		}
		String targets = String.join(",", recordNumList);
		Map<String, String> operatorLatencyValues = new HashMap<>();
		operatorLatencyValues.put("targets", targets);
		operatorLatencyValues.put("datasource", dataSource);
		operatorLatencyValues.put("suffix", "");
		String operatorLatencyRow = renderString(operatorLatencyTemplate, operatorLatencyValues);
		return operatorLatencyRow;
	}

	private List<String> renderOperatorSplitRow(List<String> operators) {
		if (operators.size() < targetLimit) {
			return Lists.newArrayList(renderOperatorLatencyRow(operators, ""), renderOutPoolUsageRow(operators, ""));
		}

		List<List<String>> operatorLists = Utils.splitList(operators, targetLimit);
		List<String> rows = Lists.newArrayList();
		for (int i = 0; i < operatorLists.size(); i++) {
			List<String> operatorList = operatorLists.get(i);
			rows.add(renderOperatorLatencyRow(operatorList, "#" + i));
			rows.add(renderOperatorLatencyRow(operatorList, "#" + i));
		}
		return rows;
	}

	private String renderKafkaOffsetRow(List<String> sources) {
		String kafkaOffsetTargetTemplate;
		String kafkaOffsetTemplate;

		try {
			kafkaOffsetTargetTemplate = renderFromResource(DashboardTemplate.KAFKA_OFFSET_TARGET_TEMPLATE);
			kafkaOffsetTemplate = renderFromResource(DashboardTemplate.KAFKA_OFFSET_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
		String kafkaOffsetRow = renderString(kafkaOffsetTemplate, kafkaOffsetValues);
		return kafkaOffsetRow;
	}

	private String renderKafkaLatencyRow(List<String> sources) {
		String kafkaLatencyTargetTemplate;
		String kafkaLatencyTemplate;

		try {
			kafkaLatencyTargetTemplate = renderFromResource(DashboardTemplate.KAFKA_LATENCY_TARGET_TEMPLATE);
			kafkaLatencyTemplate = renderFromResource(DashboardTemplate.KAFKA_LATENCY_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
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
		String kafkaLatencyRow = renderString(kafkaLatencyTemplate, kafkaOffsetValues);
		return kafkaLatencyRow;
	}

	private String renderKafkaPollIntervalRow(List<String> sources) {
		String kafkaPollIntervalTargetTemplate;
		String kafkaPollIntervalTemplate;

		try {
			kafkaPollIntervalTargetTemplate = renderFromResource(DashboardTemplate.KAFKA_POLL_INTERVAL_TARGET_TEMPLATE);
			kafkaPollIntervalTemplate = renderFromResource(DashboardTemplate.KAFKA_POLL_INTERVAL_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		List<String> kafkaPollIntervalList = new ArrayList<>();
		for (String s : sources) {
			Map<String, String> kafkaPollIntervalTargetValues = new HashMap<>();
			kafkaPollIntervalTargetValues.put("kafka_source", s);
			kafkaPollIntervalTargetValues.put("jobname", formatJobName);
			kafkaPollIntervalList.add(renderString(kafkaPollIntervalTargetTemplate, kafkaPollIntervalTargetValues));
		}
		String targets = String.join(",", kafkaPollIntervalList);
		Map<String, String> kafkaOffsetValues = new HashMap<>();
		kafkaOffsetValues.put("targets", targets);
		kafkaOffsetValues.put("datasource", dataSource);
		String kafPollIntervalRow = renderString(kafkaPollIntervalTemplate, kafkaOffsetValues);
		return kafPollIntervalRow;
	}

	public String renderDashboard() {
		List<String> rows = new ArrayList<>();
		List <String> operators = Utils.getSortedOperators(streamGraph);
		List <String> operatorsButSources = Utils.getSortedOperatorsExceptSources(streamGraph);
		List <String> sources = Utils.getSources(streamGraph);
		List <String> sinks = Utils.getSinks(streamGraph);
		List <String> tasks = Utils.getSortedTasks(jobGraph);
		List <String> lookupOperators = Utils.filterLookupOperators(operators);
		List <String> windowOperators = Utils.filterWindowOperators(operators);
		String kafkaServerUrl = System.getProperty(ConfigConstants.KAFKA_SERVER_URL_KEY,
			ConfigConstants.KAFKA_SERVER_URL_DEFAUL);
		JSONArray rocketmqConfigArray = Utils.getRocketMQConfigurations();
		List<String> kafkaMetricsList = Utils.getKafkaLagSizeMetrics(kafkaServerUrl);
		List<Tuple2<String, String>> kafkaConsumerUrls = Utils.getKafkaConsumerUrls(kafkaServerUrl, dataSource);

		List<String> overViewPanels = Lists.newArrayList();
		overViewPanels.add(renderOverviewRow());
		overViewPanels.add(renderJobInfoRow());
		List<String> kafkaPanels = Lists.newArrayList();
		if (!kafkaMetricsList.isEmpty()) {
			String kafkaLagSizeRow = renderKafkaLagSizeRow(kafkaMetricsList, kafkaConsumerUrls);
			overViewPanels.add(kafkaLagSizeRow);
			kafkaPanels.add(renderKafkaOffsetRow(sources));
			kafkaPanels.add(renderKafkaLatencyRow(sources));
			kafkaPanels.add(renderKafkaPollIntervalRow(sources));
		}
		if (!rocketmqConfigArray.isEmpty()) {
			overViewPanels.add(renderRocketMQLagSizeRow(rocketmqConfigArray));
		}
		overViewPanels.add(renderOperatorLatencyRow(operatorsButSources));
		overViewPanels.add(renderPoolUsageRow(tasks));
		overViewPanels.add(renderRecordNumRow(operators));

		// add overview row
		rows.add(renderRowTemplate(DashboardTemplate.OVERVIEW_ROW_TEMPLATE, overViewPanels, false));

		// add kafka row
		if (CollectionUtils.isNotEmpty(kafkaPanels)) {
			rows.add(renderRowTemplate(DashboardTemplate.KAFKA_ROW_TEMPLATE, kafkaPanels, false));
		}

		// add network row
		List<String> networkPanels = Lists.newArrayList();
		networkPanels.addAll(renderPoolUsageSplitRow(tasks));
		networkPanels.addAll(renderRecordNumSplitRow(operators));
		networkPanels.add(renderNetworkMemoryRow());
		rows.add(renderRowTemplate(DashboardTemplate.NETWORK_ROW_TEMPLATE, networkPanels));

		// add jvm row
		List<String> jvmPanels = Lists.newArrayList();
		jvmPanels.add(renderJMMemoryRow());
		jvmPanels.add(renderTMMemoryRow());
		jvmPanels.add(renderJMGcRow());
		jvmPanels.add(renderTMGcRow());
		jvmPanels.add(renderJMThreadRow());
		jvmPanels.add(renderTMThreadRow());
		rows.add(renderRowTemplate(DashboardTemplate.JVM_ROW_TEMPLATE, jvmPanels, false));

		// add schedule info row
		List<String> scheduleInfoPanels = Lists.newArrayList();
		scheduleInfoPanels.add(renderTmSlotRow());
		scheduleInfoPanels.add(renderYarnContainerRow());
		scheduleInfoPanels.add(renderSlowContainerRow());
		scheduleInfoPanels.add(renderCompletedContainerRow());
		rows.add(renderRowTemplate(DashboardTemplate.SCHEDULE_INFO_ROW_TEMPLATE, scheduleInfoPanels, false));

		// add watermark row
		List<String> watermarkPanels = Lists.newArrayList();
		watermarkPanels.add(renderLateRecordsDropped(operators));
		watermarkPanels.add(renderWindowWatermarkLagRow(windowOperators));
		rows.add(renderRowTemplate(DashboardTemplate.WATERMARK_ROW_TEMPLATE, watermarkPanels));

		// add checkpoint row
		rows.add(renderCheckpointOverviewRow());
		rows.add(renderCheckpointTimerRow(tasks));
		rows.add(renderCheckpointOperatorRow(tasks, operators));

		// add state row
		rows.add(renderOperatorStatePerformanceRow(operators));

		// add sql operator row
		List<String> sqlOperatorPanels = Lists.newArrayList();

		sqlOperatorPanels.add(renderDirtyRecordsSourceSkippedRow(sources));
		sqlOperatorPanels.add(renderRecordsSinkSkippedRow(sinks));
		sqlOperatorPanels.add(renderLookupRow(
			lookupOperators,
			DashboardTemplate.LOOKUP_JOIN_HIT_RATE_TARGET_TEMPLATE,
			DashboardTemplate.LOOKUP_JOIN_HIT_RATE_TEMPLATE));
		sqlOperatorPanels.add(renderLookupRow(
			lookupOperators,
			DashboardTemplate.LOOKUP_JOIN_FAIL_PER_SECOND_TARGET_TEMPLATE,
			DashboardTemplate.LOOKUP_JOIN_FAIL_PER_SECOND_TEMPLATE));
		sqlOperatorPanels.add(renderLookupRow(
			lookupOperators,
			DashboardTemplate.LOOKUP_JOIN_REQUEST_PER_SECOND_TARGET_TEMPLATE,
			DashboardTemplate.LOOKUP_JOIN_REQUEST_PER_SECOND_TEMPLATE));
		sqlOperatorPanels.add(renderLookupRow(
			lookupOperators,
			DashboardTemplate.LOOKUP_JOIN_REQUEST_DELAY_P99_PER_SECOND_TARGET_TEMPLATE,
			DashboardTemplate.LOOKUP_JOIN_REQUEST_DELAY_P99_PER_SECOND_TEMPLATE));
		rows.add(renderRowTemplate(DashboardTemplate.SQLOPERATOR_ROW_TEMPLATE, sqlOperatorPanels));

		String template;

		try {
			template = renderFromResource(DashboardTemplate.DASHBOARD_TEMPLATE);
		} catch (IOException e) {
			LOG.error("Fail to render row template.", e);
			return "";
		}
		String rowsStr = String.join(",", rows);
		Map<String, String> map = new HashMap<>();
		map.put("rows", rowsStr);
		map.put("jobname", jobName);
		map.put("cluster", clusterName);
		String dashboardJson = renderAutoIncreasingGlobalId(renderString(template, map));
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

	private String renderAutoIncreasingGlobalId(String template){
		String idOrder = renderAutoIncreasingGlobalId(template, "\\$\\{id\\}");
		String rowOrder = renderAutoIncreasingGlobalId(idOrder, "\\$\\{rowOrder\\}");
		return rowOrder;
	}

	private String renderAutoIncreasingGlobalId(String template, String regex) {
		int id = 0;
		Matcher matcher = Pattern.compile(regex).matcher(template);
		matcher.reset();
		boolean result = matcher.find();
		if (result) {
			StringBuffer sb = new StringBuffer();
			do {
				matcher.appendReplacement(sb, String.valueOf(id += 100));
				result = matcher.find();
			} while (result);
			matcher.appendTail(sb);
			return sb.toString();
		}
		return template;
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
