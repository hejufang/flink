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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.SlowTaskOptions;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.runtime.externalhandler.ExternalRequestHandleReport;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.flink.shaded.httpclient.org.apache.http.client.methods.HttpPost;
import org.apache.flink.shaded.httpclient.org.apache.http.entity.ContentType;
import org.apache.flink.shaded.httpclient.org.apache.http.entity.StringEntity;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.flink.shaded.httpclient.org.apache.http.impl.client.HttpClients;
import org.apache.flink.shaded.httpclient.org.apache.http.util.EntityUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manager that controls slow task behavior.
 */
public class SlowTaskManager {

	private final Logger log = LoggerFactory.getLogger(SlowTaskManager.class);

	private final ScheduledExecutorService scheduledExecutorService;
	private final JobManagerJobMetricGroup jobManagerJobMetricGroup;
	private final Boolean enableSlowTask;

	private final String externalServiceBaseUrl;

	private final String externalServiceCheckApi;

	private final Duration checkInterval;

	private final Duration minCheckInterval;

	private Long lastReleaseTaskTime;

	private final String jobName;

	private final String jobUniqueKey;

	private boolean running = false;

	private final TagGauge handleResultGuage = new TagGauge.TagGaugeBuilder().setClearWhenFull(true).build();

	public SlowTaskManager(Configuration configuration,
		ScheduledExecutorService scheduledExecutorService,
		JobManagerJobMetricGroup jobManagerJobMetricGroup) {
		this.scheduledExecutorService = scheduledExecutorService;
		this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
		this.enableSlowTask = configuration.get(SlowTaskOptions.SLOW_TASK_ENABLED);
		this.externalServiceBaseUrl = configuration.get(SlowTaskOptions.ENGINE_EXTERNAL_SERVICE_BASE_URL);
		this.externalServiceCheckApi = configuration.get(SlowTaskOptions.SLOW_TASK_EXTERNAL_SERVICE_CHECK_API);
		this.minCheckInterval = Duration.ofMinutes(10);
		Duration configCheckDuration = configuration.get(SlowTaskOptions.SLOW_TASK_CHECK_INTERVAL);
		this.checkInterval = configCheckDuration.compareTo(minCheckInterval) < 0 ? minCheckInterval : configCheckDuration;
		this.jobName = configuration.get(PipelineOptions.NAME);
		this.jobUniqueKey = configuration.get(SlowTaskOptions.EXTERNAL_EXTERNAL_SERVICE_JOB_UNIQUE_KEY);
		this.lastReleaseTaskTime = 0L;
		registerMetrics();
	}

	public boolean inEnabled() {
		return enableSlowTask;
	}

	public void start() {
		running = true;
		if (enableSlowTask) {
			if (StringUtils.isBlank(externalServiceBaseUrl)) {
				log.warn("slow task check url is empty, will not check");
				return;
			}
			log.info("start to schedule slow task check, jobUniqueKey:{}, checkIntervalMs:{}", jobUniqueKey, checkInterval.toMillis());
			scheduleRunAsync(this::check, checkInterval);
		}
	}

	public void stop() {
		running = false;
	}

	private void check() {
		if (!running) {
			return;
		}

		try (CloseableHttpClient client = HttpClients.createDefault()) {
			HttpPost httpPost = buildHttpPost();
			CloseableHttpResponse response =  client.execute(httpPost);
			if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
				log.warn("call slow task check API failed, status code: {}", response.getStatusLine().getStatusCode());
			} else {
				String resStr = EntityUtils.toString(response.getEntity(), "UTF-8");
				log.debug("call slow task check success, res: {}", resStr);
			}
		} catch (Exception e) {
			log.error("call slow task check API failed.", e);
		}

		scheduleRunAsync(this::check, checkInterval);
	}

	private HttpPost buildHttpPost() {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode node = mapper.createObjectNode();
		node.put("jobUniqueKey", jobUniqueKey);
		node.put("jobName", jobName);
		String json = node.toString();

		HttpPost httpPost = new HttpPost(externalServiceBaseUrl + externalServiceCheckApi);
		httpPost.setEntity(new StringEntity(json, ContentType.APPLICATION_JSON));
		httpPost.setHeader("Accept", "application/json");
		httpPost.setHeader("Content-type", "application/json");
		return httpPost;
	}

	private void scheduleRunAsync(Runnable runnable, Duration delay) {
		scheduledExecutorService.schedule(runnable, delay.toMillis(), TimeUnit.MILLISECONDS);
	}

	public void callBackHandleRequest(ExternalRequestHandleReport requestHandleReport) {
		if (!requestHandleReport.isHandleSuccess()) {
			log.warn("handle slow task failed, requestId:{}", requestHandleReport.getRequestId());
		}
		if (requestHandleReport.isTaskRelease()) {
			lastReleaseTaskTime = System.currentTimeMillis();
		}
		log.info("handle slow task requestId:{}, log:{}",
			requestHandleReport.getRequestId(), requestHandleReport.getHandleLog());
		handleResultGuage(requestHandleReport);
	}

	private void registerMetrics() {
		jobManagerJobMetricGroup.gauge(MetricNames.SLOW_TASK_HANDLE_RESULT, handleResultGuage);
	}

	private void handleResultGuage(ExternalRequestHandleReport requestHandleReport) {
		handleResultGuage.addMetric(
			1,
			new TagGaugeStoreImpl.TagValuesBuilder()
				.addTagValue("requestId", requestHandleReport.getRequestId())
				.addTagValue("handleSuccess", String.valueOf(requestHandleReport.isHandleSuccess()))
				.addTagValue("taskRelease", String.valueOf(requestHandleReport.isTaskRelease()))
				.build());
	}

	public Long getLastReleaseTaskTime() {
		return lastReleaseTaskTime;
	}
}
