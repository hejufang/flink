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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.handler.util.SourceMetaMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.JobSourceMetaMetricsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.SourceMetaMetricsInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Request handler for source meta used in FFS.
 */
public class SourceMetadataHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobSourceMetaMetricsInfo, JobMessageParameters> {

	private final MetricFetcher fetcher;

	public SourceMetadataHandler(
		GatewayRetriever<? extends RestfulGateway> leaderRetriever,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<EmptyRequestBody, JobSourceMetaMetricsInfo, JobMessageParameters> messageHeaders,
		MetricFetcher metricFetcher) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.fetcher = metricFetcher;
	}

	@Override
	protected CompletableFuture<JobSourceMetaMetricsInfo> handleRequest(
		@Nonnull HandlerRequest<EmptyRequestBody, JobMessageParameters> request,
		@Nonnull RestfulGateway gateway) throws RestHandlerException {
		if (fetcher != null) {
			fetcher.update();

			final List<SourceMetaMetrics.ConsumerMetaInfo> list = new ArrayList<>();
			final Map<String, MetricStore.JobMetricStore> jobMetricStoreMap = fetcher.getMetricStore().getJobs();

			for (Map.Entry<String, MetricStore.JobMetricStore> jobMetricStoreEntry : jobMetricStoreMap.entrySet()) {
				final MetricStore.JobMetricStore jobStore = jobMetricStoreEntry.getValue();
				final Map<String, MetricStore.TaskMetricStore> taskMetricsStores = jobStore.getAllTaskMetricStores();
				for (Map.Entry<String, MetricStore.TaskMetricStore> taskMetricStoreEntry : taskMetricsStores.entrySet()) {
					final MetricStore.TaskMetricStore taskStore = taskMetricStoreEntry.getValue();
					final Collection<MetricStore.SubtaskMetricStore> subtaskMetricStores = taskStore.getAllSubtaskMetricStores().values();
					for (MetricStore.SubtaskMetricStore subtaskMetricStore : subtaskMetricStores) {
						list.add(SourceMetaMetrics.parseConsumerMetaInfo(subtaskMetricStore.metrics));
					}
				}
			}

			// Merge all consumerMetaInfos
			final Map<String, SourceMetaMetricsInfo> m = new HashMap<>();
			final Map<String, SourceMetaMetricsInfo> rmqMeta = new HashMap<>();
			for (SourceMetaMetrics.ConsumerMetaInfo info : list) {
				if (info.equals(SourceMetaMetrics.ConsumerMetaInfo.EMPTY_INSTANCE)) {
					continue;
				}
				final String cluster = info.getCluster();
				final String group = info.getConsumerGroup();
				final Set<String> topics = info.getTopics();
				final String key = String.join("-", cluster, group);
				final String sourceType = info.getSourceType();
				if (sourceType.equals(SourceMetaMetrics.ConsumerMetaInfo.SOURCE_TYPE_KAFKA)) {
					if (!m.containsKey(key)) {
						m.put(key, new SourceMetaMetricsInfo(cluster, group, new ArrayList<>()));
					}
					for (String topic : topics) {
						if (!m.get(key).getTopics().contains(topic)) {
							m.get(key).getTopics().add(topic);
						}
					}
				} else if (sourceType.equals(SourceMetaMetrics.ConsumerMetaInfo.SOURCE_TYPE_ROCKETMQ)){
					if (!rmqMeta.containsKey(key)) {
						rmqMeta.put(key, new SourceMetaMetricsInfo(cluster, group, new ArrayList<>()));
					}
					for (String topic : topics) {
						if (!rmqMeta.get(key).getTopics().contains(topic)) {
							rmqMeta.get(key).getTopics().add(topic);
						}
					}
				} else {
					log.warn("Ignore unsupported ConsumerMetaInfo {}", info);
				}
			}

			return CompletableFuture.completedFuture(
				new JobSourceMetaMetricsInfo(new ArrayList<>(m.values()), new ArrayList<>(rmqMeta.values())));
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}
}
