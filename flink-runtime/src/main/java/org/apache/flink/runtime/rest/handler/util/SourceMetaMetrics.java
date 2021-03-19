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

package org.apache.flink.runtime.rest.handler.util;

import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source meta metrics.
 */
public class SourceMetaMetrics {
	private static final Logger LOG = LoggerFactory.getLogger(SourceMetaMetrics.class);

	private static ObjectMapper objectMapper = new ObjectMapper();

	private ConsumerMetaInfo metaInfo;

	public SourceMetaMetrics() {}

	public void addMeta(AccessExecution attempt, @Nullable MetricFetcher fetcher, String jobID, String taskID) {
		if (fetcher != null) {
			fetcher.update();
			MetricStore.ComponentMetricStore metrics = fetcher.getMetricStore()
				.getSubtaskMetricStore(jobID, taskID, attempt.getParallelSubtaskIndex());

			if (metrics != null) {
				metaInfo = parseConsumerMetaInfo(metrics.metrics);
			}
		}
	}

	public static ConsumerMetaInfo parseConsumerMetaInfo(Map<String, String> metrics) {
		for (Map.Entry<String, String> entry : metrics.entrySet()) {
			if (entry.getKey().contains(MetricNames.SOURCE_TOPIC_PARTITIONS)) {
				final String jsonString = entry.getValue();
				try {
					return objectMapper.readValue(entry.getValue(), ConsumerMetaInfo.class);
				} catch (IOException e) {
					LOG.warn("Fail to parse " + jsonString, e);
				}
			}
		}
		return ConsumerMetaInfo.EMPTY_INSTANCE;
	}

	public ConsumerMetaInfo getMetaInfo() {
		if (metaInfo == null) {
			return ConsumerMetaInfo.EMPTY_INSTANCE;
		} else {
			return metaInfo;
		}
	}

	/**
	 * Consumer metadata parsed from metrics.
	 */
	public static class ConsumerMetaInfo {

		public static final ConsumerMetaInfo EMPTY_INSTANCE = new ConsumerMetaInfo("", "");

		private String cluster;
		private String consumerGroup;
		private Map<String, List<Integer>> topicAndPartitions;

		// used for jackson's ObjectMapper
		public ConsumerMetaInfo() {}

		public ConsumerMetaInfo(String cluster, String consumerGroup) {
			this.cluster = cluster;
			this.consumerGroup = consumerGroup;
			this.topicAndPartitions = new HashMap<>();
		}

		public void setCluster(String cluster) {
			this.cluster = cluster;
		}

		public void setConsumerGroup(String consumerGroup) {
			this.consumerGroup = consumerGroup;
		}

		public void setTopicAndPartitions(Map<String, List<Integer>> topicAndPartitions) {
			this.topicAndPartitions = topicAndPartitions;
		}

		public String getCluster() {
			return cluster;
		}

		public String getConsumerGroup() {
			return consumerGroup;
		}

		public Map<String, List<Integer>> getTopicAndPartitions() {
			return topicAndPartitions;
		}

		public String calculatePartitions() {
			final List<String> partitions = new ArrayList<>();
			for (List<Integer> list : topicAndPartitions.values()) {
				for (int element : list) {
					partitions.add(String.valueOf(element));
				}
			}

			return String.join(",", partitions);
		}
	}
}
