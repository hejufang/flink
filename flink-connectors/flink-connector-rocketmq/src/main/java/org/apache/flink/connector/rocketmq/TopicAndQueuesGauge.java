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

package org.apache.flink.connector.rocketmq;

import org.apache.flink.metrics.Gauge;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Gauge for getting topic and broker/queues.
 */
public class TopicAndQueuesGauge implements Gauge<String> {
	private final ConsumerMetaInfo consumerMetaInfo;
	private final ObjectMapper objectMapper;

	public TopicAndQueuesGauge(String cluster, String consumerGroup) {
		this.consumerMetaInfo = new ConsumerMetaInfo(cluster, consumerGroup);
		this.objectMapper = new ObjectMapper();
	}

	public void addTopicAndQueue(String topic, String queueInfo) {
		if (!consumerMetaInfo.getTopicAndQueues().containsKey(topic)) {
			consumerMetaInfo.getTopicAndQueues().put(topic, new HashSet<>());
		}
		consumerMetaInfo.getTopicAndQueues().get(topic).add(queueInfo);
	}

	@Override
	public String getValue() {
		try {
			return objectMapper.writeValueAsString(consumerMetaInfo);
		} catch (JsonProcessingException e) {
			return "";
		}
	}

	private static class ConsumerMetaInfo {
		private final String cluster;
		private final String consumerGroup;
		private final Map<String, Set<String>> topicAndQueues;

		ConsumerMetaInfo(String cluster, String consumerGroup) {
			this.cluster = cluster;
			this.consumerGroup = consumerGroup;
			this.topicAndQueues = new HashMap<>();
		}

		public String getCluster() {
			return cluster;
		}

		public String getConsumerGroup() {
			return consumerGroup;
		}

		public Map<String, Set<String>> getTopicAndQueues() {
			return topicAndQueues;
		}
	}
}
