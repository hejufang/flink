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

package org.apache.flink.streaming.connectors.kafka.utils;

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer configuration.
 */
public class KafkaConsumerConf implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String clusterName;
	private final List<String> topics;
	private final String group;
	private final Properties properties;
	private final StartupMode startupMode;

	public KafkaConsumerConf(String clusterName, List<String> topics, String group, Properties properties) {
		this(clusterName, topics, group, properties, StartupMode.GROUP_OFFSETS);
	}

	public KafkaConsumerConf(
			String clusterName,
			List<String> topics,
			String group,
			Properties properties,
			StartupMode startupMode) {
		Preconditions.checkNotNull(clusterName);
		Preconditions.checkNotNull(topics);
		Preconditions.checkNotNull(group);
		Preconditions.checkNotNull(properties);
		Preconditions.checkNotNull(startupMode);
		this.clusterName = clusterName;
		this.topics = topics;
		this.group = group;
		this.properties = properties;
		this.startupMode = startupMode;
	}

	public String getClusterName() {
		return clusterName;
	}

	public List<String> getTopics() {
		return topics;
	}

	public String getGroup() {
		return group;
	}

	public Properties getProperties() {
		return properties;
	}

	public StartupMode getStartupMode() {
		return startupMode;
	}

	public Properties toKafkaProperties() {
		Properties properties = new Properties();
		properties.putAll(this.properties);
		String joinedTopics = String.join(",", topics);
		properties.put(ConsumerConfig.CLUSTER_NAME_CONFIG, clusterName);
		properties.put(ConsumerConfig.TOPIC_NAME_CONFIG, joinedTopics);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		return properties;
	}

	@Override
	public String toString() {
		return "KafkaConsumerConf{" +
			"clusterName='" + clusterName + '\'' +
			", topics=" + topics +
			", group='" + group + '\'' +
			", properties=" + properties +
			", startupMode=" + startupMode +
			'}';
	}
}
