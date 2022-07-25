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

package com.bytedance.flink.utils;

import org.apache.flink.util.StringUtils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka utils.
 */
public class KafkaUtils {
	private static final Logger LOG = LoggerFactory.getLogger(CoreDumpUtils.class);

	private static Map<String, KafkaProducer> kafkaProducerMap = new HashMap<>();

	private static Properties getKafkaClusterProperties(String cluster, String bootstrapServers) {
		Properties properties = new Properties();
		if (!StringUtils.isNullOrWhitespaceOnly(bootstrapServers)) {
			LOG.info("use bootstrapServers");
			properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Arrays.asList(bootstrapServers.split(",")));
			properties.put(CommonClientConfigs.ENABLE_DEFAULT_CONFIG_CLIENT, true);
		} else if (cluster != null){
			LOG.info("use cluster");
			properties.put(ProducerConfig.CLUSTER_NAME_CONFIG, cluster);
		}
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}

	public static int getPartitionNum(String cluster, String topic, String bootstrapServers) {
		int maxRetryTimes = 3;
		int partitions = -1;
		for (int i = 0; i < maxRetryTimes; i++) {
			try {
				KafkaProducer producer = null;
				if (StringUtils.isNullOrWhitespaceOnly(cluster) && !StringUtils.isNullOrWhitespaceOnly(bootstrapServers)) {
					producer = kafkaProducerMap.get(bootstrapServers);
				} else if (!StringUtils.isNullOrWhitespaceOnly(cluster)) {
					producer = kafkaProducerMap.get(cluster);
				}
				if ((StringUtils.isNullOrWhitespaceOnly(cluster) && StringUtils.isNullOrWhitespaceOnly(bootstrapServers))
					|| topic == null) {
					throw new RuntimeException("Cluster/BootStrapServers or topic can't be NULL.");
				}

				if (producer == null) {
					Properties properties = getKafkaClusterProperties(cluster, bootstrapServers);
					properties.setProperty("request.timeout.ms", "100000");
					producer = new KafkaProducer(properties);
					if (StringUtils.isNullOrWhitespaceOnly(cluster) && !StringUtils.isNullOrWhitespaceOnly(bootstrapServers)) {
						kafkaProducerMap.put(bootstrapServers, producer);
					} else if (!StringUtils.isNullOrWhitespaceOnly(cluster)) {
						kafkaProducerMap.put(cluster, producer);
					}
				}

				List partitionInfoList = producer.partitionsFor(topic);
				partitions = partitionInfoList.size();
				break;
			} catch (Throwable t) {
				kafkaProducerMap.remove(cluster);
				if (i < maxRetryTimes - 1) {
					LOG.warn("Failed to get partition info, retry...");
				} else {
					LOG.error("Error occurred while get partition info.", t);
				}
			}
		}
		return partitions;
	}
}
