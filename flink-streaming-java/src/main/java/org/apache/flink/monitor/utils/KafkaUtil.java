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
import org.apache.flink.runtime.util.HttpUtil;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kafka util to parse topic information.
 */
public class KafkaUtil {
	private static final String METRICS_PREFIX_QUERY_URL =
		"/queryClusterMetricsPrefix.do?cluster=%s";
	private static final String KAFKA_CONSUMER_DASHBOARD_URL_FORMAT = "%s/dashboard/db/data-inf-kafka-user-consumer_offset?orgId=1&var-consumer_group=%s&var-topic=%s&var-topic_related_metric_prefix=%s&var-broker_related_metric_prefix=%s&var-data_source=%s&var-dc=*&refresh=1m";
	private static final String CLUSTER_METRICS_PREFIX_KEY = "clusterMetricsPrefix";
	private static final String TOPIC_RELATED_METRIC_PREFIX = "topic_related_metric_prefix";
	private static final String BROKER_RELATED_METRIC_PREFIX = "broker_related_metric_prefix";
	private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
	private static final int MAX_RETRY_TIMES = 3;
	private static final String METRICS_SEPARATOR = ",";

	/**
	 * @return Kafka topic prefix. Return null if there is something wrong.
	 */
	public static Tuple2<String, String> getKafkaTopicPrefix(String cluster, String kafkaServerUrl) {
		String url = String.format(kafkaServerUrl + METRICS_PREFIX_QUERY_URL, cluster);
		LOG.info("kafka metrics query url = {}", url);
		int retryTimes = 0;
		while (retryTimes++ < MAX_RETRY_TIMES) {
			try {
				HttpUtil.HttpResponsePojo response = HttpUtil.sendGet(url);
				int statusCode = response.getStatusCode();
				boolean success = statusCode == 200;
				String resStr = response.getContent();
				if (!success) {
					LOG.warn("Failed to get kafka metrics prefix, response: {}", resStr);
					return null;
				}
				JSONParser parser = new JSONParser();
				JSONObject json = (JSONObject) parser.parse(resStr);
				JSONObject kafkaMetricPrefix =
					(JSONObject) json.getOrDefault(CLUSTER_METRICS_PREFIX_KEY, new JSONObject());
				String topicMetricPrefix =
					(String) kafkaMetricPrefix.get(TOPIC_RELATED_METRIC_PREFIX);
				if (topicMetricPrefix != null && topicMetricPrefix.contains(METRICS_SEPARATOR)) {
					topicMetricPrefix = topicMetricPrefix.split(METRICS_SEPARATOR)[0];
				}
				String brokerMetricPrefix =
					(String) kafkaMetricPrefix.get(BROKER_RELATED_METRIC_PREFIX);
				if (brokerMetricPrefix != null && brokerMetricPrefix.contains(METRICS_SEPARATOR)) {
					brokerMetricPrefix = brokerMetricPrefix.split(METRICS_SEPARATOR)[0];
				}
				return Tuple2.of(topicMetricPrefix, brokerMetricPrefix);
			} catch (IOException | ParseException e) {
				LOG.warn("Failed to get kafka topic prefix. kafka cluster = {}, " +
					"kafkaServerUrl = {}", cluster, kafkaServerUrl, e);
			}
		}
		return null;
	}

	public static String getKafkaConsumerDashboardUrl(String consumerGroup, String topic, String topicPrefix, String brokerPrefix, String datasource) {
		String grafanaDomainUrl = System.getProperty(ConfigConstants.GRAFANA_DOMAIN_URL_KEY,
			ConfigConstants.GRAFANA_DOMAIN_URL_VALUE);
		return String.format(KAFKA_CONSUMER_DASHBOARD_URL_FORMAT, grafanaDomainUrl, consumerGroup, topic, topicPrefix, brokerPrefix, datasource);
	}
}
