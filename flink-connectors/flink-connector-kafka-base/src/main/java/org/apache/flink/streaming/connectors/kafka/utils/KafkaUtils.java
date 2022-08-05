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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

/**
 * Utilities.
 */
public class KafkaUtils {
	public static final int KAFKA_CONNECTOR_VERSION = 1;

	public static ProcessingTimeService getTimeService(RuntimeContext context) {
		if (context instanceof StreamingRuntimeContext) {
			return ((StreamingRuntimeContext) context).getProcessingTimeService();
		}
		throw new IllegalArgumentException("Failed to get processing time service of context.");
	}

	public static void addKafkaVersionMetrics(
		MetricGroup metricGroup,
		String owner,
		String topic,
		String cluster,
		String connectorType,
		String consumerGroup,
		Gauge gauge) {
			metricGroup.addGroup("owner", owner)
				.addGroup("topic", topic)
				.addGroup("cluster", cluster)
				.addGroup("connectorType", connectorType)
				.addGroup("consumerGroup", consumerGroup)
				.gauge("kafkaConnectorVersion", gauge);
	}
}
