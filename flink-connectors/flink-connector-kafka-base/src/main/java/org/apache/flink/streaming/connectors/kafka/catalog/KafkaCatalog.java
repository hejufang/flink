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

package org.apache.flink.streaming.connectors.kafka.catalog;

import org.apache.flink.connector.catalog.BytedSchemaCatalog;

import com.bytedance.schema.registry.common.request.ClusterType;
import com.bytedance.schema.registry.common.response.QuerySchemaResponse;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_CLUSTER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * Kafka catalog.
 */
public class KafkaCatalog extends BytedSchemaCatalog {
	private static final String KAFAK_CONNECTOR_IDENTIFIER_DEAULT = "kafka-0.10";

	public KafkaCatalog(String name, String defaultDatabase) {
		super(name, defaultDatabase, ClusterType.kafka_bmq);
	}

	@Override
	protected Map<String, String> getDefaultProperties(QuerySchemaResponse response) {
		Map<String, String> defaultProperties = new HashMap<>();
		String cluster = response.getClusterId();
		String topic = response.getTopic();

		defaultProperties.putIfAbsent(PROPS_CLUSTER.key(), cluster);
		defaultProperties.putIfAbsent(TOPIC.key(), topic);
		defaultProperties.put(CONNECTOR, KAFAK_CONNECTOR_IDENTIFIER_DEAULT);

		return defaultProperties;
	}
}
