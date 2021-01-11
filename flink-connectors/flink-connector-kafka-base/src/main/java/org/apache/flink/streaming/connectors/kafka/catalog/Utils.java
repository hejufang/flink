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

import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.schema.registry.common.request.SchemaType;
import com.bytedance.schema.registry.common.response.QuerySchemaResponse;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_CLUSTER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

/**
 * Util class for kafka catalog.
 */
public class Utils {
	private static final String KAFAK_CONNECTOR_IDENTIFIER_DEAULT = "kafka-0.10";
	private static final String FLINK_PROPERTY_PREFIX = "flink.";
	private static final String JSON_FORMAT = "json";
	private static final String PB_FORMAT = "pb";
	private static final String PB_CONTENT_KEY = "pb.pb-content";
	private static final String PB_ENTRY_CLASS_NAME = "pb.pb-entry-class-name";

	public static Map<String, String> generateDDLProperties(QuerySchemaResponse response) {
		Map<String, String> properties = getDefaultProperties(response);
		properties.putAll(filterFlinkProperties(response.getExtraContent()));
		return properties;
	}

	private static Map<String, String> getDefaultProperties(QuerySchemaResponse response) {
		Map<String, String> defaultProperties = new HashMap<>();
		String cluster = response.getClusterId();
		String topic = response.getTopic();
		String format = parseFormat(response.getSchemaType());

		defaultProperties.putIfAbsent(PROPS_CLUSTER.key(), cluster);
		defaultProperties.putIfAbsent(TOPIC.key(), topic);
		defaultProperties.putIfAbsent(FactoryUtil.FORMAT.key(), format);
		defaultProperties.put(CONNECTOR, KAFAK_CONNECTOR_IDENTIFIER_DEAULT);

		if (PB_FORMAT.equalsIgnoreCase(format)) {
			defaultProperties.put(PB_CONTENT_KEY, response.getSchemaContent());
			defaultProperties.put(PB_ENTRY_CLASS_NAME, response.getProtobufRootClassName());
		} else if (JSON_FORMAT.equalsIgnoreCase(format)) {
			// No properties to add for json format.
		} else {
			throw new IllegalArgumentException(String.format("Unsupported schema type, supported types: %s, %s.", SchemaType.pb, SchemaType.json));
		}

		return defaultProperties;
	}

	private static Map<String, String> filterFlinkProperties(Map<String, String> propertiesInKafka) {
		Map<String, String> flinkProperties = new HashMap<>();
		propertiesInKafka.entrySet().stream()
			.filter(entry -> entry.getKey().startsWith(FLINK_PROPERTY_PREFIX))
			.forEach(entry ->
				flinkProperties.put(entry.getKey().substring(FLINK_PROPERTY_PREFIX.length()), entry.getValue()));
		return flinkProperties;
	}

	private static String parseFormat(SchemaType schemaType) {
		switch (schemaType) {
			case pb:
				return PB_FORMAT;
			case json:
				return JSON_FORMAT;
			default:
				throw new FlinkRuntimeException(String.format("Unsupported schema type: %s, " +
					"supported types: '%s', '%s'.", schemaType, SchemaType.pb, SchemaType.json));
		}
	}
}
