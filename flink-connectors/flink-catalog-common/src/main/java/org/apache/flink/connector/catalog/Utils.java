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

package org.apache.flink.connector.catalog;

import org.apache.flink.table.factories.FactoryUtil;

import com.bytedance.schema.registry.common.response.QuerySchemaResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Util class for kafka catalog.
 */
public class Utils {
	private static final String FLINK_PROPERTY_PREFIX = "flink.";
	private static final String JSON_FORMAT = "json";
	private static final String PB_FORMAT = "pb";
	private static final String PB_CONTENT_KEY = "pb.pb-content";
	private static final String PB_ENTRY_CLASS_NAME = "pb.pb-entry-class-name";

	public static void addCommonProperty(
			Map<String, String> defaultProperties,
			QuerySchemaResponse response) {
		String schemaType = response.getSchemaType();
		if (schemaType.equals(PB_FORMAT)) {
			defaultProperties.put(FactoryUtil.FORMAT.key(), PB_FORMAT);
			defaultProperties.put(PB_CONTENT_KEY, response.getSchemaContent());
			defaultProperties.put(PB_ENTRY_CLASS_NAME, response.getProtobufRootClassName());
		} else {
			defaultProperties.putIfAbsent(FactoryUtil.FORMAT.key(), JSON_FORMAT);
		}
	}

	public static Map<String, String> filterFlinkProperties(Map<String, Object> propertiesInKafka) {
		Map<String, String> flinkProperties = new HashMap<>();
		propertiesInKafka.entrySet().stream()
			.filter(entry -> entry.getKey().startsWith(FLINK_PROPERTY_PREFIX))
			.forEach(entry ->
				flinkProperties.put(entry.getKey().substring(FLINK_PROPERTY_PREFIX.length()), entry.getValue().toString()));
		return flinkProperties;
	}
}
