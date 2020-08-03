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

package org.apache.flink.connectors.faas.utils;

import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * faas utils.
 */
public class FaasUtils {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	/**
	 * Used to parse fass response, There are three situations here:
	 * 1. throw IOException, means it fails and should retry.
	 * 2. return null, means it fails directly without retry.
	 * 3. return list, means it it successful.
	 */
	public static List<Row> parseJSONString(
			String s,
			JsonRowDeserializationSchema deserializationSchema) throws IOException {
		JsonNode tree = OBJECT_MAPPER.readTree(s);
		if (!tree.isArray()) {
			return null;
		}
		ArrayNode arrayNode = (ArrayNode) tree;
		List<Row> results = new ArrayList<>();
		for (int i = 0; i < arrayNode.size(); i++) {
			JsonNode arrayElement = arrayNode.get(i);
			if (arrayElement.isContainerNode()) {
				results.add(deserializationSchema.deserialize(arrayElement.toString().getBytes()));
			} else {
				results.add(deserializationSchema.deserialize(arrayElement.asText().getBytes()));
			}
		}
		return results;
	}
}
