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

package org.apache.flink.connectors.rpc.util;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ValueNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Json util.
 */
public class JsonUtil {
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	/**
	 * Get the json string value.
	 * @param key the json key
	 * @param jsonString the json string which we will find in this json.
	 * @return the corresponding value of key.
	 */
	public static String getInnerJson(String key, String jsonString) {
		JsonNode tree;
		try {
			tree = OBJECT_MAPPER.readTree(jsonString);
		} catch (IOException e) {
			throw new FlinkRuntimeException(String.format("Parse jsonString : %s failed.", jsonString), e);
		}
		JsonNode innerNode = tree.findValue(key);
		return innerNode == null ? null : innerNode.toString();
	}

	/**
	 * jsonStrA,jsonStrB is json string. Judge that all the keys in jsonStrA, jsonStrB contain them.
	 * And the corresponding value is also same.
	 */
	public static boolean isSecondJsonCoversFirstJson(String jsonStrA, String jsonStrB) {
		if (jsonStrA == null) {
			return jsonStrB == null;
		}
		if (jsonStrA.equals(jsonStrB)) {
			return true;
		}
		JsonNode aTree;
		JsonNode bTree;
		try {
			aTree = OBJECT_MAPPER.readTree(jsonStrA);
			bTree = OBJECT_MAPPER.readTree(jsonStrB);
		} catch (IOException e) {
			throw new FlinkRuntimeException(String.format("Parse json failed. User provide : %s. RPC response" +
				" : %s", jsonStrA, jsonStrB), e);
		}
		return isSecondJsonCoversFirstJson(aTree, bTree);
	}

	private static boolean isSecondJsonCoversFirstJson(JsonNode jsonNodeA, JsonNode jsonNodeB) {
		if (jsonNodeA != null && jsonNodeB == null) {
			return false;
		}
		if (jsonNodeA instanceof ObjectNode && jsonNodeB instanceof ObjectNode) {
			return isSecondJsonCoversFirstJson((ObjectNode) jsonNodeA, (ObjectNode) jsonNodeB);
		} else if (jsonNodeA instanceof ValueNode && jsonNodeB instanceof ValueNode) {
			return equals((ValueNode) jsonNodeA, (ValueNode) jsonNodeB);
		} else if (jsonNodeA instanceof ArrayNode) {
			throw new FlinkRuntimeException("connector.response-value only support basic types.");
		} else {
			return false;
		}
	}

	private static boolean isSecondJsonCoversFirstJson(ObjectNode jsonObjectA, ObjectNode jsonObjectB) {
		Iterator<Map.Entry<String, JsonNode>> fields = jsonObjectA.fields();
		while (fields.hasNext()) {
			Map.Entry<String, JsonNode> cur = fields.next();
			if (!(isSecondJsonCoversFirstJson(cur.getValue(), jsonObjectB.get(cur.getKey())))){
				return false;
			}
		}
		return true;
	}

	private static boolean equals(ValueNode jsonNodeA, ValueNode jsonNodeB) {
		return jsonNodeA.equals(jsonNodeB);
	}
}
