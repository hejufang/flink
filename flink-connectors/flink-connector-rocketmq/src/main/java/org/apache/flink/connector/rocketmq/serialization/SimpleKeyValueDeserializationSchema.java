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

package org.apache.flink.connector.rocketmq.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * SimpleKeyValueDeserializationSchema.
 * Deserialize rocketmq MessageQueue to Map.
 */
public class SimpleKeyValueDeserializationSchema implements RocketMQDeserializationSchema<Map> {
	public static final String DEFAULT_KEY_FIELD = "key";
	public static final String DEFAULT_VALUE_FIELD = "value";

	private final String keyField;
	private final String valueField;

	public SimpleKeyValueDeserializationSchema() {
		this(DEFAULT_KEY_FIELD, DEFAULT_VALUE_FIELD);
	}

	/**
	 * SimpleKeyValueDeserializationSchema Constructor.
	 *
	 * @param keyField   tuple field for selecting the key
	 * @param valueField tuple field for selecting the value
	 */
	public SimpleKeyValueDeserializationSchema(String keyField, String valueField) {
		this.keyField = keyField;
		this.valueField = valueField;
	}

	@Override
	public boolean isEndOfStream(Set<MessageQueue> balanceMQSet, Map nextElement) {
		return false;
	}

	@Override
	public Map deserialize(MessageQueue messageQueue, MessageExt record) throws Exception {
		byte[] key =
			record.getMsg().getKeys() != null ? record.getMsg().getKeys().getBytes(StandardCharsets.UTF_8) : null;
		byte[] value = record.getMsg().getBody().toByteArray();

		HashMap map = new HashMap(2);
		if (keyField != null) {
			String k = (key != null ? new String(key, StandardCharsets.UTF_8) : null);
			map.put(keyField, k);
		}
		if (valueField != null) {
			String v = (value != null ? new String(value, StandardCharsets.UTF_8) : null);
			map.put(valueField, v);
		}
		return map;
	}

	@Override
	public boolean isStreamingMode() {
		return true;
	}

	@Override
	public TypeInformation<Map> getProducedType() {
		return TypeInformation.of(Map.class);
	}

	@Override
	public void deserialize(byte[] message, Collector<Map> out) throws Exception {
		throw new FlinkRuntimeException("Shouldn't reach here.");
	}
}
