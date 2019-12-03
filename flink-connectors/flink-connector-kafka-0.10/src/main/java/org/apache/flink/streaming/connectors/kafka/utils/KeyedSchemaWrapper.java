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

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

/**
 * The keyedSerializationSchema which handles RecordWrapperPOJO.
 */
public class KeyedSchemaWrapper<T> implements KeyedSerializationSchema<RecordWrapperPOJO<T>> {
	KeyedSerializationSchema<T> keyedSerializationSchema;

	public KeyedSchemaWrapper(KeyedSerializationSchema<T> keyedSerializationSchema) {
		this.keyedSerializationSchema = keyedSerializationSchema;
	}

	@Override
	public byte[] serializeKey(RecordWrapperPOJO<T> element) {
		return keyedSerializationSchema.serializeKey(element.getRecord());
	}

	@Override
	public byte[] serializeValue(RecordWrapperPOJO<T> element) {
		return keyedSerializationSchema.serializeValue(element.getRecord());

	}

	@Override
	public String getTargetTopic(RecordWrapperPOJO<T> element) {
		return element.getTopic();
	}
}

