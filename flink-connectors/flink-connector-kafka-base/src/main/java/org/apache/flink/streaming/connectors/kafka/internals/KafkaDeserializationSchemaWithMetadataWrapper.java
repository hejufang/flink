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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

/**
 * KafkaDeserializationSchemaWithMetadataWrapper.
 * @param <T>
 */
public abstract class KafkaDeserializationSchemaWithMetadataWrapper<T> implements KafkaDeserializationSchema<T> {
	private final DeserializationSchema<T> deserializationSchema;
	protected final TypeInformation<T> producerType;
	protected final Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;

	public KafkaDeserializationSchemaWithMetadataWrapper(
			DeserializationSchema<T> deserializationSchema,
			TypeInformation<T> typeInformation,
			Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
		this.deserializationSchema = deserializationSchema;
		this.producerType = typeInformation;
		this.metadataMap = metadataMap;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		KafkaDeserializationSchema.super.open(context);
		this.deserializationSchema.open(context);
	}

	@Override
	public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		T t = deserializationSchema.deserialize(record.value());
		if (t == null) {
			return null;
		}
		return addMetadata(t, record);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return deserializationSchema.isEndOfStream(nextElement);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return producerType;
	}

	public abstract T addMetadata(T element, ConsumerRecord<byte[], byte[]> record);
}
