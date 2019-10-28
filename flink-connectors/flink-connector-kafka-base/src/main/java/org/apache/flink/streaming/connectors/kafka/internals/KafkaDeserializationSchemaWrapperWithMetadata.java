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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A wrapper for using DeserializationSchema with KafkaDeserializationSchema, and with kafka schema messages.
 * @param <T> The type created by the deserialization schema.
 */
public class KafkaDeserializationSchemaWrapperWithMetadata<T> implements KafkaDeserializationSchema<Tuple2<ConsumerRecord, T>> {

	private final DeserializationSchema<T> deserializationSchema;

	public KafkaDeserializationSchemaWrapperWithMetadata(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public boolean isEndOfStream(Tuple2<ConsumerRecord, T> nextElement) {
		return deserializationSchema.isEndOfStream(nextElement.f1);
	}

	@Override
	public Tuple2<ConsumerRecord, T> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		return Tuple2.of(record, deserializationSchema.deserialize(record.value()));
	}

	@Override
	public TypeInformation<Tuple2<ConsumerRecord, T>> getProducedType() {
		return new TupleTypeInfo(TypeInformation.of(ConsumerRecord.class), deserializationSchema.getProducedType());
	}
}
