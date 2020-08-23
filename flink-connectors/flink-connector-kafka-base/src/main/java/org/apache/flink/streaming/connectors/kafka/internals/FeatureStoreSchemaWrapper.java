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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.types.Row;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;

/**
 * Feature store wrapper for kafka schema.
 */
@Internal
public class FeatureStoreSchemaWrapper extends KafkaDeserializationSchemaWrapper<Row> {
	public FeatureStoreSchemaWrapper(DeserializationSchema<Row> deserializationSchema) {
		super(deserializationSchema);
	}

	@Override
	public Row deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		int len = record.colNames().length;
		Row row = new Row(len);
		for (int i = 0; i < len; i++) {
			Object colValue = record.colValues()[i];
			if (colValue instanceof ByteBuffer) {
				row.setField(i, getBytesFromBuffer((ByteBuffer) colValue));
			} else if (colValue instanceof Object[]) {
				Object[] rawColList = (Object[]) colValue;
				byte[][] bytes = new byte[rawColList.length][];
				for (int j = 0; j < rawColList.length; j++) {
					bytes[j] = getBytesFromBuffer((ByteBuffer) rawColList[j]);
				}
				row.setField(i, bytes);
			}
		}
		return row;
	}

	private byte[] getBytesFromBuffer(ByteBuffer byteBuffer) {
		byte[] bytes = new byte[byteBuffer.remaining()];
		byteBuffer.get(bytes);
		return bytes;
	}
}
