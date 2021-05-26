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

package org.apache.flink.streaming.connectors.rocketmq.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Wrap a SerializationSchema to KeyValueDeserializationSchema.
 * The key is the joined keyBy fields in Row.
 */
public class KeyBySerializationSchemaWrapper implements KeyValueSerializationSchema<Row> {

	private final int[] keyByFieldIndexes;
	private final SerializationSchema<Row> serializationSchema;
	private final String[] reusableKeyArray;

	public KeyBySerializationSchemaWrapper(
			int[] keyByFieldIndexes,
			SerializationSchema<Row> serializationSchema) {
		Preconditions.checkState(keyByFieldIndexes != null && keyByFieldIndexes.length > 0,
			"keyByFieldIndexes cannot be null or empty!");
		this.keyByFieldIndexes = keyByFieldIndexes;
		this.serializationSchema = serializationSchema;
		this.reusableKeyArray = new String[keyByFieldIndexes.length];
	}

	@Override
	public byte[] serializeKey(Row row) {
		for (int i = 0; i < keyByFieldIndexes.length; i++) {
			int keyFieldIndex = keyByFieldIndexes[i];
			reusableKeyArray[i] = row.getField(keyFieldIndex).toString();
		}

		return String.join(",", reusableKeyArray).getBytes();
	}

	@Override
	public byte[] serializeValue(Row row) {
		return serializationSchema.serialize(row);
	}
}
