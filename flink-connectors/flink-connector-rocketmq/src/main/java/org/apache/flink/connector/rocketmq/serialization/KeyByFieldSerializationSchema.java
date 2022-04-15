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

package org.apache.flink.connector.rocketmq.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * KeyByFieldSerializationSchema.
 */
public class KeyByFieldSerializationSchema extends KeyByPartitionSerializationSchema {
	private final int[] keyByFields;
	private final RowData.FieldGetter[] msgKeyFieldGetters;

	public KeyByFieldSerializationSchema(
			SerializationSchema<RowData> serializationSchema,
			int[] keyByFields,
			int[] partitionKeyByFields,
			TableSchema tableSchema) {
		super(serializationSchema, partitionKeyByFields, tableSchema);
		this.keyByFields = keyByFields;

		msgKeyFieldGetters = new RowData.FieldGetter[keyByFields.length];
		for (int i = 0; i < keyByFields.length; i++) {
			LogicalType type = tableSchema.getFieldDataType(keyByFields[i]).get().getLogicalType();
			if (!isSupportKeyType(type)) {
				throw new UnsupportedOperationException(String.format("%s is not supported as message key", type));
			}

			msgKeyFieldGetters[i] = RowData.createFieldGetter(
				type,
				keyByFields[i]);
		}
	}

	@Override
	public byte[] serializeKey(RowData tuple) {
		return IntStream.range(0, keyByFields.length)
			.mapToObj(idx -> format(msgKeyFieldGetters[idx].getFieldOrNull(tuple)))
			.collect(Collectors.joining(",")).getBytes(StandardCharsets.UTF_8);
	}
}
