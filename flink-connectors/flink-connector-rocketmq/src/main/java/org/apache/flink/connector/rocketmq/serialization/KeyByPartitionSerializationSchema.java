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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * KeyByPartitionSerializationSchema.
 */
public class KeyByPartitionSerializationSchema extends KeyValueSerializationSchemaWrapper<RowData> {
	private final int[] partitionKeyByFields;
	private final RowData.FieldGetter[] keyFieldGetters;

	public KeyByPartitionSerializationSchema(
			SerializationSchema<RowData> serializationSchema,
			int[] keyByFields,
			TableSchema tableSchema) {
		super(serializationSchema);
		this.partitionKeyByFields = keyByFields;

		if (keyByFields != null) {
			keyFieldGetters = new RowData.FieldGetter[keyByFields.length];
			for (int i = 0; i < keyByFields.length; i++) {
				LogicalType type = tableSchema.getFieldDataType(keyByFields[i]).get().getLogicalType();
				if (!isSupportKeyType(type)) {
					throw new UnsupportedOperationException(String.format("%s is not supported as partition key", type));
				}

				keyFieldGetters[i] = RowData.createFieldGetter(
					type,
					keyByFields[i]);
			}
		} else {
			keyFieldGetters = null;
		}
	}

	@Override
	public String getPartitionKey(RowData tuple) {
		if (partitionKeyByFields == null) {
			return null;
		}

		return IntStream.range(0, partitionKeyByFields.length)
			.mapToObj(idx -> format(keyFieldGetters[idx].getFieldOrNull(tuple)))
			.collect(Collectors.joining());
	}

	public static String format(Object value) {
		if (value == null) {
			return "NULL";
		} else if (value instanceof byte[]) {
			return new String((byte[]) value);
		} else {
			return value.toString();
		}
	}

	public static boolean isSupportKeyType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
			case BIGINT:
			case INTERVAL_DAY_TIME:
			case FLOAT:
			case DOUBLE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return true;
			case TIMESTAMP_WITH_TIME_ZONE:
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case STRUCTURED_TYPE:
			case DISTINCT_TYPE:
			case RAW:
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
			default:
				return false;
		}
	}
}
