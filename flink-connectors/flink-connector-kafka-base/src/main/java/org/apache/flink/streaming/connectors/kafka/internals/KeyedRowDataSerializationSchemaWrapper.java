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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.io.Serializable;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;

/**
 * A simple wrapper for extracting and serialize key field from row data.
 */
public class KeyedRowDataSerializationSchemaWrapper extends KeyedSerializationSchemaWrapper<RowData> {

	private final RowData.FieldGetter keyFieldGetters;

	private RowDataSerializedKeyGetter serializedKeyGetter;

	private final LogicalTypeRoot[] sinkMsgKeyType;

	public KeyedRowDataSerializationSchemaWrapper(SerializationSchema serializationSchema, int keyIndex, LogicalType keyLogicalType) {
		super(serializationSchema);

		keyFieldGetters = RowData.createFieldGetter(keyLogicalType, keyIndex);

		sinkMsgKeyType = new LogicalTypeRoot[]{VARCHAR, BOOLEAN, DECIMAL, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE};

		if (keyLogicalType.getTypeRoot() == BINARY) {
			serializedKeyGetter = o -> (byte[]) keyFieldGetters.getFieldOrNull(o);
		} else {
			for (LogicalTypeRoot type :  sinkMsgKeyType){
				if (keyLogicalType.getTypeRoot() == type){
					serializedKeyGetter = o -> keyFieldGetters.getFieldOrNull(o).toString().getBytes();
					break;
				}
			}
		}

		if (serializedKeyGetter == null) {
			throw new IllegalArgumentException(String.format("Unsupported type for kafka sink message key field: %s.", keyLogicalType));
		}

	}

	@Override
	public byte[] serializeKey(RowData element) {
		return this.serializedKeyGetter.getSerializedKey(element);
	}

	@FunctionalInterface
	private interface RowDataSerializedKeyGetter extends Serializable {
		byte[] getSerializedKey(RowData o);
	}
}
