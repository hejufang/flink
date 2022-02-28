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

package org.apache.flink.connector.abase.executor;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.KeyFormatterHelper;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.FlinkRuntimeException;

import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * schema lookup executor supports: json/pb and other formats.
 */
public class AbaseLookupSchemaExecutor extends AbaseLookupExecutor {

	private static final long serialVersionUID = 1L;

	private final DeserializationSchema<RowData> deserializationSchema; //for json/pb and other formats deserialization

	private final RowData.FieldGetter[] fieldGetters;

	public AbaseLookupSchemaExecutor(
			AbaseNormalOptions normalOptions,
			RowData.FieldGetter[] fieldGetters,
			@Nonnull DeserializationSchema<RowData> deserializationSchema) {
		super(normalOptions);
		this.deserializationSchema = deserializationSchema;
		this.fieldGetters = fieldGetters;
	}

	@Override
	public RowData doLookup(Object[] keys) throws IOException {
		byte[] value;
		try {
			String key = KeyFormatterHelper.formatKey(normalOptions.getKeyFormatter(), keys);
			value = client.get(key.getBytes());
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Schema Get value failed. Key : %s, " +
				"Related command: 'get key'.", keys[0]), e);
		}
		RowData row = null;
		if (value != null) {
			row = deserializationSchema.deserialize(value);
		}
		if (row == null) {
			return null;
		}
		int size = keys.length + row.getArity();
		List<Object> valueList = new ArrayList<>(size);
		int[] keyIndices = normalOptions.getKeyIndices();
		int keyIndex = 0;
		int valueIndex = 0;
		for (int i = 0; i < size; i++) {
			if (keyIndex < keyIndices.length && keyIndices[keyIndex] == i) {
				valueList.add(keys[keyIndex++]);
			} else {
				valueList.add(fieldGetters[valueIndex++].getFieldOrNull(row));
			}
		}
		return GenericRowData.of(valueList.toArray(new Object[0]));
	}

	public void open(FunctionContext context) throws Exception {
		super.open();
		deserializationSchema.open(() -> context.getMetricGroup().addGroup("user"));
	}

}
