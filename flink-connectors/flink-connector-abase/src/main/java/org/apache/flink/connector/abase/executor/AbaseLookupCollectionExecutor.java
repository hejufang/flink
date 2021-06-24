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

import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * value-type lookup executor supports: List, Set, Zset, Hash(without specify-hash-key) .
 */
public class AbaseLookupCollectionExecutor extends AbaseLookupExecutor {

	private static final long serialVersionUID = 1L;

	private final DataStructureConverter converter;

	public AbaseLookupCollectionExecutor(
			AbaseNormalOptions normalOptions,
			DataStructureConverter converter) {
		super(normalOptions);
		this.converter = converter;
	}

	@Override
	public RowData doLookup(Object key) {
		Object value = getValueFromExternal(key.toString());
		if (value != null) {
			return convertToRow(key, value);
		} else {
			return null;
		}
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		converter.open(RuntimeConverter.Context.create(AbaseLookupCollectionExecutor.class.getClassLoader()));
	}

	private Object getValueFromExternal(String key) {
		try {
			switch (normalOptions.getAbaseValueType()) {
				case LIST:
					return abaseTable.lrange(key, 0, -1).toArray(new String[0]);
				case SET:
					return abaseTable.smembers(key).toArray(new String[0]);
				case ZSET:
					return abaseTable.zrange(key, 0, -1).toArray(new String[0]);
				case HASH:
					return abaseTable.hgetAll(key);
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported data type, currently supported type: %s",
							AbaseValueType.getCollectionStr()));
			}
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Get value failed. Key : %s, " +
				"Related command: 'get key'.", key), e);
		}
	}

	private RowData convertToRow(Object key, Object value) {
		Row row = new Row(2);
		row.setField(0, key.toString());
		row.setField(1, value);
		return (RowData) converter.toInternal(row);
	}
}
