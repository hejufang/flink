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
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
	public void open() throws Exception {
		super.open();
		converter.open(RuntimeConverter.Context.create(AbaseLookupCollectionExecutor.class.getClassLoader()));
	}

	private Object getValueFromExternal(String key) {
		try {
			switch (normalOptions.getAbaseValueType()) {
				case LIST:
					List<String> list = client.lrange(key, 0, -1);
					if (list == null || list.isEmpty()) {
						return null;
					}
					return list.toArray(new String[0]);
				case SET:
					Set<String> set = client.smembers(key);
					if (set == null || set.isEmpty()) {
						return null;
					}
					return set.toArray(new String[0]);
				case ZSET:
					Set<String> zset = client.zrange(key, 0, -1);
					if (zset == null || zset.isEmpty()) {
						return null;
					}
					return zset.toArray(new String[0]);
				case HASH:
					Map<String, String> map = client.hgetAll(key);
					if (map == null || map.isEmpty()) {
						return null;
					}
					return map;
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported data type, currently supported type: %s",
							AbaseValueType.getCollectionStr()));
			}
		} catch (JedisDataException e) {
			switch (normalOptions.getAbaseValueType()) {
				case LIST:
					throw new FlinkRuntimeException(String.format("Collection Get value failed. Key : %s, " +
						"Related command: 'lrange key'.", key), e);
				case SET:
					throw new FlinkRuntimeException(String.format("Collection Get value failed. Key : %s, " +
						"Related command: 'smembers key'.", key), e);
				case ZSET:
					throw new FlinkRuntimeException(String.format("Collection Get value failed. Key : %s, " +
						"Related command: 'zrange key'.", key), e);
				case HASH:
					throw new FlinkRuntimeException(String.format("Collection Get value failed. Key : %s, " +
						"Related command: 'hgetAll key'.", key), e);
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported data type, currently supported type: %s",
							AbaseValueType.getCollectionStr()));
			}
		}
	}

	private RowData convertToRow(Object key, Object value) {
		Row row = new Row(2);
		row.setField(0, key.toString());
		row.setField(1, value);
		return (RowData) converter.toInternal(row);
	}
}
