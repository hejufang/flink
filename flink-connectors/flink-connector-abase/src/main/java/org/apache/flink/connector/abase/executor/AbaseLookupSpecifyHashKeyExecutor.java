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
import org.apache.flink.connector.abase.utils.KeyFormatterHelper;
import org.apache.flink.connector.abase.utils.StringValueConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * value-type lookup executor supports: Hash(with specify-hash-key).
 */
public class AbaseLookupSpecifyHashKeyExecutor extends AbaseLookupExecutor {

	private static final long serialVersionUID = 1L;

	private final StringValueConverters.StringValueConverter[] stringValueConverters;

	private final String[] fieldNames;
	//use for hash value-type
	private final String[] hashKeys;

	private final int[] requestedHashKeysIndex;

	private final boolean checkRequestedHashKeys;

	public AbaseLookupSpecifyHashKeyExecutor(
			AbaseNormalOptions normalOptions,
			String[] fieldNames,
			DataType[] fieldTypes,
			List<String> requestedHashKeys) {
		super(normalOptions);
		this.fieldNames = fieldNames;
		this.hashKeys = getHashKeys(fieldNames, normalOptions.getValueIndices());
		this.stringValueConverters = Arrays.stream(fieldTypes)
			.map(StringValueConverters::getConverter).toArray(StringValueConverters.StringValueConverter[]::new);
		if (requestedHashKeys != null && !requestedHashKeys.isEmpty()) {
			List<String> keyList = Arrays.stream(hashKeys).collect(Collectors.toList());
			requestedHashKeysIndex = requestedHashKeys.stream()
				.mapToInt(keyList::indexOf)
				.toArray();
		} else {
			requestedHashKeysIndex = null;
		}

		checkRequestedHashKeys = (requestedHashKeysIndex != null && requestedHashKeysIndex.length > 0);
	}

	@Override
	public RowData doLookup(Object[] keys) {
		String key = KeyFormatterHelper.formatKey(normalOptions.getKeyFormatter(), keys);
		String[] values = getValuesFromExternal(key);
		if (values == null) {
			return null;
		}
		GenericRowData rowData = new GenericRowData(fieldNames.length);
		for (int i = 0; i < normalOptions.getKeyIndices().length; i++) {
			int idx = normalOptions.getKeyIndices()[i];
			rowData.setField(idx, keys[i]);
		}
		for (int i = 0; i < normalOptions.getValueIndices().length; i++) {
			int idx = normalOptions.getValueIndices()[i];
			rowData.setField(idx, stringValueConverters[idx].toInternal(values[i]));
		}
		return rowData;
	}

	private String[] getValuesFromExternal(String key) {
		List<String> values;
		try {
			values = client.hmget(key, hashKeys);
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("Specify-Hash-Key Get value failed. Key : %s, " +
				"Related command: 'hmget key'.", key), e);
		}
		if (isEmpty(values)) {
			return null;
		}
		String[] arr = values.toArray(new String[0]);
		if (checkRequestedHashKeys) {
			for (int i : requestedHashKeysIndex) {
				if (StringUtils.isNullOrWhitespaceOnly(arr[i])) {
					return null;
				}
			}
		}
		return arr;
	}

	private static String[] getHashKeys(String[] fieldNames, int[] idx) {
		String[] hashKeys = new String[idx.length];
		for (int i = 0; i < idx.length; i++) {
			hashKeys[i] = fieldNames[idx[i]];
		}
		return hashKeys;
	}

	private static boolean isEmpty(List<String> values) {
		if (values == null || values.isEmpty()) {
			return true;
		}
		for (String val : values) {
			if (val != null) {
				return false;
			}
		}
		return true;
	}
}