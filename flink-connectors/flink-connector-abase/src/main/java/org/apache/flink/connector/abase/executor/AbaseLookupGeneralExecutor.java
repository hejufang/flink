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
import org.apache.flink.connector.abase.utils.StringValueConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;

/**
 * value-type lookup executor supports: General.
 */
public class AbaseLookupGeneralExecutor extends AbaseLookupExecutor {

	private static final long serialVersionUID = 1L;

	private final StringValueConverters.StringValueConverter[] stringValueConverters;

	public AbaseLookupGeneralExecutor(
			AbaseNormalOptions normalOptions,
			DataType[] fieldTypes) {
		super(normalOptions);
		this.stringValueConverters = Arrays.stream(fieldTypes)
			.map(StringValueConverters::getConverter).toArray(StringValueConverters.StringValueConverter[]::new);
	}

	@Override
	public RowData doLookup(Object key) {
		Object value;
		try {
			value = client.get(key.toString());
		} catch (JedisDataException e) {
			throw new FlinkRuntimeException(String.format("General Get value failed. Key : %s, " +
				"Related command: 'get key'.", key), e);
		}
		if (value != null) {
			return convertToRow(key, value);
		} else {
			return null;
		}
	}

	private RowData convertToRow(Object key, Object value) {
		GenericRowData row = new GenericRowData(2);
		row.setField(0, stringValueConverters[0].toInternal(key.toString()));
		row.setField(1, stringValueConverters[1].toInternal(value.toString()));
		return row;
	}
}
