/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.options.RedisInsertOptions;
import org.apache.flink.connector.redis.options.RedisLookupOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CLUSTER;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.PSM;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.TABLE;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.VALUE_TYPE;

/**
 * Factory for creating abase sink and lookup.
 */
public class AbaseDynamicTableSourceSinkFactory extends RedisDynamicTableSourceSinkFactory {
	private static final String IDENTIFIER = "abase";
	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CLUSTER);
		requiredOptions.add(TABLE);
		requiredOptions.add(PSM);
		return requiredOptions;
	}

	@Override
	protected void validateConfigOptions(ReadableConfig config) {
		Preconditions.checkState(config.get(VALUE_TYPE).equals(RedisValueType.GENERAL),
			String.format("Abase only support String data type now." +
				" Please don't configure %s.", VALUE_TYPE.key()));
		super.validateConfigOptions(config);
	}

	@Override
	protected RedisDynamicTableSource createRedisDynamicTableSource(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			TableSchema schema,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		return new AbaseDynamicTableSource(
			options,
			lookupOptions,
			schema,
			decodingFormat
		);
	}

	@Override
	protected RedisDynamicTableSink createRedisDynamicTableSink(
			RedisOptions options,
			RedisInsertOptions insertOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		return new AbaseDynamicTableSink(
			options,
			insertOptions,
			schema,
			encodingFormat
		);
	}
}
