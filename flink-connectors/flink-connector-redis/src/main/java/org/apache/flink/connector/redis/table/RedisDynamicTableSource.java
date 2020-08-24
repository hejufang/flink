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

package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.options.RedisLookupOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPoolProvider;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A {@link DynamicTableSource} for Redis.
 */
public class RedisDynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {
	private final RedisOptions options;
	private final RedisLookupOptions lookupOptions;
	private TableSchema schema;
	protected ClientPoolProvider clientPoolProvider;
	@Nullable
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	public RedisDynamicTableSource(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			TableSchema schema,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
		this.decodingFormat = decodingFormat;
		this.clientPoolProvider = getClientPoolProvider();
	}

	protected ClientPoolProvider getClientPoolProvider() {
		return (ClientPoolProvider & Serializable) (redisOptions) -> RedisUtils.getRedisClientPool(
			redisOptions.getCluster(),
			redisOptions.getPsm(),
			redisOptions.getTimeout(),
			redisOptions.getMaxTotalConnections(),
			redisOptions.getMaxIdleConnections(),
			redisOptions.getMinIdleConnections()
		);
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		Preconditions.checkArgument(context.getKeys().length == 1,
			"Redis/Abase only supports one lookup key");
		DataStructureConverter converter = context.createDataStructureConverter(schema.toPhysicalRowDataType());
		return TableFunctionProvider.of(new RedisRowDataLookupFunction(
			options,
			lookupOptions,
			schema.getFieldDataTypes(),
			clientPoolProvider,
			decodingFormat == null ? null : decodingFormat.createRuntimeDecoder(context, schema.toRowDataType()),
			converter));
	}

	@Override
	public DynamicTableSource copy() {
		return new RedisDynamicTableSource(
			options,
			lookupOptions,
			schema,
			decodingFormat
		);
	}

	@Override
	public String asSummaryString() {
		return options.getStorage();
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.schema = TableSchemaUtils.projectSchema(schema, projectedFields);
	}
}
