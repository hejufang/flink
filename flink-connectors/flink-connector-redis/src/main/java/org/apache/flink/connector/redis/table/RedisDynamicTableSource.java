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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import com.bytedance.kvclient.ClientPool;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

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
		return new ClientPoolProviderImpl();
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		DataType realDataType;
		if (options.getKeyIndex() >= 0) {
			int keyIndex = options.getKeyIndex();
			String keyName = schema.getFieldName(keyIndex).get();
			realDataType = schema
				.toPhysicalRowDataTypeWithFilter(tableColumn -> !tableColumn.getName().equals(keyName));
		} else {
			realDataType = schema.toRowDataType();
		}
		DataStructureConverter converter = context.createDataStructureConverter(realDataType);
		List<DataType> childrenType = realDataType.getChildren();
		RowData.FieldGetter[] fieldGetters = IntStream
			.range(0, childrenType.size())
			.mapToObj(pos -> RowData.createFieldGetter(childrenType.get(pos).getLogicalType(), pos))
			.toArray(RowData.FieldGetter[]::new);
		return TableFunctionProvider.of(new RedisRowDataLookupFunction(
			options,
			lookupOptions,
			schema.getFieldDataTypes(),
			schema.getFieldNames(),
			fieldGetters,
			clientPoolProvider,
			decodingFormat == null ? null : decodingFormat.createRuntimeDecoder(context, realDataType),
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
	public int getLaterJoinRetryTimes() {
		return lookupOptions.getLaterRetryTimes();
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.schema = TableSchemaUtils.projectSchema(schema, projectedFields);
	}

	@Override
	public long getLaterJoinMs() {
		return lookupOptions.getLaterRetryMs();
	}

	private static class ClientPoolProviderImpl implements ClientPoolProvider {
		private static final long serialVersionUID = 1L;
		@Override
		public ClientPool createClientPool(RedisOptions redisOptions) {
			return RedisUtils.getRedisClientPool(
				redisOptions.getCluster(),
				redisOptions.getPsm(),
				redisOptions.getTimeout(),
				redisOptions.getMaxTotalConnections(),
				redisOptions.getMaxIdleConnections(),
				redisOptions.getMinIdleConnections()
			);
		}
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(lookupOptions.isInputKeyByEnabled());
	}
}
