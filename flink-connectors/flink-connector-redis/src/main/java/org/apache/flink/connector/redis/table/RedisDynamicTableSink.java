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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.options.RedisInsertOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.ClientPipelineProvider;
import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.kvclient.ClientPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;


/**
 * A {@link DynamicTableSink} for Redis.
 */
public class RedisDynamicTableSink implements DynamicTableSink {
	private final RedisOptions options;
	private final RedisInsertOptions insertOptions;
	private final ClientPipelineProvider clientPipelineProvider;
	private final TableSchema schema;
	@Nullable
	protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

	public RedisDynamicTableSink(
			RedisOptions options,
			RedisInsertOptions insertOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.schema = schema;
		this.encodingFormat = encodingFormat;
		this.clientPipelineProvider = getClientPipelineProvider();
	}

	protected ClientPipelineProvider getClientPipelineProvider() {
		return new ClientPipelineProvider() {
			private static final long serialVersionUID = 1L;
			@Override
			public Pipeline createPipeline(ClientPool clientPool, Jedis jedis) {
				return jedis.pipelined();
			}

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
		};
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		validateChangelogMode(requestedMode);
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	private void validateChangelogMode(ChangelogMode requestedMode) {
		if (!requestedMode.equals(ChangelogMode.insertOnly())) {
			checkState(!options.getRedisValueType().isAppendOnly(),
				String.format("Upstream should be append only as %s cannot be updated.", options.getRedisValueType()));
			checkState(insertOptions.getMode() != RedisSinkMode.INCR,
				"Upstream should be append only when incr mode is on, " +
					"as incremental value cannot be updated.");
		}
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		TableSchema realSchema = schema;
		if (encodingFormat != null && insertOptions.isSkipFormatKey()) {
			TableSchema.Builder builder = new TableSchema.Builder();
			List<TableColumn> columns = schema.getTableColumns();
			columns.remove(0);
			columns.forEach(column -> builder.field(column.getName(), column.getType()));
			realSchema = builder.build();
		}
		final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
		RedisRowDataOutputFormat outputFormat = new RedisRowDataOutputFormat(
			options,
			insertOptions,
			rowType,
			clientPipelineProvider,
			encodingFormat == null ? null : encodingFormat.createRuntimeEncoder(context, realSchema.toRowDataType())
		);
		return SinkFunctionProvider.of(new RedisRowDataSinkFunction(
			outputFormat,
			insertOptions.getParallelism(),
			options.getRateLimiter()
		));
	}

	@Override
	public DynamicTableSink copy() {
		return new RedisDynamicTableSink(
			options,
			insertOptions,
			schema,
			encodingFormat
		);
	}

	@Override
	public String asSummaryString() {
		return options.getStorage();
	}
}
