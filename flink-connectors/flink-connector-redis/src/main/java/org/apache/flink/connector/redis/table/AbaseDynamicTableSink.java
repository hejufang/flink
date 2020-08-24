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
import org.apache.flink.connector.redis.utils.RedisUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

import com.bytedance.kvclient.ClientPool;
import com.bytedance.springdb.SpringDbPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import javax.annotation.Nullable;

/**
 * A {@link DynamicTableSink} for Abase.
 */
public class AbaseDynamicTableSink extends RedisDynamicTableSink {
	public AbaseDynamicTableSink(
			RedisOptions options,
			RedisInsertOptions insertOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		super(options, insertOptions, schema, encodingFormat);
	}

	@Override
	protected ClientPipelineProvider getClientPipelineProvider() {
		return new ClientPipelineProvider() {
			private static final long serialVersionUID = 1L;
			@Override
			public Pipeline createPipeline(ClientPool clientPool, Jedis jedis) {
				return ((SpringDbPool) clientPool).pipelined(jedis);
			}

			@Override
			public ClientPool createClientPool(RedisOptions redisOptions) {
				return RedisUtils.getAbaseClientPool(
					redisOptions.getCluster(),
					redisOptions.getPsm(),
					redisOptions.getTable(),
					redisOptions.getTimeout(),
					redisOptions.getMaxTotalConnections(),
					redisOptions.getMaxIdleConnections(),
					redisOptions.getMinIdleConnections()
				);
			}
		};
	}
}
