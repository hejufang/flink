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
import org.apache.flink.table.data.RowData;

import com.bytedance.kvclient.ClientPool;

/**
 * A {@link DynamicTableSource} for Abase.
 */
public class AbaseDynamicTableSource extends RedisDynamicTableSource {
	public AbaseDynamicTableSource(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			TableSchema schema,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		super(options, lookupOptions, schema, decodingFormat);
	}

	@Override
	protected ClientPoolProvider getClientPoolProvider() {
		return new ClientPoolProvider() {
			private static final long serialVersionUID = 1L;
			@Override
			public ClientPool createClientPool(RedisOptions options) {
				return RedisUtils.getAbaseClientPool(
					options.getCluster(),
					options.getPsm(),
					options.getTable(),
					options.getTimeout(),
					options.getMaxTotalConnections(),
					options.getMaxIdleConnections(),
					options.getMinIdleConnections()
				);
			}
		};
	}
}
