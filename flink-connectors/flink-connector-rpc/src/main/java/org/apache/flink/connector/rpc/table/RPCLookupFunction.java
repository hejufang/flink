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

package org.apache.flink.connector.rpc.table;

import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.connector.rpc.table.executors.RPCLookupExecutor;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * An sync lookup function for reading from RPC server.
 */
public class RPCLookupFunction extends TableFunction<RowData> {
	private static final long serialVersionUID = 1L;
	private final RPCLookupExecutor rpcLookupExecutor;
	private final RPCLookupOptions rpcLookupOptions;
	private transient Cache<RowData, RowData> cache;

	public RPCLookupFunction(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		this.rpcLookupExecutor = new RPCLookupExecutor(
			rpcLookupOptions,
			rpcOptions,
			keyIndices,
			dataType,
			fieldNames
		);
		this.rpcLookupOptions = rpcLookupOptions;
	}

	public void open(FunctionContext context) throws Exception {
		if (rpcLookupOptions.getCacheMaxSize() != -1 && rpcLookupOptions.getCacheExpireMs() != -1) {
			cache = CacheBuilder.newBuilder()
				.expireAfterWrite(rpcLookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(rpcLookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		rpcLookupExecutor.open(context);
	}

	public void eval(Object... keys) {
		GenericRowData requestValue = GenericRowData.of(keys);
		if (cache != null) {
			RowData cachedRow = cache.getIfPresent(requestValue);
			if (cachedRow != null) {
				collect(cachedRow);
				return;
			}
		}
		RowData result = rpcLookupExecutor.doLookup(requestValue);
		if (result != null) {
			collect(result);
			// Actually rpc will never return a null response.
			// If emit-empty is enabled, null result will occur, for this scenario,
			// empty row should not be cached too.
			if (cache != null) {
				cache.put(requestValue, result);
			}
		}
	}

	@Override
	public void close() throws Exception {
		rpcLookupExecutor.close();
	}
}
