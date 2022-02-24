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
import org.apache.flink.connector.rpc.table.executors.RPCBatchLookupExecutor;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.MiniBatchTableFunction;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A Batch lookup function for sending batch requests to the rpc service.
 */
public class RPCBatchedLookupFunction extends MiniBatchTableFunction<RowData> {
	private static final long serialVersionUID = 1L;
	private final RPCBatchLookupExecutor rpcLookupExecutor;
	private final RPCLookupOptions rpcLookupOptions;
	private transient Cache<RowData, RowData> cache;

	public RPCBatchedLookupFunction(
			RPCLookupOptions rpcLookupOptions,
			RPCOptions rpcOptions,
			int[] keyIndices,
			DataType dataType,
			String[] fieldNames) {
		this.rpcLookupExecutor = new RPCBatchLookupExecutor(
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

	@Override
	public List<Collection<RowData>> eval(List<Object[]> keySequenceList) {
		Collection[] result = new Collection[keySequenceList.size()];
		// Indices of result which cannot be found in the cache and should be lookuped from the server and then inserted
		// into the result.
		List<Integer> insertIndices = new ArrayList<>();
		List<RowData> lookupKeysList = new ArrayList<>();
		for (int i = 0; i < keySequenceList.size(); i++) {
			Object[] keys = keySequenceList.get(i);
			GenericRowData requestValue = GenericRowData.of(keys);
			if (cache != null) {
				RowData cachedRow = cache.getIfPresent(requestValue);
				if (cachedRow != null) {
					result[i] = Collections.singleton(cachedRow);
					continue;
				}
			}
			insertIndices.add(i);
			lookupKeysList.add(requestValue);
		}
		if (lookupKeysList.size() > 0) {
			List<RowData> lookupResult = rpcLookupExecutor.doLookup(lookupKeysList);
			if (lookupResult == null) {
				//failure occurs and is set to be ignored.
				return Collections.nCopies(keySequenceList.size(), null);
			}
			for (int i = 0; i < lookupKeysList.size(); i++) {
				result[insertIndices.get(i)] = Collections.singleton(lookupResult.get(i));
				if (cache != null) {
					cache.put(lookupKeysList.get(i), lookupResult.get(i));
				}
			}
		}
		return Arrays.asList(result);
	}

	@Override
	public int batchSize() {
		return rpcLookupOptions.getBatchSize();
	}
}
