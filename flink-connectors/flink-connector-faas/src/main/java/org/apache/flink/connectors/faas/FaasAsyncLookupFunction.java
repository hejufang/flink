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

package org.apache.flink.connectors.faas;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A {@link AsyncTableFunction} to query fields from Faas by keys.
 */
public class FaasAsyncLookupFunction extends AsyncTableFunction<Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(FaasAsyncLookupFunction.class);

	private final FaasLookupOptions faasLookupOptions;
	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final String[] lookupKeys;

	private transient FaasClient faasClient;
	private transient Cache<String, List<Row>> cache;

	public FaasAsyncLookupFunction(
			FaasLookupOptions faasLookupOptions,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			String[] lookupKeys) {
		this.faasLookupOptions = faasLookupOptions;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		this.lookupKeys = lookupKeys;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		if (faasLookupOptions.getCacheMaxSize() != -1 && faasLookupOptions.getCacheExpireMs() != -1) {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(faasLookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(faasLookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		faasClient = new FaasClient(fieldNames, fieldTypes, faasLookupOptions, lookupKeys);
		faasClient.open();
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param keys the lookup key.
	 */
	public void eval(CompletableFuture<Collection<Row>> result, Object... keys) {
		String requestUrl = faasClient.buildRequestUrl(keys);
		if (cache != null) {
			List<Row> cachedList = cache.getIfPresent(requestUrl);
			if (cachedList != null) {
				result.complete(cachedList);
				return;
			}
		}
		faasClient.doAsyncRequest(requestUrl, result, cache, 1);
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public void close() throws IOException {
		if (faasClient != null) {
			faasClient.close();
		}
	}
}
