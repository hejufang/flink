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

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.abase.executor.AbaseLookupExecutor;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A lookup function for {@link AbaseTableSource}.
 */
public class AbaseLookupFunction extends TableFunction<RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(AbaseLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private static final RowData EMPTY_ROW = new GenericRowData(0);

	private transient Cache<RowData, RowData> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	private final int[] pkRuntimeIdx;

	private final AbaseLookupOptions lookupOptions;

	private final FlinkConnectorRateLimiter rateLimiter;

	private final AbaseLookupExecutor abaseLookupExecutor;

	public AbaseLookupFunction(
			int[] pkRuntimeIdx,
			AbaseNormalOptions normalOptions,
			AbaseLookupOptions lookupOptions,
			AbaseLookupExecutor abaseLookupExecutor) {
		this.pkRuntimeIdx = pkRuntimeIdx;
		this.lookupOptions = lookupOptions;
		this.abaseLookupExecutor = abaseLookupExecutor;
		this.rateLimiter = normalOptions.getRateLimiter();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		if (lookupOptions.getCacheMaxSize() != -1 && lookupOptions.getCacheExpireMs() != -1) {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(lookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(lookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		if (rateLimiter != null) {
			rateLimiter.open(context.getRuntimeContext());
		}
		abaseLookupExecutor.open();
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
	}

	@Override
	public void close() throws Exception {
		abaseLookupExecutor.close();
	}

	/**
	 * The invoke entry point of lookup function.
	 *
	 * @param keys the lookup key. Currently only support single rowkey.
	 * @throws Exception
	 */
	public void eval(Object... keys) throws Exception {
		keys = filterPrimaryKeys(keys);  // reserve lookup keys that are also primary keys and ignore others
		RowData keyRow = GenericRowData.of(keys);
		if (cache != null) {
			RowData cacheRow = cache.getIfPresent(keyRow);
			if (cacheRow != null) {
				if (cacheRow.getArity() > 0) {
					collect(cacheRow);
				}
				return;
			}
		}
		RowData row = null;
		for (int retry = 1; retry <= lookupOptions.getMaxRetryTimes(); retry++) {
			try {
				if (rateLimiter != null) {
					rateLimiter.acquire(1);
				}
				lookupRequestPerSecond.markEvent();
				long startRequest = System.currentTimeMillis();

				// do lookup in executor.
				row = abaseLookupExecutor.doLookup(keys);

				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);

				if (cache != null && (row != null || lookupOptions.isCacheNull())) {
					cache.put(keyRow, row == null ? EMPTY_ROW : row);
				}
				// break instead of return to make sure the result is collected outside this loop
				break;
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();
				LOG.error("Abase execute read error, retry times = {}", retry, e);
				if (retry >= lookupOptions.getMaxRetryTimes()) {
					throw new RuntimeException("Execution of Abase read failed.", e);
				}
				try {
					Thread.sleep(1000L * retry);
				} catch (InterruptedException ie) {
					throw new RuntimeException(ie);
				}
			}
		}
		if (row != null) {
			// should be outside of retry loop.
			// else the chained downstream exception will be caught.
			collect(row);
		}
	}

	private Object[] filterPrimaryKeys(Object[] keys) {
		Object[] val = new Object[pkRuntimeIdx.length];
		int i = 0;
		for (int pos : pkRuntimeIdx) {   // order are preserved
			val[i++] = keys[pos];
		}
		return val;
	}

}
