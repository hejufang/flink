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

package org.apache.flink.connectors.bytable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.bytable.util.BytableReadWriteHelper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import com.bytedance.bytable.Client;
import com.bytedance.bytable.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.getBytableClient;

/**
 * A {@link TableFunction} to query fields from Bytable by keys.
 */
public class BytableLookupFunction extends TableFunction<Row> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(BytableLookupFunction.class);

	private final BytableTableSchema bytableTableSchema;
	private final BytableOption bytableOption;
	private final BytableLookupOptions bytableLookupOptions;

	private transient BytableReadWriteHelper readHelper;
	private transient Client bytableClient;
	private transient Table table;

	private transient Cache<Row, Row> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public BytableLookupFunction(
			BytableTableSchema bytableTableSchema,
			BytableOption bytableOption,
			BytableLookupOptions bytableLookupOptions) {
		this.bytableTableSchema = bytableTableSchema;
		this.bytableOption = bytableOption;
		this.bytableLookupOptions = bytableLookupOptions;

	}

	@Override
	public void open(FunctionContext context) {
		if (bytableLookupOptions.getCacheMaxSize() != -1 && bytableLookupOptions.getCacheExpireMs() != -1) {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(bytableLookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(bytableLookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());

		initBytableClient();
		this.readHelper = new BytableReadWriteHelper(bytableTableSchema);
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param keys the lookup key. Currently only support single rowkey.
	 */
	public void eval(Object... keys) {
		Row keyRow = Row.of(keys[0]);
		if (cache != null) {
			Row cachedRow = cache.getIfPresent(keyRow);
			if (cachedRow != null) {
				collect(cachedRow);
				return;
			}
		}
		Row row = null;
		for (int retry = 1; retry <= bytableLookupOptions.getMaxRetryTimes(); retry++) {
			lookupRequestPerSecond.markEvent();

			try {
				long startRequest = System.currentTimeMillis();
				row = readHelper.getReadResult(table, keys[0]);
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);

				if (cache != null) {
					cache.put(keyRow, row);
				}
				// break instead of return to make sure the result is collected outside this loop
				break;
			} catch (Exception e) {
				lookupFailurePerSecond.markEvent();

				LOG.error(String.format("Bytable execute read error, retry times = %d", retry), e);
				if (retry >= bytableLookupOptions.getMaxRetryTimes()) {
					throw new RuntimeException("Execution of Bytable read failed.", e);
				}
				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}
		if (row != null) {
			// should be outside of retry loop.
			// else the chained downstream exception will be caught.
			collect(row);
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return bytableTableSchema.convertsToTableSchema().toRowType();
	}

	/**
	 * Init bytable client and get table client from it.
	 * */
	private void initBytableClient() {
		this.bytableClient = getBytableClient(bytableOption);
		try {
			this.table = bytableClient.openTable(bytableOption.getTableName());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Open the table failed.", e);
		}
	}

	@Override
	public void close() {
		if (table != null) {
			table.close();
		}
		if (bytableClient != null) {
			bytableClient.close();
		}
	}
}
