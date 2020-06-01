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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
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

import static org.apache.flink.connectors.bytable.util.BytableConnectorUtils.closeFile;
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
		if (bytableLookupOptions.getCacheMaxSize() == -1 || bytableLookupOptions.getCacheExpireMs() == -1) {
			this.cache = CacheBuilder.newBuilder()
				.expireAfterWrite(bytableLookupOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
				.maximumSize(bytableLookupOptions.getCacheMaxSize())
				.recordStats()
				.build();
		}
		if (cache != null) {
			context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
		}
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

		for (int retry = 1; retry <= bytableLookupOptions.getMaxRetryTimes(); retry++) {
			try {
				Row row = readHelper.getReadResult(table, keys[0]);
				collect(row);
				if (cache != null) {
					cache.put(keyRow, row);
				}
				break;
			} catch (Exception e) {
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
			closeFile();
			bytableClient.close();
		}
	}
}
