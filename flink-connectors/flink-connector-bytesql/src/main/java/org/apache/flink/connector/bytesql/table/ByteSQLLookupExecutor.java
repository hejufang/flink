/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.bytesql.internal.ByteSQLRowConverter;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import com.bytedance.infra.bytesql4j.ByteSQLDB;
import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;
import com.bytedance.infra.bytesql4j.proto.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A query executor for lookupFunctions reading data from ByteSQL.
 */
public class ByteSQLLookupExecutor implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLLookupExecutor.class);
	private final ByteSQLOptions options;
	private final ByteSQLLookupOptions lookupOptions;
	private final ByteSQLRowConverter rowConverter;
	private final String query;
	private final RowData.FieldGetter[] fieldGetters;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient ByteSQLDB byteSQLDB;
	private transient Cache<RowData, List<RowData>> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public ByteSQLLookupExecutor(
			ByteSQLOptions options,
			ByteSQLLookupOptions lookupOptions,
			List<String> keyNames,
			RowType rowType) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		List<String> fieldNames = rowType.getFieldNames();
		this.query = ByteSQLUtils.getSelectFromStatement(
			options.getTableName(), fieldNames, keyNames);
		this.rowConverter = new ByteSQLRowConverter(rowType);
		this.fieldGetters = IntStream
			.range(0, fieldNames.size())
			.mapToObj(pos -> RowData.createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
		this.rateLimiter = options.getRateLimiter();
	}

	public void init(FunctionContext context) {
		ByteSQLOption byteSQLOption = ByteSQLOption.build(
			options.getConsul(),
			options.getDatabaseName())
			.withUserName(options.getUsername())
			.withPassword(options.getPassword())
			.withRpcTimeoutMs(options.getConnectionTimeout());
		byteSQLDB = ByteSQLDB.newInstance(byteSQLOption);

		if (lookupOptions.getCacheMaxSize() == -1 || lookupOptions.getCacheExpireMs() == -1) {
			this.cache = null;
		} else {
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
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
	}

	public List<RowData> doLookup(Object... inputs) {
		final GenericRowData keyRow = GenericRowData.of(inputs);
		// As "IS NOT DISTINCT FROM" is not supported yet, all records when there is any nulls on the join key
		// have been filtered in generated code for lookup function.
		// However when it's supported in the future, this check should be done here as in present implement,
		// ByteSQL source is not going to accept a null key.
		if (Arrays.asList(inputs).contains(null)) {
			throw new IllegalArgumentException(String.format(
				"Lookup key %s contains null value, which should not happen.", keyRow));
		}
		if (cache != null) {
			List<RowData> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				return cachedRows;
			}
		}
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		String realSQL;
		try {
			realSQL = ByteSQLUtils.generateActualSql(query, keyRow, fieldGetters);
		} catch (ByteSQLException e) {
			throw new RuntimeException(String.format("Resolving parameters from %s failed for query: %s.",
				keyRow.toString(), query), e);
		}
		for (int retry = 1; retry <= lookupOptions.getMaxRetryTimes(); retry++) {
			lookupRequestPerSecond.markEvent();
			try {
				long startRequest = System.currentTimeMillis();
				QueryResponse response = byteSQLDB.rawQuery(realSQL);
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);
				List<RowData> rows = ByteSQLUtils.convertResponseToRows(response, rowConverter);
				if (cache != null) {
					cache.put(keyRow, rows);
				}
				return rows;
			} catch (ByteSQLException e) {
				lookupFailurePerSecond.markEvent();
				LOG.error(String.format("ByteSQL execute error, retry times = %d", retry), e);
				if (retry >= lookupOptions.getMaxRetryTimes() || e instanceof DuplicatedEntryException) {
					throw new RuntimeException("Execution of ByteSQL statement failed.", e);
				}
			} catch (SQLException e) {
				throw new RuntimeException("Type in Flink query is not compatible with type in ByteSQL.", e);
			}
		}
		return Collections.emptyList();
	}

	public void close() {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		if (byteSQLDB != null) {
			byteSQLDB.close();
		}
	}

}
