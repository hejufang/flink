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

package org.apache.flink.connectors.bytesql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.bytesql.internal.ByteSQLRowConverter;
import org.apache.flink.connectors.bytesql.utils.ByteSQLUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import com.bytedance.infra.bytesql4j.ByteSQLDB;
import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;
import com.bytedance.infra.bytesql4j.proto.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link TableFunction} to query fields from ByteSQL by keys.
 */
public class ByteSQLLookupFunction extends TableFunction<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLLookupFunction.class);
	private final ByteSQLOptions options;
	private final ByteSQLLookupOptions lookupOptions;
	private final TypeInformation<?>[] keyTypes;
	private final String[] fieldNames;
	private final TypeInformation<?>[] fieldTypes;
	private final ByteSQLRowConverter rowConverter;
	private final String query;
	private transient ByteSQLDB byteSQLDB;
	private transient Cache<Row, List<Row>> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public ByteSQLLookupFunction(
			ByteSQLOptions options,
			ByteSQLLookupOptions lookupOptions,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes,
			String[] keyNames,
			RowType rowType) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
			.map(s -> {
				checkArgument(nameList.contains(s),
					"keyName %s can't find in fieldNames %s.", s, nameList);
				return fieldTypes[nameList.indexOf(s)];
			})
			.toArray(TypeInformation[]::new);
		this.query = ByteSQLUtils.getSelectFromStatement(
			options.getTableName(), fieldNames, keyNames);
		this.rowConverter = new ByteSQLRowConverter(rowType);
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		ByteSQLOption byteSQLOption = ByteSQLOption.build(
				options.getConsul(),
				options.getDatabaseName())
			.withUserName(options.getUsername())
			.withPassword(options.getPassword())
			.withRpcTimeoutMs(options.getConnectionTimeout());
		byteSQLDB = new ByteSQLDB(byteSQLOption);

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
		lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
		lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
		requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
	}

	public void eval(Object... keys) {
		Row keyRow = Row.of(keys);
		if (cache != null) {
			List<Row> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (Row cachedRow : cachedRows) {
					collect(cachedRow);
				}
				return;
			}
		}
		String realSQL;
		try {
			realSQL = ByteSQLUtils.generateActualSql(query, keyRow);
		} catch (ByteSQLException e) {
			throw new RuntimeException(String.format("Resolving parameters from %s failed for query: %s.",
				keyRow.toString(), query), e);
		}
		doLookup(realSQL, keyRow);
	}

	private void doLookup(String sql, Row keyRow) {
		for (int retry = 1; retry <= lookupOptions.getMaxRetryTimes(); retry++) {
			lookupRequestPerSecond.markEvent();

			try {
				long startRequest = System.currentTimeMillis();
				QueryResponse response = byteSQLDB.rawQuery(sql);
				long requestDelay = System.currentTimeMillis() - startRequest;
				requestDelayMs.update(requestDelay);
				List<Row> rows = ByteSQLUtils.convertResponseToRows(response, rowConverter);
				rows.forEach(this::collect);
				if (cache != null) {
					cache.put(keyRow, rows);
				}
				break;
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
	}

	@Override
	public void close() throws IOException {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		if (byteSQLDB != null) {
			byteSQLDB.close();
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}

}
