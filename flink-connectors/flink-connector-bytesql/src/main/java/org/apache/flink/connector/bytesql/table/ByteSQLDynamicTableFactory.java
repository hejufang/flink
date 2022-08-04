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
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.metric.SinkMetricUtils;
import org.apache.flink.table.metric.SinkMetricsOptions;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.CONSUL;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.DATABASE;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.DB_CLASS;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.LOOKUP_ASYNC_CONCURRENCY;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.LOOKUP_ASYNC_ENABLED;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.PASSWORD;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.PRIMARY_KEY_FIELDS;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.SINK_IGNORE_NULL_COLUMNS;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.TABLE;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_TTL;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_LATENCY;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_LATER_JOIN_RETRY_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_IGNORE_DELETE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;
import static org.apache.flink.table.factories.FactoryUtil.SINK_MAX_RETRIES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_NUMBER;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SERIES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_BUCKET_SIZE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_NAME;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_EVENT_TS_WRITEABLE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_PROPS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_QUANTILES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_NAMES;
import static org.apache.flink.table.factories.FactoryUtil.SINK_METRICS_TAGS_WRITEABLE;
import static org.apache.flink.table.factories.FactoryUtil.SINK_RECORD_TTL;
import static org.apache.flink.table.utils.TableSchemaUtils.replacePrimaryKeyIfNotSpecified;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Factory for creating configured instances of {@link ByteSQLDynamicTableSource}
 * and {@link ByteSQLDynamicTableSink}.
 */
public class ByteSQLDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	private static final String IDENTIFIER = "bytesql";
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		ByteSQLOptions options = getByteSQLOptions(config);
		String primaryKeyFields = config.get(PRIMARY_KEY_FIELDS);
		TableSchema newSchema = replacePrimaryKeyIfNotSpecified(physicalSchema, primaryKeyFields);
		SinkMetricsOptions insertMetricsOptions = SinkMetricUtils.getSinkMetricsOptions(config, physicalSchema);
		ByteSQLInsertOptions insertOptions = getByteSQLInsertOptions(config, newSchema, insertMetricsOptions);
		return new ByteSQLDynamicTableSink(
			options,
			insertOptions,
			insertMetricsOptions,
			physicalSchema
		);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		helper.validate();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		ByteSQLOptions options = getByteSQLOptions(config);
		ByteSQLLookupOptions lookupOptions = getByteSQLLookupOptions(config);
		return new ByteSQLDynamicTableSource(
			options,
			lookupOptions,
			physicalSchema
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CONSUL);
		requiredOptions.add(DATABASE);
		requiredOptions.add(TABLE);
		requiredOptions.add(USERNAME);
		requiredOptions.add(PASSWORD);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(PARALLELISM);
		optionalOptions.add(CONNECTION_TIMEOUT);
		optionalOptions.add(DB_CLASS);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_LOG_FAILURES_ONLY);
		optionalOptions.add(SINK_IGNORE_NULL_COLUMNS);
		optionalOptions.add(SINK_IGNORE_DELETE);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(SINK_RECORD_TTL);

		optionalOptions.add(LOOKUP_ASYNC_ENABLED);
		optionalOptions.add(LOOKUP_ASYNC_CONCURRENCY);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(LOOKUP_LATER_JOIN_RETRY_TIMES);
		optionalOptions.add(LOOKUP_LATER_JOIN_LATENCY);
		optionalOptions.add(LOOKUP_ENABLE_INPUT_KEYBY);

		optionalOptions.add(PRIMARY_KEY_FIELDS);
		optionalOptions.add(RATE_LIMIT_NUM);

		optionalOptions.add(SINK_METRICS_QUANTILES);
		optionalOptions.add(SINK_METRICS_EVENT_TS_NAME);
		optionalOptions.add(SINK_METRICS_EVENT_TS_WRITEABLE);
		optionalOptions.add(SINK_METRICS_TAGS_NAMES);
		optionalOptions.add(SINK_METRICS_TAGS_WRITEABLE);
		optionalOptions.add(SINK_METRICS_PROPS);
		optionalOptions.add(SINK_METRICS_BUCKET_SIZE);
		optionalOptions.add(SINK_METRICS_BUCKET_NUMBER);
		optionalOptions.add(SINK_METRICS_BUCKET_SERIES);
		return optionalOptions;
	}

	private static ByteSQLOptions getByteSQLOptions(ReadableConfig configs) {
		FlinkConnectorRateLimiter rateLimiter = null;
		Optional<Long> rateLimitNum = configs.getOptional(RATE_LIMIT_NUM);
		if (rateLimitNum.isPresent()) {
			rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitNum.get());
		}
		return ByteSQLOptions.builder()
			.setConsul(configs.get(CONSUL))
			.setDatabaseName(configs.get(DATABASE))
			.setTableName(configs.get(TABLE))
			.setUsername(configs.get(USERNAME))
			.setPassword(configs.get(PASSWORD))
			.setDbClassName(configs.get(DB_CLASS))
			.setConnectionTimeout(configs.get(CONNECTION_TIMEOUT).toMillis())
			.setRateLimiter(rateLimiter)
			.build();
	}

	private static ByteSQLLookupOptions getByteSQLLookupOptions(ReadableConfig configs) {
		int asyncConcurrency = configs.get(LOOKUP_ASYNC_CONCURRENCY);
		if (configs.get(LOOKUP_ASYNC_ENABLED)) {
			checkArgument(asyncConcurrency > 1,
				"When async mode is on, concurrency should be greater than 1");
		}
		ByteSQLLookupOptions.Builder builder = ByteSQLLookupOptions.builder()
			.setCacheExpireMs(configs.get(LOOKUP_CACHE_TTL).toMillis())
			.setCacheMaxSize(configs.get(LOOKUP_CACHE_MAX_ROWS))
			.setMaxRetryTimes(configs.get(LOOKUP_MAX_RETRIES))
			.setLaterJoinRetryTimes(configs.get(LOOKUP_LATER_JOIN_RETRY_TIMES))
			.setLaterJoinLatency(configs.get(LOOKUP_LATER_JOIN_LATENCY).toMillis())
			.setAsync(configs.get(LOOKUP_ASYNC_ENABLED))
			.setAsyncConcurrency(asyncConcurrency);
		configs.getOptional(LOOKUP_ENABLE_INPUT_KEYBY).ifPresent(builder::setIsInputKeyByEnabled);
		return builder.build();
	}

	private static ByteSQLInsertOptions getByteSQLInsertOptions(
			ReadableConfig configs,
			TableSchema physicalSchema,
			SinkMetricsOptions metricsOptions) {
		String[] keyFields = physicalSchema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);
		int[] writableIndices = getWritableCols(physicalSchema.getFieldCount(), metricsOptions);

		// check if all primary keys are in the writableIndices.
		int[] primaryKeyIndices = physicalSchema.getPrimaryKeyIndices();
		if (primaryKeyIndices != null && primaryKeyIndices.length > 0) {
			Set<Integer> writableIndicesSet = Arrays.stream(writableIndices).boxed().collect(Collectors.toSet());
			for (int pk : primaryKeyIndices) {
				if (!writableIndicesSet.contains(pk)) {
					throw new FlinkRuntimeException("The primary key with index " + pk + " is not writable!");
				}
			}
		}
		return ByteSQLInsertOptions.builder()
			.setBufferFlushIntervalMills(configs.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setBufferFlushMaxRows(configs.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setMaxRetryTimes(configs.get(SINK_MAX_RETRIES))
			.setIgnoreNull(configs.get(SINK_IGNORE_NULL_COLUMNS))
			.setLogFailuresOnly(configs.get(SINK_LOG_FAILURES_ONLY))
			.setIgnoreDelete(configs.get(SINK_IGNORE_DELETE))
			.setTtlSeconds((int) configs.get(SINK_RECORD_TTL).getSeconds())
			.setParallelism(configs.get(PARALLELISM))
			.setKeyFields(keyFields)
			.setWritableColIndices(writableIndices)
			.setWritableNames(getWritableColNames(writableIndices, physicalSchema))
			.build();
	}

	private static int[] getWritableCols(int fieldCnt, SinkMetricsOptions metricsOptions) {
		int[] indices;
		if (!metricsOptions.isCollected()) {
			indices = new int[fieldCnt];
			for (int i = 0; i < fieldCnt; i++) {
				indices[i] = i;
			}
		} else {
			int[] tmp = new int[fieldCnt];
			int idx = 0;
			Set<Integer> tags = new HashSet<>();
			if (metricsOptions.getTagNameIndices() != null && !metricsOptions.getTagNameIndices().isEmpty()) {
				tags = new HashSet<>(metricsOptions.getTagNameIndices());
			}
			for (int i = 0; i < fieldCnt; i++) {
				if ((!metricsOptions.isEventTsWriteable() && i == metricsOptions.getEventTsColIndex())
					|| (!metricsOptions.isTagWriteable() && tags.contains(i))) {
					continue;
				}
				tmp[idx++] = i;
			}
			indices = Arrays.copyOf(tmp, idx);
		}
		return indices;
	}

	private static String[] getWritableColNames(int[] indices, TableSchema schema) {
		String[] names = new String[indices.length];
		String[] fieldNames = schema.getFieldNames();
		for (int i = 0; i < indices.length; i++) {
			names[i] = fieldNames[indices[i]];
		}
		return names;
	}

}