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
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.CONSUL;
import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.DATABASE;
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
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;
import static org.apache.flink.table.factories.FactoryUtil.SINK_MAX_RETRIES;
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
		ByteSQLInsertOptions insertOptions = getByteSQLInsertOptions(config, newSchema);
		return new ByteSQLDynamicTableSink(
			options,
			insertOptions,
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
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_LOG_FAILURES_ONLY);
		optionalOptions.add(SINK_IGNORE_NULL_COLUMNS);
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

	private static ByteSQLInsertOptions getByteSQLInsertOptions(ReadableConfig configs, TableSchema physicalSchema) {
		String[] keyFields = physicalSchema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);
		return ByteSQLInsertOptions.builder()
			.setBufferFlushIntervalMills(configs.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setBufferFlushMaxRows(configs.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setMaxRetryTimes(configs.get(SINK_MAX_RETRIES))
			.setIgnoreNull(configs.get(SINK_IGNORE_NULL_COLUMNS))
			.setLogFailuresOnly(configs.get(SINK_LOG_FAILURES_ONLY))
			.setTtlSeconds((int) configs.get(SINK_RECORD_TTL).getSeconds())
			.setParallelism(configs.get(PARALLELISM))
			.setKeyFields(keyFields)
			.build();
	}

}
