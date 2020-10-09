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

package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.options.RedisInsertOptions;
import org.apache.flink.connector.redis.options.RedisLookupOptions;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.utils.RedisSinkMode;
import org.apache.flink.connector.redis.utils.RedisValueType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CLUSTER;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CONNECTION_MAX_IDLE_NUM;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CONNECTION_MAX_RETRIES;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CONNECTION_MAX_TOTAL_NUM;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CONNECTION_MIN_IDLE_NUM;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.PSM;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_MAX_RETRIES;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_MODE;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.SINK_RECORD_TTL_SECONDS;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.TABLE;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.VALUE_FORMAT_SKIP_KEY;
import static org.apache.flink.connector.redis.table.descriptors.RedisConfigs.VALUE_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_LOG_FAILURES_ONLY;

/**
 * Factory for creating configured instances of {@link RedisDynamicTableSource}.
 */
public class RedisDynamicTableSourceSinkFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
	private static final String IDENTIFIER = "redis";
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
			helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT)
				.orElse(null);
		helper.validate();
		validateConfigOptions(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		RedisOptions options = getRedisOptions(config);
		validateSchema(options, physicalSchema);
		return createRedisDynamicTableSink(
			options,
			getRedisInsertOptions(config),
			physicalSchema,
			encodingFormat
		);
	}

	protected RedisDynamicTableSink createRedisDynamicTableSink(
			RedisOptions options,
			RedisInsertOptions insertOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		return new RedisDynamicTableSink(
			options,
			insertOptions,
			schema,
			encodingFormat
		);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
			helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT)
				.orElse(null);
		helper.validate();
		validateConfigOptions(config);

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		RedisOptions options = getRedisOptions(config);
		validateSchema(options, physicalSchema);
		return createRedisDynamicTableSource(
			options,
			getRedisLookupOptions(config),
			physicalSchema,
			decodingFormat
		);
	}

	protected RedisDynamicTableSource createRedisDynamicTableSource(
			RedisOptions options,
			RedisLookupOptions lookupOptions,
			TableSchema schema,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		return new RedisDynamicTableSource(
			options,
			lookupOptions,
			schema,
			decodingFormat
		);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CLUSTER);
		requiredOptions.add(PSM);
		return requiredOptions;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.add(FORMAT);
		optionalOptions.add(VALUE_TYPE);
		optionalOptions.add(CONNECTION_TIMEOUT);
		optionalOptions.add(CONNECTION_MAX_RETRIES);
		optionalOptions.add(CONNECTION_MAX_TOTAL_NUM);
		optionalOptions.add(CONNECTION_MAX_IDLE_NUM);
		optionalOptions.add(CONNECTION_MIN_IDLE_NUM);
		optionalOptions.add(PARALLELISM);
		optionalOptions.add(SINK_MODE);
		optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
		optionalOptions.add(SINK_LOG_FAILURES_ONLY);
		optionalOptions.add(SINK_RECORD_TTL_SECONDS);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(VALUE_FORMAT_SKIP_KEY);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		return optionalOptions;
	}

	private RedisOptions getRedisOptions(ReadableConfig config) {
		return RedisOptions.builder()
			.setCluster(config.get(CLUSTER))
			.setTable(config.get(TABLE))
			.setStorage(config.get(CONNECTOR)).setPsm(config.get(PSM))
			.setTimeout((int) config.get(CONNECTION_TIMEOUT).toMillis())
			.setMinIdleConnections(config.get(CONNECTION_MIN_IDLE_NUM))
			.setMaxIdleConnections(config.get(CONNECTION_MAX_IDLE_NUM))
			.setMaxTotalConnections(config.get(CONNECTION_MAX_TOTAL_NUM))
			.setGetResourceMaxRetries(config.get(CONNECTION_MAX_RETRIES))
			.setRedisValueType(config.get(VALUE_TYPE))
			.build();
	}

	private RedisLookupOptions getRedisLookupOptions(ReadableConfig readableConfig) {
		return new RedisLookupOptions(
			readableConfig.get(LOOKUP_CACHE_MAX_ROWS),
			readableConfig.get(LOOKUP_CACHE_TTL),
			readableConfig.get(LOOKUP_MAX_RETRIES));
	}

	private RedisInsertOptions getRedisInsertOptions(ReadableConfig readableConfig) {
		RedisInsertOptions.RedisInsertOptionsBuilder builder = RedisInsertOptions.builder()
			.setFlushMaxRetries(readableConfig.get(SINK_MAX_RETRIES))
			.setMode(readableConfig.get(SINK_MODE))
			.setBufferMaxRows(readableConfig.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setBufferFlushInterval(readableConfig.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setLogFailuresOnly(readableConfig.get(SINK_LOG_FAILURES_ONLY))
			.setSkipFormatKey(readableConfig.get(VALUE_FORMAT_SKIP_KEY));
		readableConfig.getOptional(SINK_RECORD_TTL_SECONDS).ifPresent(builder::setTtlSeconds);
		readableConfig.getOptional(PARALLELISM).ifPresent(builder::setParallelism);
		return builder.build();
	}

	protected void validateConfigOptions(ReadableConfig config) {
		if (config.get(SINK_MODE).equals(RedisSinkMode.INCR)) {
			Preconditions.checkState(config.get(VALUE_TYPE).equals(RedisValueType.GENERAL)
					|| config.get(VALUE_TYPE).equals(RedisValueType.HASH),
				"INCR mode can only be used when value-type is STRING or HASH");
		}

		if (config.getOptional(FactoryUtil.FORMAT).isPresent() && config.getOptional(VALUE_TYPE).isPresent()) {
			throw new ValidationException("Can't configure format and value-type at the same time.");
		}

		checkAllOrNone(config, new ConfigOption[]{
			LOOKUP_CACHE_MAX_ROWS,
			LOOKUP_CACHE_TTL
		});
	}

	private void validateSchema(RedisOptions options, TableSchema schema) {
		if (options.getRedisValueType().equals(RedisValueType.HASH) && schema.getFieldCount() == 2) {
			Preconditions.checkState(schema.getFieldDataTypes()[1]
				.equals(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
				"Unsupported type for hash value, should be map<varchar, varchar>");
		}
	}

	private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
		int presentCount = 0;
		for (ConfigOption<?> configOption : configOptions) {
			if (config.getOptional(configOption).isPresent()) {
				presentCount++;
			}
		}
		String[] propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
		Preconditions.checkArgument(configOptions.length == presentCount || presentCount == 0,
			"Either all or none of the following options should be provided:\n" +
				String.join("\n", propertyNames));
	}
}
