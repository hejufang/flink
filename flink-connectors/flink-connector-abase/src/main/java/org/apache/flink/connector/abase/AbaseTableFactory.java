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
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.connector.abase.utils.Constants;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.PipelineOptions.JOB_PSM_PREFIX;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CLUSTER;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MAX_TOTAL_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_MIN_IDLE_NUM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.LOOKUP_SPECIFY_HASH_KEYS;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.PSM;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_IGNORE_DELETE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MAX_RETRIES;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_MODE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.SINK_RECORD_TTL;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.TABLE;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_FORMAT_SKIP_KEY;
import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.VALUE_TYPE;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.LOOKUP_CACHE_NULL_VALUE;
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
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory for creating configured instances of {@link AbaseTableSource} and {@link AbaseTableSink}.
 */
public class AbaseTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	private static final Logger LOG = LoggerFactory.getLogger(AbaseTableFactory.class);

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
			helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT).orElse(null);
		helper.validate();
		validateConfigOptions(config);

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		AbaseNormalOptions normalOptions = getAbaseNormalOptions(config, physicalSchema, getJobPSM(context.getConfiguration()));
		AbaseLookupOptions lookupOptions = getAbaseLookupOptions(config);
		validateSchema(normalOptions, lookupOptions, physicalSchema);
		return createAbaseTableSource(
			normalOptions,
			lookupOptions,
			physicalSchema,
			decodingFormat);
	}

	protected AbaseTableSource createAbaseTableSource(
			AbaseNormalOptions normalOptions,
			AbaseLookupOptions lookupOptions,
			TableSchema schema,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		return new AbaseTableSource(normalOptions, lookupOptions, schema, decodingFormat);
	}

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
		AbaseNormalOptions normalOptions = getAbaseNormalOptions(config, physicalSchema, getJobPSM(context.getConfiguration()));
		validateSchema(normalOptions, null, physicalSchema);
		return createAbaseTableSink(
			normalOptions,
			getAbaseSinkOptions(config),
			physicalSchema,
			encodingFormat
		);

	}

	protected AbaseTableSink createAbaseTableSink(
			AbaseNormalOptions options,
			AbaseSinkOptions insertOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		return new AbaseTableSink(
			options,
			insertOptions,
			schema,
			encodingFormat
		);
	}

	@Override
	public String factoryIdentifier() {
		return Constants.ABASE_IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CLUSTER);
		requiredOptions.add(TABLE);
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
		optionalOptions.add(SINK_RECORD_TTL);
		optionalOptions.add(SINK_MAX_RETRIES);
		optionalOptions.add(VALUE_FORMAT_SKIP_KEY);
		optionalOptions.add(SINK_IGNORE_DELETE);
		optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
		optionalOptions.add(LOOKUP_CACHE_TTL);
		optionalOptions.add(LOOKUP_MAX_RETRIES);
		optionalOptions.add(LOOKUP_LATER_JOIN_LATENCY);
		optionalOptions.add(LOOKUP_LATER_JOIN_RETRY_TIMES);
		optionalOptions.add(LOOKUP_CACHE_NULL_VALUE);
		optionalOptions.add(LOOKUP_ENABLE_INPUT_KEYBY);
		optionalOptions.add(PSM);
		optionalOptions.add(LOOKUP_SPECIFY_HASH_KEYS);
		optionalOptions.add(LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS);
		optionalOptions.add(RATE_LIMIT_NUM);
		return optionalOptions;
	}

	private AbaseNormalOptions getAbaseNormalOptions(ReadableConfig config, TableSchema physicalSchema, String jobPSM) {
		Optional<Integer> keyIndex = validateAndGetKeyIndex(config, physicalSchema);
		AbaseNormalOptions.AbaseOptionsBuilder builder = AbaseNormalOptions.builder()
			.setCluster(config.get(CLUSTER))
			.setTable(config.getOptional(TABLE).orElse(null))
			.setStorage(config.get(CONNECTOR))
			.setPsm(jobPSM)
			.setTimeout((int) config.get(CONNECTION_TIMEOUT).toMillis())
			.setMinIdleConnections(config.get(CONNECTION_MIN_IDLE_NUM))
			.setMaxIdleConnections(config.get(CONNECTION_MAX_IDLE_NUM))
			.setMaxTotalConnections(config.get(CONNECTION_MAX_TOTAL_NUM))
			.setGetResourceMaxRetries(config.get(CONNECTION_MAX_RETRIES))
			.setAbaseValueType(config.get(VALUE_TYPE));
		keyIndex.ifPresent(builder::setKeyIndex);
		config.getOptional(RATE_LIMIT_NUM).ifPresent(rate -> {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rate);
			builder.setRateLimiter(rateLimiter); });
		return builder.build();
	}

	private AbaseLookupOptions getAbaseLookupOptions(ReadableConfig config) {
		return new AbaseLookupOptions(
			config.get(LOOKUP_CACHE_MAX_ROWS),
			config.get(LOOKUP_CACHE_TTL).toMillis(),
			config.get(LOOKUP_MAX_RETRIES),
			config.get(LOOKUP_LATER_JOIN_LATENCY).toMillis(),
			config.get(LOOKUP_LATER_JOIN_RETRY_TIMES),
			config.get(LOOKUP_CACHE_NULL_VALUE),
			config.getOptional(LOOKUP_ENABLE_INPUT_KEYBY).orElse(null),
			config.get(LOOKUP_SPECIFY_HASH_KEYS),
			config.getOptional(LOOKUP_LATER_JOIN_REQUESTED_HASH_KEYS).orElse(null));
	}

	private AbaseSinkOptions getAbaseSinkOptions(ReadableConfig config) {
		AbaseSinkOptions.AbaseInsertOptionsBuilder builder = AbaseSinkOptions.builder()
			.setFlushMaxRetries(config.get(SINK_MAX_RETRIES))
			.setMode(config.get(SINK_MODE))
			.setBufferMaxRows(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setBufferFlushInterval(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis())
			.setLogFailuresOnly(config.get(SINK_LOG_FAILURES_ONLY))
			.setSkipFormatKey(config.get(VALUE_FORMAT_SKIP_KEY))
			.setIgnoreDelete(config.get(SINK_IGNORE_DELETE))
			.setParallelism(config.get(PARALLELISM))
			.setTtlSeconds((int) config.get(SINK_RECORD_TTL).getSeconds());
		return builder.build();
	}

	protected void validateConfigOptions(ReadableConfig config) {
		if (config.get(SINK_MODE).equals(AbaseSinkMode.INCR)) {
			checkState(config.get(VALUE_TYPE).equals(AbaseValueType.GENERAL)
					|| config.get(VALUE_TYPE).equals(AbaseValueType.HASH),
				"INCR mode can only be used when value-type is GENERAL or HASH");
		}

		if (config.getOptional(FactoryUtil.FORMAT).isPresent() && config.getOptional(VALUE_TYPE).isPresent()) {
			throw new ValidationException("Can't configure format and value-type at the same time.");
		}

		checkAllOrNone(config, new ConfigOption[]{
			LOOKUP_CACHE_MAX_ROWS,
			LOOKUP_CACHE_TTL
		});
	}

	private void validateSchema(
			AbaseNormalOptions options,
			@Nullable AbaseLookupOptions lookupOptions,
			TableSchema schema) {
		if (options.getAbaseValueType().equals(AbaseValueType.HASH) && schema.getFieldCount() == 2) {
			// Both sink and lookup should be checked.
			if (lookupOptions == null || !lookupOptions.isSpecifyHashKeys()) {
				checkState(schema.getFieldDataTypes()[1]
						.equals(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
					"Unsupported type for hash value, should be map<varchar, varchar>");
			}
		}

		// check whether lookup later join requested hash keys are in the schema
		if (lookupOptions != null && lookupOptions.isSpecifyHashKeys() &&
			lookupOptions.getRequestedHashKeys() != null && !lookupOptions.getRequestedHashKeys().isEmpty()) {
			Set<String> columns = Arrays.stream(schema.getFieldNames()).collect(Collectors.toSet());
			boolean valid = columns.containsAll(lookupOptions.getRequestedHashKeys());
			checkState(valid, "Requested hash keys are not in the schema!");
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

	private Optional<Integer> validateAndGetKeyIndex(
			ReadableConfig config,
			TableSchema physicalSchema) {
		String[] keyFields = physicalSchema.getPrimaryKey()
			.map(pk -> pk.getColumns().toArray(new String[0]))
			.orElse(null);
		if (keyFields != null) {
			checkState(keyFields.length == 1,
				"Abase can only accept one primary key.");
			// TODO: when format is not set, primary should be supported too.
			checkState(config.getOptional(FORMAT).isPresent(),
				"Currently, primary key can only be set when format is set.");
			return physicalSchema.getFieldNameIndex(keyFields[0]);
		}
		return Optional.empty();
	}

	private static String getJobPSM(ReadableConfig config) {
		String jobName = config.getOptional(PipelineOptions.NAME).orElse("UNKNOWN_JOB");
		String jobPSM = JOB_PSM_PREFIX + jobName;
		LOG.info("Get job psm: {}", jobPSM);
		return jobPSM;
	}
}
