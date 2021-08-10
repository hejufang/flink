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

package org.apache.flink.connectors.redis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.connectors.util.RedisDataType;
import org.apache.flink.connectors.util.RedisMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;
import org.apache.flink.table.descriptors.RedisValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.PropertyUtils;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.util.Constant.INCR_MODE;
import static org.apache.flink.connectors.util.Constant.INSERT_MODE;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_HASH;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_LIST;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_SET;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_STRING;
import static org.apache.flink.connectors.util.Constant.REDIS_DATATYPE_ZSET;
import static org.apache.flink.connectors.util.Constant.REDIS_INCR_VALID_DATATYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_KEY_FIELDS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_LOOKUP_CACHE_NULL_VALUE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_NAME;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_BUFFER_FLUSH_INTERVAL_MS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_TYPE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_FLUSH_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_FORCE_CONNECTION_SETTINGS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_GET_RESOURCE_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LATER_JOIN_LATENCY_MS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOG_FAILURES_ONLY;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_SPECIFY_HASH_KEYS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_IDLE_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_TOTAL_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MIN_IDLE_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MODE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PSM;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_RATE_LIMIT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_SKIP_FORMAT_KEY;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TTL_SECONDS;
import static org.apache.flink.table.descriptors.RedisValidator.REDIS;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link RedisUpsertTableSink}.
 */
public class RedisTableFactory implements StreamTableSourceFactory<Row>,
		StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, REDIS); // redis
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_PARALLELISM);
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_PSM);
		properties.add(CONNECTOR_MODE);
		properties.add(CONNECTOR_DATA_TYPE);
		properties.add(CONNECTOR_GET_RESOURCE_MAX_RETRIES);
		properties.add(CONNECTOR_FLUSH_MAX_RETRIES);
		properties.add(CONNECTOR_BATCH_SIZE);
		properties.add(CONNECTOR_BUFFER_FLUSH_INTERVAL_MS);
		properties.add(CONNECTOR_TTL_SECONDS);
		properties.add(CONNECTOR_TIMEOUT_MS);
		properties.add(CONNECTOR_FORCE_CONNECTION_SETTINGS);
		properties.add(CONNECTOR_MAX_TOTAL_CONNECTIONS);
		properties.add(CONNECTOR_MAX_IDLE_CONNECTIONS);
		properties.add(CONNECTOR_MIN_IDLE_CONNECTIONS);
		properties.add(CONNECTOR_LOG_FAILURES_ONLY);
		properties.add(CONNECTOR_SKIP_FORMAT_KEY);
		properties.add(CONNECTOR_LOOKUP_CACHE_NULL_VALUE);
		properties.add(CONNECTOR_RATE_LIMIT);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		//lookup option
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);
		properties.add(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY);
		properties.add(CONNECTOR_LATER_JOIN_LATENCY_MS);
		properties.add(CONNECTOR_KEY_FIELDS);
		properties.add(CONNECTOR_LOOKUP_SPECIFY_HASH_KEYS);

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	/**
	 * attention: only support redis lookup source currently.
	 * @param properties normalized properties describing a stream table source.
	 */
	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		DeserializationSchema<Row> deserializationSchema = null;
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		if (properties.containsKey(FormatDescriptorValidator.FORMAT_TYPE)) {
			if (properties.containsKey(CONNECTOR_DATA_TYPE)) {
				throw new FlinkRuntimeException("Can't configure the format.type and " +
					"connector.redis-data-type at the same time.");
			}
			if (properties.containsKey(CONNECTOR_KEY_FIELDS)) {
				String keyFieldName = properties.get(CONNECTOR_KEY_FIELDS);
				Map<String, String> newProperties = new HashMap<>(properties);
				tableSchema.getFieldNameIndex(keyFieldName).ifPresent(
					fieldIndex ->
						PropertyUtils.removeFieldProperties(newProperties, Collections.singletonList(fieldIndex))
				);
				properties = newProperties;
			}
			deserializationSchema = TableConnectorUtils.getDeserializationSchema(properties,
				this.getClass().getClassLoader());
		}

		RedisOptions redisOptions = getRedisOptions(descriptorProperties);
		RedisLookupOptions redisLookupOptions = getRedisLookupOptions(descriptorProperties);
		validateOptions(redisOptions, redisLookupOptions);
		return RedisTableSource.builder()
				.setOptions(redisOptions)
				.setLookupOptions(redisLookupOptions)
				.setSchema(tableSchema)
				.setDeserializationSchema(deserializationSchema)
				.build();
	}

	private void validateOptions(RedisOptions redisOptions, RedisLookupOptions redisLookupOptions) {
		if (redisLookupOptions.isSpecifyHashKeys() && redisOptions.getRedisDataType() != RedisDataType.HASH) {
			throw new ValidationException("Option 'lookup.specify-hash-keys' is only supported when 'connector.redis-data-type' is 'hash'");
		}
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		RedisOutputFormat.RedisOutputFormatBuilder builder =
			RedisOutputFormat.buildRedisOutputFormat();
		if (properties.containsKey(FormatDescriptorValidator.FORMAT_TYPE)) {
			Map<String, String> serializationProperties = getSerializationProperties(properties);
			final SerializationSchema<Row> serializationSchema = TableConnectorUtils
				.getSerializationSchema(serializationProperties, this.getClass().getClassLoader());
			builder.setSerializationSchema(serializationSchema);
		} else if (properties.containsKey(FormatDescriptorValidator.FORMAT_TYPE)
			&& properties.containsKey(CONNECTOR_DATA_TYPE)) {
			throw new FlinkRuntimeException("Can't configure the format.type and " +
				"connector.redis-data-type at the same time.");
		} else if (INCR_MODE.equalsIgnoreCase(properties.get(CONNECTOR_MODE)) &&
			!REDIS_INCR_VALID_DATATYPE.contains(properties.getOrDefault(CONNECTOR_DATA_TYPE, REDIS_DATATYPE_STRING))) {
			throw new FlinkRuntimeException("Unsupported data type in incr mode. Supported: string, hash.");
		}

		RedisOptions options = getRedisOptions(descriptorProperties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		checkSchema(options, tableSchema);
		builder.setOptions(options);
		RedisOutputFormat outputFormat = builder.build();
		return new RedisUpsertTableSink(tableSchema, outputFormat);
	}

	private void checkSchema(RedisOptions options, TableSchema tableSchema) {
		if (options.getRedisDataType().equals(RedisDataType.HASH) && tableSchema.getFieldCount() == 2) {
			Preconditions.checkState(DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
				.equals(tableSchema.getFieldDataType(1).get()),
				"Unsupported type for hash value, should be map<varchar, varchar>");
		}
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		// The origin properties is an UnmodifiableMap, so we create a new one.
		Map<String, String> newProperties = new HashMap<>(properties);
		addDefaultProperties(newProperties);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(newProperties);
		validate(descriptorProperties);
		return descriptorProperties;
	}

	protected void validate(DescriptorProperties descriptorProperties) {
		new RedisValidator().validate(descriptorProperties);
	}

	/**
	 * Add default psm info to properties.
	 * */
	private void addDefaultProperties(Map<String, String> properties) {
		String jobName = System.getProperty(ConfigConstants.JOB_NAME_KEY,
			ConfigConstants.JOB_NAME_DEFAULT);
		if (!properties.containsKey(CONNECTOR_PSM)) {
			properties.put(CONNECTOR_PSM,
				String.format(ConfigConstants.FLINK_PSM_TEMPLATE, jobName));
		}
	}

	private RedisOptions getRedisOptions(DescriptorProperties descriptorProperties) {
		RedisOptions.RedisOptionsBuilder builder =
				RedisOptions.builder();

		RedisMode mode = getRedisMode(descriptorProperties);
		builder.setMode(mode);
		RedisDataType redisDataType = getRedisDataType(descriptorProperties);
		builder.setRedisDataType(redisDataType);

		descriptorProperties.getOptionalString(CONNECTOR_CLUSTER).ifPresent(builder::setCluster);
		descriptorProperties.getOptionalString(CONNECTOR_PSM).ifPresent(builder::setPsm);
		descriptorProperties.getOptionalString(CONNECTOR_TYPE).ifPresent(builder::setStorage);
		descriptorProperties.getOptionalString(CONNECTOR_TABLE).ifPresent(builder::setTable);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::setBatchSize);
		descriptorProperties.getOptionalLong(CONNECTOR_BUFFER_FLUSH_INTERVAL_MS).ifPresent(builder::setBufferFlushInterval);
		descriptorProperties.getOptionalInt(CONNECTOR_FLUSH_MAX_RETRIES)
				.ifPresent(builder::setFlushMaxRetries);
		descriptorProperties.getOptionalInt(CONNECTOR_GET_RESOURCE_MAX_RETRIES)
				.ifPresent(builder::setGetResourceMaxRetries);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_TOTAL_CONNECTIONS)
				.ifPresent(builder::setMaxTotalConnections);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_IDLE_CONNECTIONS)
				.ifPresent(builder::setMaxIdleConnections);
		descriptorProperties.getOptionalInt(CONNECTOR_MIN_IDLE_CONNECTIONS)
				.ifPresent(builder::setMinIdleConnections);
		descriptorProperties.getOptionalInt(CONNECTOR_TIMEOUT_MS).ifPresent(builder::setTimeout);
		descriptorProperties.getOptionalInt(CONNECTOR_TTL_SECONDS).ifPresent(builder::setTtlSeconds);
		descriptorProperties.getOptionalBoolean(CONNECTOR_FORCE_CONNECTION_SETTINGS)
				.ifPresent(builder::setForceConnectionsSetting);
		descriptorProperties.getOptionalBoolean(CONNECTOR_LOG_FAILURES_ONLY)
				.ifPresent(builder::setLogFailuresOnly);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::setParallelism);
		descriptorProperties.getOptionalBoolean(CONNECTOR_SKIP_FORMAT_KEY)
				.ifPresent(builder::setSkipFormatKey);
		descriptorProperties.getOptionalInt(CONNECTOR_LATER_JOIN_LATENCY_MS)
				.ifPresent(builder::setLaterJoinLatencyMs);
		return builder.build();
	}

	/**
	 * When user set connector.skip-format-key true, it means that cut off first column
	 * which is regarded as redis/abase key. At this case, it should remove the schema.0.name
	 * and schema.0.type. At the same time, schema.i.name and schema.i.type need change to
	 * schema.i-1.name and schema.i-1.type.
	 * @param oldProperties origin properties
	 * @return transformed properties
	 */
	private Map<String, String> getSerializationProperties(Map<String, String> oldProperties) {
		if ("true".equalsIgnoreCase(oldProperties.get(CONNECTOR_SKIP_FORMAT_KEY))) {
			Map<String, String> newProperties = new HashMap<>();
			newProperties.putAll(oldProperties);
			newProperties.remove("schema.0.name");
			newProperties.remove("schema.0.type");
			String nameKey, typeKey, nameValue, typeValue, newNameKey, newTypeKey;
			for (int i = 1; i < oldProperties.size(); i++) {
				nameKey = "schema" + '.' + i + '.' + TABLE_SCHEMA_NAME;
				typeKey = "schema" + '.' + i + '.' + TABLE_SCHEMA_TYPE;
				newNameKey = "schema" + '.' + (i - 1) + '.' + TABLE_SCHEMA_NAME;
				newTypeKey = "schema" + '.' + (i - 1) + '.' + TABLE_SCHEMA_TYPE;
				nameValue = newProperties.get(nameKey);
				typeValue = newProperties.get(typeKey);
				if (newProperties.containsKey(nameKey)) {
					newProperties.remove(nameKey);
					newProperties.remove(typeKey);
					newProperties.put(newNameKey, nameValue);
					newProperties.put(newTypeKey, typeValue);
				}
			}
			return newProperties;
		} else {
			return oldProperties;
		}
	}

	private RedisMode getRedisMode(DescriptorProperties descriptorProperties) {
		String tmpMode = descriptorProperties.getOptionalString(CONNECTOR_MODE)
			.orElse(INSERT_MODE);
		RedisMode mode;
		if (tmpMode.equalsIgnoreCase(INSERT_MODE)) {
			mode = RedisMode.INSERT;
		} else if (tmpMode.equalsIgnoreCase(INCR_MODE)) {
			mode = RedisMode.INCR;
		} else {
			throw new FlinkRuntimeException(String.format("Unsupported mode: %s, " +
				"currently supported modes: %s, %s", tmpMode, INCR_MODE, INSERT_MODE));
		}
		return mode;
	}

	private RedisDataType getRedisDataType(DescriptorProperties descriptorProperties) {
		String tmpDataType = descriptorProperties.getOptionalString(CONNECTOR_DATA_TYPE)
			.orElse(REDIS_DATATYPE_STRING);
		RedisDataType dataType;
		if (tmpDataType.equalsIgnoreCase(REDIS_DATATYPE_STRING)) {
			dataType = RedisDataType.STRING;
		} else if (tmpDataType.equalsIgnoreCase(REDIS_DATATYPE_HASH)) {
			dataType = RedisDataType.HASH;
		} else if (tmpDataType.equalsIgnoreCase(REDIS_DATATYPE_LIST)) {
			dataType = RedisDataType.LIST;
		} else if (tmpDataType.equalsIgnoreCase(REDIS_DATATYPE_SET)) {
			dataType = RedisDataType.SET;
		} else if (tmpDataType.equalsIgnoreCase(REDIS_DATATYPE_ZSET)) {
			dataType = RedisDataType.ZSET;
		} else {
			throw new FlinkRuntimeException(String.format("Unsupported data type: %s, " +
				"currently supported type: %s, %s, %s, %s, %s", tmpDataType, REDIS_DATATYPE_STRING,
				REDIS_DATATYPE_HASH, REDIS_DATATYPE_LIST, REDIS_DATATYPE_SET, REDIS_DATATYPE_ZSET));
		}
		return dataType;
	}

	private RedisLookupOptions getRedisLookupOptions(DescriptorProperties descriptorProperties) {
		final RedisLookupOptions.Builder builder = RedisLookupOptions.builder();

		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL).ifPresent(
				s -> builder.setCacheExpireMs(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);
		descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_CACHE_NULL_VALUE).ifPresent(builder::setCacheNullValue);
		descriptorProperties.getOptionalString(CONNECTOR_KEY_FIELDS).ifPresent(builder::setKeyField);
		descriptorProperties.getOptionalLong(CONNECTOR_RATE_LIMIT).ifPresent(builder::setRateLimit);
		descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY)
			.ifPresent(builder::setIsInputKeyByEnabled);
		descriptorProperties.getOptionalBoolean(CONNECTOR_LOOKUP_SPECIFY_HASH_KEYS).ifPresent(builder::setSpecifyHashKeys);

		return builder.build();
	}
}
