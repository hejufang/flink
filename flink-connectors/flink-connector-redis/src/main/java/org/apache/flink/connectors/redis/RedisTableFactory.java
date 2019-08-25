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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RedisValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_FLUSH_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_FORCE_CONNECTION_SETTINGS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_GET_RESOURCE_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOG_FAILURES_ONLY;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_IDLE_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_TOTAL_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MIN_IDLE_CONNECTIONS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MODE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PSM;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_STORAGE;
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

		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_PSM);
		properties.add(CONNECTOR_STORAGE);
		properties.add(CONNECTOR_MODE);
		properties.add(CONNECTOR_GET_RESOURCE_MAX_RETRIES);
		properties.add(CONNECTOR_FLUSH_MAX_RETRIES);
		properties.add(CONNECTOR_BATCH_SIZE);
		properties.add(CONNECTOR_TTL_SECONDS);
		properties.add(CONNECTOR_TIMEOUT_MS);
		properties.add(CONNECTOR_FORCE_CONNECTION_SETTINGS);
		properties.add(CONNECTOR_MAX_TOTAL_CONNECTIONS);
		properties.add(CONNECTOR_MAX_IDLE_CONNECTIONS);
		properties.add(CONNECTOR_MIN_IDLE_CONNECTIONS);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		//lookup option
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

		return properties;
	}

	/**
	 * attention: only support redis lookup source currently.
	 * @param properties normalized properties describing a stream table source.
	 */
	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		return RedisTableSource.builder()
				.setOptions(getRedisOptions(descriptorProperties))
				.setLookupOptions(getRedisLookupOptions(descriptorProperties))
				.setSchema(descriptorProperties.getTableSchema(SCHEMA))
				.build();
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		RedisOutputFormat.RedisOutputFormatBuilder builder =
			RedisOutputFormat.buildRedisOutputFormat();

		builder.setOptions(getRedisOptions(descriptorProperties));
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		RedisOutputFormat outputFormat = builder.build();
		return new RedisUpsertTableSink(tableSchema, outputFormat);
	}

	public DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new RedisValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

	private RedisOptions getRedisOptions(DescriptorProperties descriptorProperties) {
		RedisOptions.RedisOptionsBuilder builder =
				RedisOptions.builder();

		descriptorProperties.getOptionalString(CONNECTOR_CLUSTER).ifPresent(builder::setCluster);
		descriptorProperties.getOptionalString(CONNECTOR_MODE).ifPresent(builder::setMode);
		descriptorProperties.getOptionalString(CONNECTOR_PSM).ifPresent(builder::setPsm);
		descriptorProperties.getOptionalString(CONNECTOR_TYPE).ifPresent(builder::setStorage);
		descriptorProperties.getOptionalString(CONNECTOR_TABLE).ifPresent(builder::setTable);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::setBatchSize);
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

		return builder.build();
	}

	private RedisLookupOptions getRedisLookupOptions(DescriptorProperties descriptorProperties) {
		final RedisLookupOptions.Builder builder = RedisLookupOptions.builder();

		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL).ifPresent(
				s -> builder.setCacheExpireMs(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);

		return builder.build();
	}
}
