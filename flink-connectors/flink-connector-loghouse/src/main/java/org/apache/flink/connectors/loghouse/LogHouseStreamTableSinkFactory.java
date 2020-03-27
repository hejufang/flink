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

package org.apache.flink.connectors.loghouse;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.LogHouseValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.LogHouseValidator.CLUSTERING;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_CONNECTION_POOL_SIZE;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_CONSUL;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_CONSUL_INTERVAL;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_FLUSH_MAX_RETRIES;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_FLUSH_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_KEYS_INDEX;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_NAMESPACE;
import static org.apache.flink.table.descriptors.LogHouseValidator.CONNECTOR_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.LogHouseValidator.LOG_HOUSE;
import static org.apache.flink.table.descriptors.LogHouseValidator.PARTITION;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * The factory for creating a {@link LogHouseUpsertStreamTableSink}.
 */
public class LogHouseStreamTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> requiredContext = new HashMap<>(1);
		requiredContext.put(CONNECTOR_TYPE, LOG_HOUSE);
		return requiredContext;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> supportedProperties = new ArrayList<>();

		// namespace
		supportedProperties.add(CONNECTOR_NAMESPACE);

		// consul
		supportedProperties.add(CONNECTOR_CONSUL);
		supportedProperties.add(CONNECTOR_CONSUL_INTERVAL);

		// connect
		supportedProperties.add(CONNECTOR_TIMEOUT_MS);
		supportedProperties.add(CONNECTOR_CONNECTION_POOL_SIZE);

		// flush
		supportedProperties.add(CONNECTOR_FLUSH_MAX_RETRIES);
		supportedProperties.add(CONNECTOR_FLUSH_TIMEOUT_MS);
		supportedProperties.add(CONNECTOR_BATCH_SIZE);

		// index
		supportedProperties.add(CONNECTOR_KEYS_INDEX + ".#." + PARTITION);
		supportedProperties.add(CONNECTOR_KEYS_INDEX + ".#." + CLUSTERING);

		// parallelism
		supportedProperties.add(CONNECTOR_PARALLELISM);

		// schema
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// format wildcard
		supportedProperties.add(FORMAT + ".*");

		return supportedProperties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final LogHouseOptions.Builder builder = LogHouseOptions.builder();
		descriptorProperties.getOptionalString(CONNECTOR_NAMESPACE).ifPresent(builder::withNamespace);
		descriptorProperties.getOptionalString(CONNECTOR_CONSUL).ifPresent(builder::withConsul);
		descriptorProperties.getOptionalInt(CONNECTOR_FLUSH_MAX_RETRIES).ifPresent(builder::flushMaxRetries);
		descriptorProperties.getOptionalInt(CONNECTOR_FLUSH_TIMEOUT_MS).ifPresent(builder::flushTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::batchSizeKB);
		descriptorProperties.getOptionalInt(CONNECTOR_TIMEOUT_MS).ifPresent(builder::connectTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_CONSUL_INTERVAL).ifPresent(builder::consulIntervalSeconds);
		descriptorProperties.getOptionalInt(CONNECTOR_CONNECTION_POOL_SIZE).ifPresent(builder::connectionPoolSize);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::sinkParallelism);
		builder.withKeysIndex(getKeysIndex(descriptorProperties));

		final SerializationSchema<org.apache.flink.types.Row> serializationSchema = TableConnectorUtils
			.getSerializationSchema(properties, this.getClass().getClassLoader());
		builder.withSerializationSchema(serializationSchema);

		final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);

		return new LogHouseUpsertStreamTableSink(builder.build(), tableSchema);
	}

	private List<Tuple2<Integer, Integer>> getKeysIndex(DescriptorProperties descriptorProperties) {
		return descriptorProperties.getFixedIndexedProperties(CONNECTOR_KEYS_INDEX,
			Arrays.asList(CLUSTERING, PARTITION)).stream()
			.map(property -> Tuple2.of(
				descriptorProperties.getInt(property.get(CLUSTERING)),
				descriptorProperties.getInt(property.get(PARTITION))))
			.collect(Collectors.toList());
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new LogHouseValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
