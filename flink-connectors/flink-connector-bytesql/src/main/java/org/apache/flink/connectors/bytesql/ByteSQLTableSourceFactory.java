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

import org.apache.flink.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.descriptors.ByteSQLValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_CONNECTION_TIMEOUT;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_CONSUL;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_DATABASE;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_TABLE;
import static org.apache.flink.table.descriptors.ByteSQLValidator.CONNECTOR_USERNAME;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory to creating configured instances of {@link ByteSQLTableSource}.
 */
public class ByteSQLTableSourceFactory implements StreamTableSourceFactory<Row> {
	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		return new ByteSQLTableSource(
			getByteSQLOptions(descriptorProperties),
			getByteSQLLookupOptions(descriptorProperties),
			descriptorProperties.getTableSchema(SCHEMA)
		);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, ByteSQLValidator.IDENTIFIER);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(CONNECTOR_DATABASE);
		properties.add(CONNECTOR_TABLE);
		properties.add(CONNECTOR_USERNAME);
		properties.add(CONNECTOR_PASSWORD);
		properties.add(CONNECTOR_CONSUL);
		properties.add(CONNECTOR_CONNECTION_TIMEOUT);
		properties.add(CONNECTOR_PARALLELISM);

		// lookup options
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new ByteSQLValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

	private ByteSQLOptions getByteSQLOptions(DescriptorProperties descriptorProperties) {
		final ByteSQLOptions.Builder builder = ByteSQLOptions.builder()
			.setConsul(descriptorProperties.getString(CONNECTOR_CONSUL))
			.setDatabaseName(descriptorProperties.getString(CONNECTOR_DATABASE))
			.setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
			.setUsername(descriptorProperties.getString(CONNECTOR_USERNAME))
			.setPassword(descriptorProperties.getString(CONNECTOR_PASSWORD));
		descriptorProperties.getOptionalLong(CONNECTOR_CONNECTION_TIMEOUT).ifPresent(builder::setConnectionTimeout);
		return builder.build();
	}

	private ByteSQLLookupOptions getByteSQLLookupOptions(DescriptorProperties descriptorProperties) {
		final ByteSQLLookupOptions.Builder builder = ByteSQLLookupOptions.builder();
		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS).ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalDuration(CONNECTOR_LOOKUP_CACHE_TTL).ifPresent(
			s -> builder.setCacheExpireMs(s.toMillis()));
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES).ifPresent(builder::setMaxRetryTimes);
		return builder.build();
	}
}
