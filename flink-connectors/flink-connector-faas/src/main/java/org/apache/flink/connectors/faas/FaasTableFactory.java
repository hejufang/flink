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

package org.apache.flink.connectors.faas;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FaasValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_LOOKUP_CACHE_TTL;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_LOOKUP_MAX_RETRIES;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_MAX_CONNECTIONS;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.FaasValidator.CONNECTOR_URL;
import static org.apache.flink.table.descriptors.FaasValidator.FAAS;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link FaasTableSource}.
 */
public class FaasTableFactory implements StreamTableSourceFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, FAAS);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// faas option
		properties.add(CONNECTOR_URL);
		properties.add(CONNECTOR_TIMEOUT_MS);
		properties.add(CONNECTOR_MAX_CONNECTIONS);

		// lookup option
		properties.add(CONNECTOR_LOOKUP_CACHE_MAX_ROWS);
		properties.add(CONNECTOR_LOOKUP_CACHE_TTL);
		properties.add(CONNECTOR_LOOKUP_MAX_RETRIES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);

		FaasLookupOptions.Builder builder = FaasLookupOptions.builder();
		descriptorProperties.getOptionalString(CONNECTOR_URL).ifPresent(builder::setUrl);
		descriptorProperties.getOptionalInt(CONNECTOR_TIMEOUT_MS)
			.ifPresent(builder::setConnectionTimeoutMs);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_CONNECTIONS)
			.ifPresent(builder::setMaxConnections);
		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS)
			.ifPresent(builder::setCacheMaxSize);
		descriptorProperties.getOptionalLong(CONNECTOR_LOOKUP_CACHE_TTL)
			.ifPresent(builder::setCacheExpireMs);
		descriptorProperties.getOptionalInt(CONNECTOR_LOOKUP_MAX_RETRIES)
			.ifPresent(builder::setMaxRetryTimes);

		return new FaasTableSource(tableSchema, builder.build());
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		validate(descriptorProperties);
		return descriptorProperties;
	}

	private void validate(DescriptorProperties descriptorProperties) {
		new FaasValidator().validate(descriptorProperties);
	}
}
