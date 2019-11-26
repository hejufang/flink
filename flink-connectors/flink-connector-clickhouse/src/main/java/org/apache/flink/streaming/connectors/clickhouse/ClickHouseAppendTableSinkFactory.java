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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * ClickHouse append table sink factory.
 */
public class ClickHouseAppendTableSinkFactory implements StreamTableSinkFactory<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, ClickHouseValidator.CONNECTOR_TYPE_VALUE_CLICKHOUSE); // clickhouse
		context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
		return context;
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		String[] fieldNames = tableSchema.getFieldNames();
		TypeInformation<?>[] fieldTypes = tableSchema.getFieldTypes();

		final ClickHouseAppendTableSinkBuilder builder = ClickHouseAppendTableSink.builder()
			.setDrivername(descriptorProperties.getString(ClickHouseValidator.CONNECTOR_DRIVER))
			.setDbName(descriptorProperties.getString(ClickHouseValidator.CONNECTOR_DB))
			.setTableName(descriptorProperties.getString(ClickHouseValidator.CONNECTOR_TABLE))
			.setSignColumn(descriptorProperties.getString(ClickHouseValidator.CONNECTOR_TABLE_SIGN_COLUMN))
			.setColumnNames(fieldNames)
			.setParameterTypes(fieldTypes)
			.setTableScehma(tableSchema);

		descriptorProperties.getOptionalString(ClickHouseValidator.CONNECTOR_URL).ifPresent(builder::setDbUrl);
		descriptorProperties.getOptionalString(ClickHouseValidator.CONNECTOR_PSM).ifPresent(builder::setPsm);
		descriptorProperties.getOptionalString(ClickHouseValidator.CONNECTOR_USERNAME).ifPresent(builder::setUsername);
		descriptorProperties.getOptionalString(ClickHouseValidator.CONNECTOR_PASSWORD).ifPresent(builder::setPassword);
		descriptorProperties.getOptionalInt(ClickHouseValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(builder::setFlushMaxSize);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::setParallelism);

		return builder.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new ClickHouseValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		// common options
		properties.add(ClickHouseValidator.CONNECTOR_DRIVER);
		properties.add(ClickHouseValidator.CONNECTOR_URL);
		properties.add(ClickHouseValidator.CONNECTOR_PSM);
		properties.add(ClickHouseValidator.CONNECTOR_DB);
		properties.add(ClickHouseValidator.CONNECTOR_TABLE);
		properties.add(ClickHouseValidator.CONNECTOR_TABLE_SIGN_COLUMN);
		properties.add(ClickHouseValidator.CONNECTOR_USERNAME);
		properties.add(ClickHouseValidator.CONNECTOR_PASSWORD);

		// sink options
		properties.add(ClickHouseValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		properties.add(ClickHouseValidator.CONNECTOR_WRITE_FLUSH_INTERVAL);
		properties.add(CONNECTOR_PARALLELISM);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}
}
