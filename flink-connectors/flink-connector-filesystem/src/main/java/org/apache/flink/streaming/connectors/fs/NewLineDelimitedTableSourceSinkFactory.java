/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.CsvValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FileSystemValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_PATH;
import static org.apache.flink.table.descriptors.FileSystemValidator.CONNECTOR_TYPE_VALUE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_PARAMETERS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * newLine delimited table source factory.
 */
public class NewLineDelimitedTableSourceSinkFactory implements
	StreamTableSourceFactory<Row>,
	StreamTableSinkFactory<Row> {

	private static final String CONF_OUTPUT_FORMAT_COMPRESS_CODEC = "fileoutputformat.compress.codec";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_PROPERTY_VERSION);

		properties.add(CONNECTOR_PATH);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_PARAMETERS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		// format wildcard
		properties.add(FORMAT + ".*");

		properties.add(CONF_OUTPUT_FORMAT_COMPRESS_CODEC);

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		return createTableSource(true, properties);
	}

	protected NewLineDelimitedTableSource createTableSource(
		boolean isStreaming,
		Map<String, String> properties) {

		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);

		// validate
		new FileSystemValidator().validate(params);
		new SchemaValidator(isStreaming, false, false).validate(params);

		final DeserializationSchema<Row> deserializationSchema = getDeserializationSchema(properties);

		// build
		NewLineDelimitedTableSource.Builder newLineDelimitedTableSourceBuilder = NewLineDelimitedTableSource.builder();
		newLineDelimitedTableSourceBuilder.setDeserializationSchema(deserializationSchema);
		params.getOptionalString(CONNECTOR_PATH).ifPresent(newLineDelimitedTableSourceBuilder::setPath);

		Optional<String> proctimeAttribute = SchemaValidator.deriveProctimeAttribute(params);
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors = SchemaValidator.deriveRowtimeAttributes(params);
		newLineDelimitedTableSourceBuilder.setProctimeAttribute(proctimeAttribute);
		newLineDelimitedTableSourceBuilder.setRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);

		Configuration configuration = new Configuration();
		for (String key : properties.keySet()) {
			configuration.setString(key, properties.get(key));
		}
		newLineDelimitedTableSourceBuilder.setConfig(configuration);

		TableSchema tableSchema = params.getTableSchema(SCHEMA);
		newLineDelimitedTableSourceBuilder.setSchema(tableSchema);

		return newLineDelimitedTableSourceBuilder.build();
	}

	private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked") final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			DeserializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createDeserializationSchema(properties);
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		return createTableSink(true, properties);
	}

	private StreamTableSink<Row> createTableSink(boolean isStreaming, Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);

		// validate
		new FileSystemValidator().validate(params);
		new SchemaValidator(isStreaming, false, false).validate(params);

		final SerializationSchema<Row> serializationSchema = getSerializationSchema(properties);
		String path = params.getString(CONNECTOR_PATH);

		TableSchema tableSchema = params.getTableSchema(SCHEMA);
		Optional<String> codec = params.getOptionalString(CONF_OUTPUT_FORMAT_COMPRESS_CODEC);

		// csv格式需打印出header信息
		String fieldDelimiter = params.getOptionalString(CsvValidator.FORMAT_FIELD_DELIMITER).orElse(null);

		return codec.map(
			value -> new NewLineDelimitedTableSink(path, tableSchema, serializationSchema, value, fieldDelimiter)
		).orElse(new NewLineDelimitedTableSink(path, tableSchema, serializationSchema, fieldDelimiter));
	}

	private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked") final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			SerializationSchemaFactory.class,
			properties,
			this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(properties);
	}
}
