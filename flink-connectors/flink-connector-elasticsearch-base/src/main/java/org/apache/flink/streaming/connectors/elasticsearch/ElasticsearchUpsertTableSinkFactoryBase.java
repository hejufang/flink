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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.Host;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase.SinkOption;
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.ElasticsearchValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.descriptors.StreamTableDescriptorValidator;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_DELAY;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_MAX_ACTIONS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_BULK_FLUSH_MAX_SIZE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_CONSUL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_ENABLE_PASSWORD_CONFIG;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_HTTP_SCHEMA;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_PASSWORD;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_PATH_PREFIX;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_CONNECTION_USERNAME;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_DOCUMENT_TYPE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_CLASS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_FAIL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FAILURE_HANDLER_VALUE_RETRY;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_FLUSH_ON_CHECKPOINT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS_HOSTNAME;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS_PORT;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_HOSTS_PROTOCOL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_INDEX;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_KEY_DELIMITER;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_KEY_FIELD_INDICES;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_KEY_NULL_LITERAL;
import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_TYPE_VALUE_ELASTICSEARCH;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Version-agnostic table factory for creating an {@link UpsertStreamTableSink} for Elasticsearch.
 */
@Internal
public abstract class ElasticsearchUpsertTableSinkFactoryBase implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	private static final String SUPPORTED_FORMAT_TYPE = "json";
	private static final XContentType SUPPORTED_CONTENT_TYPE = XContentType.JSON;
	private static final String DEFAULT_KEY_DELIMITER = "_";
	private static final String DEFAULT_KEY_NULL_LITERAL = "null";
	private static final String DEFAULT_FAILURE_HANDLER = CONNECTOR_FAILURE_HANDLER_VALUE_FAIL;

	@Override
	public Map<String, String> requiredContext() {
		final Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ELASTICSEARCH);
		context.put(CONNECTOR_VERSION, elasticsearchVersion());
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		final List<String> properties = new ArrayList<>();

		// streaming properties
		properties.add(UPDATE_MODE);

		// Elasticsearch
		properties.add(CONNECTOR_HOSTS + ".#." + CONNECTOR_HOSTS_HOSTNAME);
		properties.add(CONNECTOR_HOSTS + ".#." + CONNECTOR_HOSTS_PORT);
		properties.add(CONNECTOR_HOSTS + ".#." + CONNECTOR_HOSTS_PROTOCOL);
		properties.add(CONNECTOR_INDEX);
		properties.add(CONNECTOR_DOCUMENT_TYPE);
		properties.add(CONNECTOR_KEY_DELIMITER);
		properties.add(CONNECTOR_KEY_NULL_LITERAL);
		properties.add(CONNECTOR_FAILURE_HANDLER);
		properties.add(CONNECTOR_FAILURE_HANDLER_CLASS);
		properties.add(CONNECTOR_FLUSH_ON_CHECKPOINT);
		properties.add(CONNECTOR_BULK_FLUSH_MAX_ACTIONS);
		properties.add(CONNECTOR_BULK_FLUSH_MAX_SIZE);
		properties.add(CONNECTOR_BULK_FLUSH_INTERVAL);
		properties.add(CONNECTOR_BULK_FLUSH_BACKOFF_TYPE);
		properties.add(CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES);
		properties.add(CONNECTOR_BULK_FLUSH_BACKOFF_DELAY);
		properties.add(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT);
		properties.add(CONNECTOR_CONNECTION_PATH_PREFIX);
		properties.add(CONNECTOR_CONNECTION_CONSUL);
		properties.add(CONNECTOR_CONNECTION_HTTP_SCHEMA);
		properties.add(CONNECTOR_CONNECTION_ENABLE_PASSWORD_CONFIG);
		properties.add(CONNECTOR_CONNECTION_USERNAME);
		properties.add(CONNECTOR_CONNECTION_PASSWORD);
		properties.add(CONNECTOR_KEY_FIELD_INDICES);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		return createElasticsearchUpsertTableSink(
			descriptorProperties.isValue(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND),
			descriptorProperties.getTableSchema(SCHEMA),
			getHosts(descriptorProperties),
			descriptorProperties.getString(CONNECTOR_INDEX),
			descriptorProperties.getString(CONNECTOR_DOCUMENT_TYPE),
			descriptorProperties.getOptionalString(CONNECTOR_KEY_DELIMITER).orElse(DEFAULT_KEY_DELIMITER),
			descriptorProperties.getOptionalString(CONNECTOR_KEY_NULL_LITERAL).orElse(DEFAULT_KEY_NULL_LITERAL),
			getSerializationSchema(properties),
			SUPPORTED_CONTENT_TYPE,
			getFailureHandler(descriptorProperties),
			getSinkOptions(descriptorProperties),
			getKeyFieldIndices(descriptorProperties));
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific factories
	// --------------------------------------------------------------------------------------------

	protected abstract String elasticsearchVersion();

	protected abstract ElasticsearchUpsertTableSinkBase createElasticsearchUpsertTableSink(
		boolean isAppendOnly,
		TableSchema schema,
		List<Host> hosts,
		String index,
		String docType,
		String keyDelimiter,
		String keyNullLiteral,
		SerializationSchema<Row> serializationSchema,
		XContentType contentType,
		ActionRequestFailureHandler failureHandler,
		Map<SinkOption, String> sinkOptions,
		int[] keyFieldIndices);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new StreamTableDescriptorValidator(true, false, true).validate(descriptorProperties);
		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new ElasticsearchValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private List<Host> getHosts(DescriptorProperties descriptorProperties) {
		final List<Map<String, String>> hosts = descriptorProperties.getFixedIndexedProperties(
			CONNECTOR_HOSTS,
			Arrays.asList(CONNECTOR_HOSTS_HOSTNAME, CONNECTOR_HOSTS_PORT, CONNECTOR_HOSTS_PROTOCOL));
		return hosts.stream()
			.map(host -> new Host(
				descriptorProperties.getString(host.get(CONNECTOR_HOSTS_HOSTNAME)),
				descriptorProperties.getInt(host.get(CONNECTOR_HOSTS_PORT)),
				descriptorProperties.getString(host.get(CONNECTOR_HOSTS_PROTOCOL))))
			.collect(Collectors.toList());
	}

	private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
		//we put fixed property here to avoid user to set format schema
		Map<String, String> extendedProperties = new HashMap<>();
		properties.forEach(extendedProperties::put);
		extendedProperties.putIfAbsent(FORMAT_TYPE, SUPPORTED_FORMAT_TYPE);
		extendedProperties.putIfAbsent(FORMAT_DERIVE_SCHEMA, "true");

		@SuppressWarnings("unchecked")
		final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
			SerializationSchemaFactory.class,
			extendedProperties,
			this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(extendedProperties);
	}

	private ActionRequestFailureHandler getFailureHandler(DescriptorProperties descriptorProperties) {
		final String failureHandler = descriptorProperties
			.getOptionalString(CONNECTOR_FAILURE_HANDLER)
			.orElse(DEFAULT_FAILURE_HANDLER);
		switch (failureHandler) {
			case CONNECTOR_FAILURE_HANDLER_VALUE_FAIL:
				return new NoOpFailureHandler();
			case CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE:
				return new IgnoringFailureHandler();
			case CONNECTOR_FAILURE_HANDLER_VALUE_RETRY:
				return new RetryRejectedExecutionFailureHandler();
			case CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM:
				final Class<? extends ActionRequestFailureHandler> clazz = descriptorProperties
					.getClass(CONNECTOR_FAILURE_HANDLER_CLASS, ActionRequestFailureHandler.class);
				return InstantiationUtil.instantiate(clazz);
			default:
				throw new IllegalArgumentException("Unknown failure handler.");
		}
	}

	protected Map<SinkOption, String> getSinkOptions(DescriptorProperties descriptorProperties) {
		final Map<SinkOption, String> options = new HashMap<>();

		descriptorProperties.getOptionalBoolean(CONNECTOR_FLUSH_ON_CHECKPOINT)
			.ifPresent(v -> options.put(SinkOption.DISABLE_FLUSH_ON_CHECKPOINT, String.valueOf(!v)));

		mapSinkOption(descriptorProperties, options, CONNECTOR_BULK_FLUSH_MAX_ACTIONS, SinkOption.BULK_FLUSH_MAX_ACTIONS);
		mapSinkOption(descriptorProperties, options, CONNECTOR_BULK_FLUSH_MAX_SIZE, SinkOption.BULK_FLUSH_MAX_SIZE);
		mapSinkOption(descriptorProperties, options, CONNECTOR_BULK_FLUSH_INTERVAL, SinkOption.BULK_FLUSH_INTERVAL);

		descriptorProperties.getOptionalString(CONNECTOR_BULK_FLUSH_BACKOFF_TYPE)
			.ifPresent(v -> {
				options.put(
					SinkOption.BULK_FLUSH_BACKOFF_ENABLED,
					String.valueOf(!v.equals(CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED)));
				switch (v) {
					case CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT:
						options.put(
							SinkOption.BULK_FLUSH_BACKOFF_TYPE,
							ElasticsearchSinkBase.FlushBackoffType.CONSTANT.toString());
						break;
					case CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL:
						options.put(
							SinkOption.BULK_FLUSH_BACKOFF_TYPE,
							ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL.toString());
						break;
					default:
						throw new IllegalArgumentException("Unknown backoff type.");
				}
			});

		mapSinkOption(descriptorProperties, options, CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES, SinkOption.BULK_FLUSH_BACKOFF_RETRIES);
		mapSinkOption(descriptorProperties, options, CONNECTOR_BULK_FLUSH_BACKOFF_DELAY, SinkOption.BULK_FLUSH_BACKOFF_DELAY);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT, SinkOption.REST_MAX_RETRY_TIMEOUT);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_PATH_PREFIX, SinkOption.REST_PATH_PREFIX);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_CONSUL, SinkOption.CONSUL);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_HTTP_SCHEMA, SinkOption.HTTP_SCHEMA);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_ENABLE_PASSWORD_CONFIG, SinkOption.ENABLE_PASSWORD_CONFIG);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_USERNAME, SinkOption.USERNAME);
		mapSinkOption(descriptorProperties, options, CONNECTOR_CONNECTION_PASSWORD, SinkOption.PASSWORD);

		return options;
	}

	private int[] getKeyFieldIndices(DescriptorProperties descriptorProperties) {
		String indicesString = descriptorProperties.getOptionalString(CONNECTOR_KEY_FIELD_INDICES).orElse(null);
		if (StringUtils.isEmpty(indicesString)) {
			return new int[0];
		}
		return Arrays.stream(indicesString.split(",")).mapToInt(Integer::parseInt).toArray();
	}

	private void mapSinkOption(
			DescriptorProperties descriptorProperties,
			Map<SinkOption, String> options,
			String fromKey,
			SinkOption toKey) {
		descriptorProperties.getOptionalString(fromKey).ifPresent(v -> options.put(toKey, v));
	}
}
