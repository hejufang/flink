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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;

/**
 * The validator for {@link Elasticsearch}.
 */
@Internal
public class ElasticsearchValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_ELASTICSEARCH = "elasticsearch";
	public static final String CONNECTOR_VERSION_VALUE_6 = "6";
	public static final String CONNECTOR_VERSION_VALUE_6_KMS = "6-kms";
	public static final String CONNECTOR_VERSION_VALUE_6_AD = "6-ad";
	public static final String CONNECTOR_VERSION_VALUE_7 = "7";
	public static final String CONNECTOR_HOSTS = "connector.hosts";
	public static final String CONNECTOR_HOSTS_HOSTNAME = "hostname";
	public static final String CONNECTOR_HOSTS_PORT = "port";
	public static final String CONNECTOR_HOSTS_PROTOCOL = "protocol";
	public static final String CONNECTOR_INDEX = "connector.index";
	public static final String CONNECTOR_DOCUMENT_TYPE = "connector.document-type";
	public static final String CONNECTOR_KEY_DELIMITER = "connector.key-delimiter";
	public static final String CONNECTOR_KEY_NULL_LITERAL = "connector.key-null-literal";
	public static final String CONNECTOR_KEY_FIELD_INDICES = "connector.key-field-indices";
	public static final String CONNECTOR_FAILURE_HANDLER = "connector.failure-handler";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_FAIL = "fail";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE = "ignore";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_RETRY = "retry-rejected";
	public static final String CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM = "custom";
	public static final String CONNECTOR_FAILURE_HANDLER_CLASS = "connector.failure-handler-class";
	public static final String CONNECTOR_FLUSH_ON_CHECKPOINT = "connector.flush-on-checkpoint";
	public static final String CONNECTOR_BULK_FLUSH_MAX_ACTIONS = "connector.bulk-flush.max-actions";
	public static final String CONNECTOR_BULK_FLUSH_MAX_SIZE = "connector.bulk-flush.max-size";
	public static final String CONNECTOR_BULK_FLUSH_INTERVAL = "connector.bulk-flush.interval";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE = "connector.bulk-flush.backoff.type";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED = "disabled";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT = "constant";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL = "exponential";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES = "connector.bulk-flush.backoff.max-retries";
	public static final String CONNECTOR_BULK_FLUSH_BACKOFF_DELAY = "connector.bulk-flush.backoff.delay";
	public static final String CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT = "connector.connection-max-retry-timeout";
	public static final String CONNECTOR_CONNECTION_PATH_PREFIX = "connector.connection-path-prefix";
	public static final String CONNECTOR_CONNECTION_CONSUL = "connector.connection-consul";
	public static final String CONNECTOR_CONNECTION_HTTP_SCHEMA = "connector.connection-http-schema";
	public static final String CONNECTOR_CONNECTION_ENABLE_PASSWORD_CONFIG = "connector.connection-enable-password-config";
	public static final String CONNECTOR_CONNECTION_USERNAME = "connector.connection-username";
	public static final String CONNECTOR_CONNECTION_PASSWORD = "connector.connection-password";
	public static final String CONNECTOR_GLOBAL_RATE_LIMIT = "connector.global-rate-limit";
	public static final String CONNECTOR_URI = "connector.uri";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ELASTICSEARCH, false);
		properties.validateLong(CONNECTOR_GLOBAL_RATE_LIMIT, true, 1);
		properties.validateString(CONNECTOR_URI, true, 1);
		validateVersion(properties);
		validateHosts(properties);
		validateGeneralProperties(properties);
		validateFailureHandler(properties);
		validateBulkFlush(properties);
		validateConnectionProperties(properties);
	}

	private void validateVersion(DescriptorProperties properties) {
		properties.validateEnumValues(
			CONNECTOR_VERSION,
			false,
			ImmutableList.of(CONNECTOR_VERSION_VALUE_6, CONNECTOR_VERSION_VALUE_6_KMS, CONNECTOR_VERSION_VALUE_6_AD,
				CONNECTOR_VERSION_VALUE_7));
	}

	private void validateHosts(DescriptorProperties properties) {
		final Map<String, Consumer<String>> hostsValidators = new HashMap<>();
		hostsValidators.put(CONNECTOR_HOSTS_HOSTNAME, (key) -> properties.validateString(key, true, 1));
		hostsValidators.put(CONNECTOR_HOSTS_PORT, (key) -> properties.validateInt(key, true, 0, 65535));
		hostsValidators.put(CONNECTOR_HOSTS_PROTOCOL, (key) -> properties.validateString(key, true, 1));
		properties.validateFixedIndexedProperties(CONNECTOR_HOSTS, true, hostsValidators);
	}

	private void validateGeneralProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_INDEX, false, 1);
		properties.validateString(CONNECTOR_DOCUMENT_TYPE, false, 1);
		properties.validateString(CONNECTOR_KEY_DELIMITER, true);
		properties.validateString(CONNECTOR_KEY_NULL_LITERAL, true);
		properties.validateString(CONNECTOR_KEY_FIELD_INDICES, true);
	}

	private void validateFailureHandler(DescriptorProperties properties) {
		final Map<String, Consumer<String>> failureHandlerValidators = new HashMap<>();
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_FAIL, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_IGNORE, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_RETRY, noValidation());
		failureHandlerValidators.put(CONNECTOR_FAILURE_HANDLER_VALUE_CUSTOM,
			key -> properties.validateString(CONNECTOR_FAILURE_HANDLER_CLASS, false, 1));
		properties.validateEnum(CONNECTOR_FAILURE_HANDLER, true, failureHandlerValidators);
	}

	private void validateBulkFlush(DescriptorProperties properties) {
		properties.validateBoolean(CONNECTOR_FLUSH_ON_CHECKPOINT, true);
		properties.validateInt(CONNECTOR_BULK_FLUSH_MAX_ACTIONS, true, 1);
		properties.validateMemorySize(CONNECTOR_BULK_FLUSH_MAX_SIZE, true, 1024 * 1024); // only allow MB precision
		properties.validateLong(CONNECTOR_BULK_FLUSH_INTERVAL, true, 0);
		properties.validateEnumValues(CONNECTOR_BULK_FLUSH_BACKOFF_TYPE,
			true,
			Arrays.asList(
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_DISABLED,
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_CONSTANT,
				CONNECTOR_BULK_FLUSH_BACKOFF_TYPE_VALUE_EXPONENTIAL));
		properties.validateInt(CONNECTOR_BULK_FLUSH_BACKOFF_MAX_RETRIES, true, 1);
		properties.validateLong(CONNECTOR_BULK_FLUSH_BACKOFF_DELAY, true, 0);
	}

	private void validateConnectionProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_CONNECTION_MAX_RETRY_TIMEOUT, true, 1);
		properties.validateString(CONNECTOR_CONNECTION_PATH_PREFIX, true);
		properties.validateString(CONNECTOR_CONNECTION_CONSUL, true);
		properties.validateString(CONNECTOR_CONNECTION_HTTP_SCHEMA, true);
		properties.validateString(CONNECTOR_CONNECTION_ENABLE_PASSWORD_CONFIG, true);
		properties.validateString(CONNECTOR_CONNECTION_USERNAME, true);
		properties.validateString(CONNECTOR_CONNECTION_PASSWORD, true);
	}
}
