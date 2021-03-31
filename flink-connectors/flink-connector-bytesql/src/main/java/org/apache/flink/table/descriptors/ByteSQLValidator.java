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

/**
 * Validator for ByteSQL connector.
 */
public class ByteSQLValidator extends ConnectorDescriptorValidator {
	public static final String IDENTIFIER = "bytesql";

	public static final String CONNECTOR_CONSUL = "connector.consul";
	public static final String CONNECTOR_DATABASE = "connector.database";
	public static final String CONNECTOR_TABLE = "connector.table-name";
	public static final String CONNECTOR_USERNAME = "connector.username";
	public static final String CONNECTOR_PASSWORD = "connector.password";
	public static final String CONNECTOR_CONNECTION_TIMEOUT = "connector.connection.timeout";

	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";

	public static final String CONNECTOR_SINK_BUFFER_FLUSH_MAX_ROWS = "connector.sink.buffer-flush.max-rows";
	public static final String CONNECTOR_SINK_BUFFER_FLUSH_INTERVAL = "connector.sink.buffer-flush.interval";
	public static final String CONNECTOR_SINK_PRIMARY_KEY_INDICES = "connector.sink.primary-key-indices";
	public static final String CONNECTOR_SINK_MAX_RETRIES = "connector.sink.max-retries";
	public static final String CONNECTOR_SINK_IGNORE_NULL_COLUMNS = "connector.sink.ignore-null-columns";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		validateCommonProperties(properties);
		validateLookupProperties(properties);
		validateSinkProperties(properties);
	}

	private void validateCommonProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_CONSUL, false);
		properties.validateString(CONNECTOR_DATABASE, false);
		properties.validateString(CONNECTOR_TABLE, false);
		properties.validateString(CONNECTOR_USERNAME, false);
		properties.validateString(CONNECTOR_PASSWORD, false);
		properties.validateDuration(CONNECTOR_CONNECTION_TIMEOUT, true, 1);
	}

	private void validateLookupProperties(DescriptorProperties properties) {
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_LOOKUP_CACHE_TTL, true, 1);
		properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true);
		properties.validateBoolean(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY, true);

		checkAllOrNone(properties, new String[]{
			CONNECTOR_LOOKUP_CACHE_MAX_ROWS,
			CONNECTOR_LOOKUP_CACHE_TTL
		});
	}

	private void validateSinkProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_SINK_BUFFER_FLUSH_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_SINK_BUFFER_FLUSH_INTERVAL, true, 1);
		properties.validateInt(CONNECTOR_SINK_MAX_RETRIES, true);
		properties.validateString(CONNECTOR_SINK_PRIMARY_KEY_INDICES, true);
		properties.validateBoolean(CONNECTOR_SINK_IGNORE_NULL_COLUMNS, true);
	}
}
