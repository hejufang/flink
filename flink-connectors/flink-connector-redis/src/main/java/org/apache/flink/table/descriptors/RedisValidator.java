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

import org.apache.flink.connectors.util.RedisDataType;
import org.apache.flink.connectors.util.RedisMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * The validator for redis.
 */
public class RedisValidator extends ConnectorDescriptorValidator {
	public static final String ABASE = "abase";
	public static final String REDIS = "redis";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_PSM = "connector.psm";
	public static final String CONNECTOR_MODE = "connector.mode";
	public static final String CONNECTOR_DATA_TYPE = "connector.redis-data-type";
	public static final String CONNECTOR_GET_RESOURCE_MAX_RETRIES = "connector.get-resource-max-retries";
	public static final String CONNECTOR_FLUSH_MAX_RETRIES = "connector.flush-max-retries";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_BUFFER_FLUSH_INTERVAL_MS = "connector.buffer-flush.interval-ms";
	public static final String CONNECTOR_TTL_SECONDS = "connector.ttl-seconds";
	public static final String CONNECTOR_TIMEOUT_MS = "connector.timeout-ms";
	public static final String CONNECTOR_LOG_FAILURES_ONLY = "connector.log-failures-only";
	public static final String CONNECTOR_MAX_TOTAL_CONNECTIONS = "connector.max-total-connections";
	public static final String CONNECTOR_MAX_IDLE_CONNECTIONS = "connector.max-idle-connections";
	public static final String CONNECTOR_MIN_IDLE_CONNECTIONS = "connector.min-idle-connections";
	public static final String CONNECTOR_FORCE_CONNECTION_SETTINGS = "connector.force-connection-settings";
	public static final String CONNECTOR_SKIP_FORMAT_KEY = "connector.skip-format-key";

	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";
	public static final String CONNECTOR_LATER_JOIN_LATENCY_MS = "connector.later-join-latency-ms";

	public static final String CONNECTOR_RATE_LIMIT = "connector.global-rate-limit";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, REDIS, false);
		properties.validateString(CONNECTOR_CLUSTER, false, 1);
		properties.validateString(CONNECTOR_PSM, false, 1);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
		properties.validateEnumValues(CONNECTOR_MODE, true,
			Arrays.stream(RedisMode.values()).map(Enum::name).map(String::toLowerCase).collect(Collectors.toList()));
		properties.validateEnumValues(CONNECTOR_DATA_TYPE, true,
			Arrays.stream(RedisDataType.values()).map(Enum::name).map(String::toLowerCase).collect(Collectors.toList()));
		properties.validateInt(CONNECTOR_GET_RESOURCE_MAX_RETRIES, true, 1);
		properties.validateInt(CONNECTOR_FLUSH_MAX_RETRIES, true, 1);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateLong(CONNECTOR_BUFFER_FLUSH_INTERVAL_MS, true, 0);
		properties.validateInt(CONNECTOR_TTL_SECONDS, true, 1);
		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1);
		properties.validateBoolean(CONNECTOR_LOG_FAILURES_ONLY, true);
		properties.validateBoolean(CONNECTOR_SKIP_FORMAT_KEY, true);
		properties.validateInt(CONNECTOR_LATER_JOIN_LATENCY_MS, true, 1000);
		properties.validateLong(CONNECTOR_RATE_LIMIT, true, 1);
		properties.validateBoolean(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY, true);
		validateKeyField(properties);
	}

	protected void validateKeyField(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_KEY_FIELDS, true, 1);
		properties.getOptionalString(CONNECTOR_KEY_FIELDS).ifPresent(
			keyFieldString -> {
				String[] fieldArray = keyFieldString.split(",");
				if (fieldArray.length > 1) {
					throw new ValidationException(String.format("%d primary keys are set. Only 1 primary key can " +
						"be set in Abase / Redis connector.", fieldArray.length));
				}
				TableSchema tableSchema = properties.getTableSchema(SCHEMA);
				if (!tableSchema.getFieldNameIndex(fieldArray[0]).isPresent()) {
					throw new ValidationException(String.format("The declared key field doesn't exist. " +
						"The schema is %s", String.join(", ", tableSchema.getFieldNames())));
				}
			}
		);
	}
}
