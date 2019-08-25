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
 * The validator for redis.
 */
public class RedisValidator extends ConnectorDescriptorValidator {
	public static final String ABASE = "abase";
	public static final String REDIS = "redis";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_PSM = "connector.psm";
	public static final String CONNECTOR_STORAGE = "connector.storage";
	public static final String CONNECTOR_MODE = "connector.mode";
	public static final String CONNECTOR_GET_RESOURCE_MAX_RETRIES = "connector.get-resource-max-retries";
	public static final String CONNECTOR_FLUSH_MAX_RETRIES = "connector.flush-max-retries";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_TTL_SECONDS = "connector.ttl-seconds";
	public static final String CONNECTOR_TIMEOUT_MS = "connector.timeout-ms";
	public static final String CONNECTOR_LOG_FAILURES_ONLY = "connector.log-failures-only";
	public static final String CONNECTOR_MAX_TOTAL_CONNECTIONS = "connector.max-total-connections";
	public static final String CONNECTOR_MAX_IDLE_CONNECTIONS = "connector.max-idle-connections";
	public static final String CONNECTOR_MIN_IDLE_CONNECTIONS = "connector.min-idle-connections";
	public static final String CONNECTOR_FORCE_CONNECTION_SETTINGS = "connector.force-connection-settings";

	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, REDIS, false);
		properties.validateString(CONNECTOR_CLUSTER, false, 1);
		properties.validateString(CONNECTOR_PSM, false, 1);
	}
}
