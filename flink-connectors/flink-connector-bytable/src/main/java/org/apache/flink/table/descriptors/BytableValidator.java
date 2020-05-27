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

import java.util.Arrays;

import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_DELAY_MS;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_MAX_TIMES;
import static org.apache.flink.table.utils.RetryUtils.CONNECTOR_RETRY_STRATEGY;

/**
 * The validator for Bytable.
 */
public class BytableValidator extends ConnectorDescriptorValidator {
	public static final String BYTABLE = "bytable";
	public static final String CONNECTOR_MASTER_URLS = "connector.master-urls";
	public static final String CONNECTOR_BYTABLE_CLIENT_METRIC = "connector.bytable-client-metric";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_THREAD_POOL_SIZE = "connector.thread-pool-size";
	public static final String CONNECTOR_MASTER_TIMEOUT_MS = "connector.master-timeout-ms";
	public static final String CONNECTOR_TABLE_SERVER_CONNECT_TIMEOUT_MS = "connector.table-server-connect-timeout-ms";
	public static final String CONNECTOR_TABLE_SERVER_READ_TIMEOUT_MS = "connector.table-server-read-timeout-ms";
	public static final String CONNECTOR_TABLE_SERVER_WRITE_TIMEOUT_MS = "connector.table-server-write-timeout-ms";
	public static final String CONNECTOR_CACHE_TYPE = "connector.cache-type";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String EXPONENTIAL_BACKOFF = "exponential-backoff";
	public static final String FIXED_DELAY = "fixed-delay";
	public static final String ON_DEMAND = "OnDemand";
	public static final String FULL_INFO = "FullInfo";
	public static final String CONNECTOR_TTL_SECONDS = "connector.ttl-seconds";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, BYTABLE, false);
		properties.validateString(CONNECTOR_MASTER_URLS, false, 1);
		properties.validateString(CONNECTOR_TABLE, false, 1);
		properties.validateString(CONNECTOR_BYTABLE_CLIENT_METRIC, true);
		properties.validateInt(CONNECTOR_THREAD_POOL_SIZE, true, 1);
		properties.validateInt(CONNECTOR_MASTER_TIMEOUT_MS, true, 1);
		properties.validateInt(CONNECTOR_TABLE_SERVER_CONNECT_TIMEOUT_MS, true, 1);
		properties.validateInt(CONNECTOR_TABLE_SERVER_READ_TIMEOUT_MS, true, 1);
		properties.validateInt(CONNECTOR_TABLE_SERVER_WRITE_TIMEOUT_MS, true, 1);
		properties.validateEnumValues(CONNECTOR_CACHE_TYPE, true, Arrays.asList(ON_DEMAND, FULL_INFO));
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateEnumValues(CONNECTOR_RETRY_STRATEGY, true,
			Arrays.asList(EXPONENTIAL_BACKOFF, FIXED_DELAY));
		properties.validateInt(CONNECTOR_RETRY_MAX_TIMES, true, 1);
		properties.validateInt(CONNECTOR_RETRY_DELAY_MS, true, 1);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
		properties.validateLong(CONNECTOR_TTL_SECONDS, true, 1);
	}
}
