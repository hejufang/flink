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

package org.apache.flink.table.descriptors;

import org.apache.flink.connectors.rpc.RPCRequestFailureStrategy;

/**
 * Validator for RPC Connector.
 */
public class RPCValidator extends ConnectorDescriptorValidator {
	public static final String RPC = "rpc";

	public static final String CONNECTOR_CONSUL = "connector.consul";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_PSM = "connector.psm";
	//only used for internal test, RPC host port will bind to this setting instead of consul discovery
	public static final String CONNECTOR_TEST_HOST_PORT = "connector.test.host-port";
	public static final String CONNECTOR_THRIFT_SERVICE_CLASS = "connector.thrift-service-class";
	public static final String CONNECTOR_THRIFT_METHOD = "connector.thrift-method";
	/**
	 * Key for describing the transport of thrift client,
	 * This should be consistent with transport of the server. Otherwise a SocketTimeoutException may be thrown
	 * when the connector tries to connect to server.
	 */
	public static final String CONNECTOR_CLIENT_TRANSPORT = "connector.client.transport";

	public static final String CONNECTOR_TIMEOUT_MS = "connector.connect-timeout-ms";
	public static final String CONNECTOR_RESPONSE_VALUE = "connector.response-value";
	public static final String CONNECTOR_RPC_TYPE = "connector.rpc-type";
	public static final String CONNECTOR_CONSUL_INTERVAL = "connector.consul-interval-second";
	public static final String CONNECTOR_CONNECTION_POOL_SIZE = "connector.connection-pool-size";

	public static final String CONNECTOR_BATCH_CLASS = "connector.batch-class";
	//the inner request list field name in request body.
	public static final String CONNECTOR_REQUEST_LIST_NAME = "connector.request-list-name";
	//the inner result list field name in response body.
	public static final String CONNECTOR_RESPONSE_LIST_NAME = "connector.response-list-name";
	//If user want enable batch lookup feature, he must set it to true. By default, batch lookup is disabled.
	public static final String CONNECTOR_USE_BATCH_LOOKUP = "connector.use-batch-lookup";

	//used in sink
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_FLUSH_TIMEOUT_MS = "connector.flush-timeout-ms";
	public static final String CONNECTOR_BATCH_CONSTANT_VALUE = "connector.batch-constant-value";

	//RPC lookup
	public static final String CONNECTOR_IS_DIMENSION_TABLE = "connector.is-dimension-table";
	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";
	public static final String CONNECTOR_LOOKUP_REQUEST_FAILURE_STRATEGY = "connector.lookup.request-failure-strategy";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, RPC, false);

		properties.validateString(CONNECTOR_CONSUL, false, 1);
		properties.validateString(CONNECTOR_CLUSTER, true, 1);
		properties.validateString(CONNECTOR_PSM, true, 1);
		properties.validateString(CONNECTOR_TEST_HOST_PORT, true, 1);
		properties.validateString(CONNECTOR_THRIFT_SERVICE_CLASS, false, 1);
		properties.validateString(CONNECTOR_THRIFT_METHOD, false, 1);
		properties.validateString(CONNECTOR_CLIENT_TRANSPORT, true, 1);

		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1000, 600_000); // 1s ~ 10min
		properties.validateString(CONNECTOR_RESPONSE_VALUE, true, 1);
		properties.validateString(CONNECTOR_RPC_TYPE, true, 1);
		properties.validateInt(CONNECTOR_CONSUL_INTERVAL, true, 5); // 5 second
		properties.validateInt(CONNECTOR_CONNECTION_POOL_SIZE, true, 1, 20); // 1 ~ 20

		properties.validateString(CONNECTOR_BATCH_CLASS, true, 1);
		properties.validateString(CONNECTOR_REQUEST_LIST_NAME, true, 1);
		properties.validateString(CONNECTOR_RESPONSE_LIST_NAME, true, 1);
		properties.validateBoolean(CONNECTOR_USE_BATCH_LOOKUP, true);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateInt(CONNECTOR_FLUSH_TIMEOUT_MS, true, 100);
		properties.validateString(CONNECTOR_BATCH_CONSTANT_VALUE, true, 1);

		properties.validateBoolean(CONNECTOR_IS_DIMENSION_TABLE, true);
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, true, 1);
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_TTL, true, 1);
		properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true, 1);
		properties.validateBoolean(CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY, true);

		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);

		properties.validateEnumValues(CONNECTOR_LOOKUP_REQUEST_FAILURE_STRATEGY, true,
			RPCRequestFailureStrategy.getAllDisplayNames());
	}
}
