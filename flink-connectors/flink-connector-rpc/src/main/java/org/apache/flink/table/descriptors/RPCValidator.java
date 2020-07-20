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

/**
 * Validator for RPC Connector.
 */
public class RPCValidator extends ConnectorDescriptorValidator {
	public static final String RPC = "rpc";

	public static final String CONNECTOR_CONSUL = "connector.consul";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_THRIFT_SERVICE_CLASS = "connector.thrift-service-class";
	public static final String CONNECTOR_THRIFT_METHOD = "connector.thrift-method";

	public static final String CONNECTOR_TIMEOUT_MS = "connector.connect-timeout-ms";
	public static final String CONNECTOR_RESPONSE_VALUE = "connector.response-value";
	public static final String CONNECTOR_RPC_TYPE = "connector.rpc-type";
	public static final String CONNECTOR_CONSUL_INTERVAL = "connector.consul-interval-second";
	public static final String CONNECTOR_CONNECTION_POOL_SIZE = "connector.connection-pool-size";

	public static final String CONNECTOR_BATCH_CLASS = "connector.batch-class";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_FLUSH_TIMEOUT_MS = "connector.flush-timeout-ms";
	public static final String CONNECTOR_BATCH_CONSTANT_VALUE = "connector.batch-constant-value";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, RPC, false);

		properties.validateString(CONNECTOR_CONSUL, false, 1);
		properties.validateString(CONNECTOR_CLUSTER, true, 1);
		properties.validateString(CONNECTOR_THRIFT_SERVICE_CLASS, false, 1);
		properties.validateString(CONNECTOR_THRIFT_METHOD, false, 1);

		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1000, 600_000); // 1s ~ 10min
		properties.validateString(CONNECTOR_RESPONSE_VALUE, true, 1);
		properties.validateString(CONNECTOR_RPC_TYPE, true, 1);
		properties.validateInt(CONNECTOR_CONSUL_INTERVAL, true, 5); // 5 second
		properties.validateInt(CONNECTOR_CONNECTION_POOL_SIZE, true, 1, 20); // 1 ~ 20

		properties.validateString(CONNECTOR_BATCH_CLASS, true, 1);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateInt(CONNECTOR_FLUSH_TIMEOUT_MS, true, 100);
		properties.validateString(CONNECTOR_BATCH_CONSTANT_VALUE, true, 1);

		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
	}

}
