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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Validator for LogHouse Connector.
 */
public class LogHouseValidator extends ConnectorDescriptorValidator {
	public static final String LOG_HOUSE = "loghouse";

	public static final String CONNECTOR_FLUSH_MAX_RETRIES = "connector.flush-max-retries";
	public static final String CONNECTOR_FLUSH_TIMEOUT_MS = "connector.flush-timeout-ms";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size-kb";

	public static final String CONNECTOR_TIMEOUT_MS = "connector.connect-timeout-ms";

	public static final String CONNECTOR_NAMESPACE = "connector.namespace";
	public static final String CONNECTOR_CONSUL = "connector.consul";

	public static final String CONNECTOR_CONSUL_INTERVAL = "connector.consul-interval-second";
	public static final String CONNECTOR_CONNECTION_POOL_SIZE = "connector.connection-pool-size";

	// e.g. connectors.keys-index.0.clustering = 1, connector.keys-index.0.partition = 2
	public static final String CONNECTOR_KEYS_INDEX = "connector.keys-index";
	public static final String CLUSTERING = "clustering";
	public static final String PARTITION = "partition";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);

		properties.validateInt(CONNECTOR_FLUSH_MAX_RETRIES, true);
		properties.validateInt(CONNECTOR_FLUSH_TIMEOUT_MS, true, 100);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 512, 1024 * 20); // 512KB ~ 20MB

		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1000, 600_000); // 1s ~ 10min

		properties.validateString(CONNECTOR_NAMESPACE, false, 1);
		properties.validateString(CONNECTOR_CONSUL, false, 1);

		properties.validateInt(CONNECTOR_CONSUL_INTERVAL, true, 5); // 5 second
		properties.validateInt(CONNECTOR_CONNECTION_POOL_SIZE, true, 1, 20); // 1 ~ 20

		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);

		validateKeysIndex(properties);
	}

	private void validateKeysIndex(DescriptorProperties properties) {
		final Map<String, Consumer<String>> keysIndexValidators = new HashMap<>();
		keysIndexValidators.put(PARTITION, (key) -> properties.validateInt(key, true, 0));
		keysIndexValidators.put(CLUSTERING, (key) -> properties.validateInt(key, true, 0));
		properties.validateFixedIndexedProperties(CONNECTOR_KEYS_INDEX, false, keysIndexValidators);
	}
}
