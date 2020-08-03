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
 * The validator for Faas.
 */
public class FaasValidator extends ConnectorDescriptorValidator {
	public static final String FAAS = "faas";
	public static final String CONNECTOR_URL = "connector.url";
	public static final String CONNECTOR_TIMEOUT_MS = "connector.timeout-ms";
	public static final String CONNECTOR_MAX_CONNECTIONS = "connector.max-connections";
	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, FAAS, false);
		properties.validateString(CONNECTOR_URL, false, 1);
		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1);
		properties.validateInt(CONNECTOR_MAX_CONNECTIONS, true, 1);
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, true, 1);
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_TTL, true, 1);
		properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true, 1);
	}
}
