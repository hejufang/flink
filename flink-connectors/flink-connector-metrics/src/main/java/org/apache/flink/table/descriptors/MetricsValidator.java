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
 * The metrics validator.
 */
public class MetricsValidator extends ConnectorDescriptorValidator {
	public static final String METRICS = "metrics";
	public static final String CONNECTOR_METRICS_PREFIX = "connector.metrics-prefix";
	public static final String CONNECTOR_WRITE_FLUSH_MAX_ROWS = "connector.write.flush.max-rows";
	public static final String CONNECTOR_WRITE_FLUSH_INTERVAL = "connector.write.flush.interval";
	public static final String CONNECTOR_LOG_FAILURES_ONLY = "connector.log-failures-only";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, METRICS, false);
		properties.validateString(CONNECTOR_METRICS_PREFIX, false, 1);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
		properties.validateInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS, true, 1);
		properties.validateInt(CONNECTOR_WRITE_FLUSH_INTERVAL, true, 1);
		properties.validateBoolean(CONNECTOR_LOG_FAILURES_ONLY, true);
	}
}
