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

import java.util.Arrays;

/**
 * Databus validator.
 */
public class DatabusValidator extends ConnectorDescriptorValidator {
	public static final String DATABUS = "databus";
	public static final String CONNECTOR_CHANNEL = "connector.channel";
	public static final String CONNECTOR_RETRY_STRATEGY = "connector.retry-strategy";
	public static final String CONNECTOR_RETRY_MAX_TIMES = "connector.retry-max-times";
	public static final String CONNECTOR_RETRY_DELAY_MS = "connector.retry-delay-ms";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_DATABUS_MAX_BUFFER_BYTES = "connector.databus-max-buffer-bytes";
	public static final String CONNECTOR_DATABUS_NEED_RESPONSE = "connector.databus-need-response";
	public static final String EXPONENTIAL_BACKOFF = "exponential-backoff";
	public static final String FIXED_DELAY = "fixed-delay";

	public static final int CONNECTOR_RETRY_MAX_TIMES_DEFAULT = 3;
	public static final int CONNECTOR_RETRY_DELAY_MS_DEFAULT = 1000;

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateString(CONNECTOR_CHANNEL, false, 1);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateEnumValues(CONNECTOR_RETRY_STRATEGY, true, Arrays.asList(EXPONENTIAL_BACKOFF, FIXED_DELAY));
		properties.validateInt(CONNECTOR_RETRY_MAX_TIMES, true, 1);
		properties.validateInt(CONNECTOR_RETRY_DELAY_MS, true, 1);
		properties.validateLong(CONNECTOR_DATABUS_MAX_BUFFER_BYTES, true, 1);
		properties.validateBoolean(CONNECTOR_DATABUS_NEED_RESPONSE, true);
	}
}
