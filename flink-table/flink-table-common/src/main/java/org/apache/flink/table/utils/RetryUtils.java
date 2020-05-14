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

package org.apache.flink.table.utils;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.RetryManager;

/**
 * Get retry strategy from the properties.
 */
public class RetryUtils {

	public static final String CONNECTOR_RETRY_STRATEGY = "connector.retry-strategy";
	public static final String CONNECTOR_RETRY_MAX_TIMES = "connector.retry-max-times";
	public static final String CONNECTOR_RETRY_DELAY_MS = "connector.retry-delay-ms";
	public static final String EXPONENTIAL_BACKOFF = "exponential-backoff";
	public static final String FIXED_DELAY = "fixed-delay";

	private static final int CONNECTOR_RETRY_MAX_TIMES_DEFAULT = 3;
	private static final int CONNECTOR_RETRY_DELAY_MS_DEFAULT = 1000;

	public static RetryManager.Strategy getRetryStrategy(DescriptorProperties descriptorProperties) {
		String strategyName = descriptorProperties
			.getOptionalString(CONNECTOR_RETRY_STRATEGY).orElse(null);
		if (strategyName == null) {
			return null;
		}
		int maxRetryTimes = descriptorProperties.getOptionalInt(CONNECTOR_RETRY_MAX_TIMES)
			.orElse(CONNECTOR_RETRY_MAX_TIMES_DEFAULT);
		int retryDelayMs = descriptorProperties.getOptionalInt(CONNECTOR_RETRY_DELAY_MS)
			.orElse(CONNECTOR_RETRY_DELAY_MS_DEFAULT);

		switch (strategyName) {
			case EXPONENTIAL_BACKOFF:
				return RetryManager.createExponentialBackoffStrategy(maxRetryTimes, retryDelayMs);
			case FIXED_DELAY:
				return RetryManager.createFixedDelayStrategy(maxRetryTimes, retryDelayMs);
			default:
				throw new IllegalArgumentException(String.format(
					"Unsupported retry strategy: %s.", strategyName));
		}
	}
}
