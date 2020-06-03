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

package org.apache.flink.runtime.failurerate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import java.util.concurrent.TimeUnit;

/**
 * Failure rate util.
 */
public class FailureRaterUtil {

	public static FailureRater createFailureRater(
		Configuration configuration) {
		double ratio = configuration.getDouble(ResourceManagerOptions.MAXIMUM_WORKERS_FAILURE_RATE_RATIO);
		double rate = configuration.getDouble(ResourceManagerOptions.MAXIMUM_WORKERS_FAILURE_RATE);
		long failureIntervalMs = configuration.getLong(ResourceManagerOptions.WORKERS_FAILURE_INTERVAL_MS);
		// rate and ratio are both disabled，use NoFailureRater
		if (rate < 0 && ratio < 0) {
			return new NoFailureRater((int) failureIntervalMs / 1000);
		}
		// rate is enabled，but ratio is disabled，use TimestampBasedFailureRater
		if (rate >= 0 && ratio < 0) {
			int maximumRate = Double.valueOf(rate).intValue();
			return new TimestampBasedFailureRater(maximumRate < rate ? maximumRate + 1 : maximumRate, Time.of(failureIntervalMs, TimeUnit.MILLISECONDS));
		}
		// otherwise use TimestampTMNumBasedFailureRater
		int numTaskSlots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		return new TimestampTMNumBasedFailureRater(ratio, Time.of(failureIntervalMs, TimeUnit.MILLISECONDS), numTaskSlots, rate);
	}
}
