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

package org.apache.flink.runtime.executiongraph.speculation;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class SpeculationOptions {

	public static final ConfigOption<Boolean> SPECULATION_ENABLED =
			key("speculation.enabled")
					.defaultValue(false)
					.withDescription("Whether enable the speculation feature");

	public static final ConfigOption<Long> SPECULATION_INTERVAL =
			key("speculation.interval")
					.defaultValue(60L * 1000L)
					.withDescription("The interval of triggering speculation");

	public static final ConfigOption<Double> SPECULATION_QUANTILE =
			key("speculation.quantile")
					.defaultValue(0.75)
					.withDescription("it specifies how many tasks must finish before enabling " +
							"the speculation for given stage. It's expressed by the fraction and " +
							"by default the value is 0.75 (75%)");

	public static final ConfigOption<Double> SPECULATION_MULTIPLIER =
			key("speculation.multiplier")
					.defaultValue(1.5)
					.withDescription("task slowness is checked against the median " +
							"time of execution of all launched tasks. " +
							"This entry defines how many times slower a task must be to be " +
							"considered for speculation. The default value is 1.5. " +
							"It means that tasks running 1.5 times slower than " +
							"the median will be taken into account for speculation.");

	public static final ConfigOption<Double> SPECULATION_CREDITS_PERCENTAGE =
			key("speculation.credits.percentage")
					.defaultValue(0.05)
					.withDescription("Number of speculative tasks running at the same time in percent with respect to the total task number. default 0.05");
}
