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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * {@link ConfigOption}s specific for a single execution of a user program.
 */
@PublicEvolving
public class ExecutionOptions {

	public static final ConfigOption<Long> EXECUTION_CANCELLATION_TIMEOUT =
		ConfigOptions.key("execution.cancellation.timeout.ms")
			.defaultValue(300000L)
			.withDescription("Timeout in milliseconds after which a execution cancellation times out and" +
				" close the TaskManager.");

	public static final ConfigOption<Boolean> EXECUTION_CANCELLATION_TIMEOUT_ENABLE =
		ConfigOptions.key("execution.cancellation.timeout.enable")
			.defaultValue(false)
			.withDescription("enable TaskManager cancellation timeout.");
}
