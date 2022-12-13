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

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options relating to the SlowTask.
 */
@PublicEvolving
public class SlowTaskOptions {
	public static final ConfigOption<Boolean> SLOW_TASK_ENABLED =
		key("slow-task.check.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Flag to activate/deactivate slow task.");

	public static final ConfigOption<Duration> SLOW_TASK_CHECK_INTERVAL =
		key("slow-task.check.interval")
			.durationType()
			.defaultValue(Duration.ofMinutes(15))
			.withDescription("The interval between two consecutive slow task checks.");

	public static final ConfigOption<String> ENGINE_EXTERNAL_SERVICE_BASE_URL =
		key("engine-external-service.base-url")
			.stringType()
			.noDefaultValue()
			.withDescription("The external service url will handle slow task.");

	public static final ConfigOption<String> SLOW_TASK_EXTERNAL_SERVICE_CHECK_API =
		key("slow-task.external-service.check-api")
			.stringType()
			.defaultValue("/v1/engine/internal/slow-task")
			.withDescription("THe external service api call to handle slow task.");

	public static final ConfigOption<String> EXTERNAL_EXTERNAL_SERVICE_JOB_UNIQUE_KEY =
		key("engine-external-service.job-unique-key")
			.stringType()
			.noDefaultValue()
			.withDescription("The job unique key of upper service");
}
