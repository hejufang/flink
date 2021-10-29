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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * ShuffleOptions. Currently only used for Cloud Shuffle Service.
 */
public class ShuffleOptions {

	/** Whether css is enabled. */
	public static final ConfigOption<Boolean> CLOUD_SHUFFLE_SERVICE_ENABLED = ConfigOptions
		.key("cloud-shuffle-service.enabled")
		.defaultValue(false)
		.withDescription("Whether css is enabled.");

	/** Whether to allow partial record in a buffer. */
	public static final ConfigOption<Boolean> SHUFFLE_ALLOW_PARTIAL_RECORD = ConfigOptions
		.key("shuffle.allow-partial-record")
		.defaultValue(true)
		.withDescription("Whether to allow partial record in a buffer.");
}
