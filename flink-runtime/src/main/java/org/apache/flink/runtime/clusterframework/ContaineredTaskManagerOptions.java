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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ContaineredTaskManagerOptions {
	public static final ConfigOption<String> METASPACE_SIZE = ConfigOptions
		.key("taskmanager.memory.jvm.metaspace")
		.defaultValue("100MB")
		.withDescription("Metaspace size MB for TaskManager.");

	public static final ConfigOption<Boolean> ENABLE_METASPACE_SIZE = ConfigOptions
		.key("taskmanager.memory.jvm.metaspace.enabled")
		.defaultValue(false)
		.withDescription("Enable metaspace limit for TaskManager, default not enabled.");

	public static final ConfigOption<Float> NON_HEAP_DIRECT_FRACTION = ConfigOptions
		.key("taskmanager.memory.non_heap_direct.fraction")
		.defaultValue(0.1f)
		.withDescription("Cutoff fraction (from container memory) for non heap and non direct memory usage.");

	public static final ConfigOption<String> NON_HEAP_DIRECT_MAX = ConfigOptions
		.key("taskmanager.memory.non_heap_direct.max")
		.defaultValue("30G")
		.withDescription("Cutoff max for non heap and non direct memory usage.");

	public static final ConfigOption<String> NON_HEAP_DIRECT_MIN = ConfigOptions
		.key("taskmanager.memory.non_heap_direct.min")
		.defaultValue("300M")
		.withDescription("Cutoff min for non heap and non direct memory usage.");

	public static final ConfigOption<Boolean> ENABLE_NON_HEAP_DIRECT_FRACTION = ConfigOptions
		.key("taskmanager.memory.non_heap_direct.enabled")
		.defaultValue(false)
		.withDescription("Enable Cutoff fraction for non heap non direct memory usage, default not enabled.");
}
