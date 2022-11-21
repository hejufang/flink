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

package org.apache.flink.metrics;

/**
 * The type of {@link Message}.
 */
public enum MessageType {

	CHECKPOINT("checkpoint"),

	CHECKPOINT_CONFIG("checkpoint_config"),

	ORIGINAL_METRICS("original_metrics"),

	BLACKLIST("blacklist"),

	JOB_CONFIG("job_config"),

	JOB_START_EVENT("job_start_event"),

	SNAPSHOT("snapshot"),

	RESTORE("restore"),

	CACHE_LAYER("cache_layer"),

	CHECKPOINT_PLACEHOLDER("checkpoint_placeholder"),

	LOCAL_STATE("local_state"),

	TASK_SHUFFLE_INFO("task_shuffle_info"),

	FLINK_BATCH("flink_batch"),

	JOB_LATENCY("job_latency"),

	CONNECTOR_SINK_LATENCY("connector_sink_latency");

	private final String name;

	MessageType(final String name) {
		this.name = name;
	}
}