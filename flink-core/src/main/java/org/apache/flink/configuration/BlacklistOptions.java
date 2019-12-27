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
 * The set of configuration options relating to the BlacklistTracker.
 */
@PublicEvolving
public class BlacklistOptions {

	public static final ConfigOption<Boolean> BLACKLIST_ENABLED = ConfigOptions
			.key("blacklist.enabled")
			.defaultValue(false)
			.withDescription("Enable blacklist mechanism if set to true.");

	public static final ConfigOption<Integer> JOB_MAX_TASK_FAILURE_PER_HOST = ConfigOptions
			.key("blacklist.job.max-task-failure-per-host")
			.defaultValue(2)
			.withDescription("Maximum task failure of a same vertex before added the Host to (Job, Host) blacklist.");

	public static final ConfigOption<Integer> SESSION_MAX_JOB_FAILURE_PER_HOST = ConfigOptions
			.key("blacklist.session.max-job-failure-per-host")
			.defaultValue(2)
			.withDescription("Maximum number of times for a Host being added to (Job, Host) blacklist " +
					"before added the Host to (Session, Host) blacklist.");

	public static final ConfigOption<Integer> SESSION_MAX_TASKMANAGER_FAILURE_PER_HOST = ConfigOptions
			.key("blacklist.session.max-taskmanager-failure-per-host")
			.defaultValue(2)
			.withDescription("Maximum TaskManager failure (launching failure or heartbeat timeout with ResourceManager) of " +
					"a same host before added the Host to (Session, Host) blacklist.");

	public static final ConfigOption<Integer> JOB_BLACKLIST_LENGTH = ConfigOptions
			.key("blacklist.job-blacklist-length")
			.defaultValue(10)
			.withDescription("Maximum length of (Job, Host) blacklist.");

	public static final ConfigOption<Integer> SESSION_BLACKLIST_LENGTH = ConfigOptions
			.key("blacklist.session-blacklist-length")
			.defaultValue(10)
			.withDescription("Maximum length of (Session, Host) blacklist.");

	public static final ConfigOption<String> FAILURE_TIMEOUT = ConfigOptions
			.key("blacklist.failure-timeout")
			.defaultValue("60 min")
			.withDescription("Only failures in the time window are taken into consideration. " +
					"the timeout value requires a time-unit specifier (ms/s/min/h/d).");

	public static final ConfigOption<String> CHECK_INTERVAL = ConfigOptions
			.key("blacklist.check-interval")
			.defaultValue("5 min")
			.withDescription("Interval to check failure is timeout." +
					"the timeout value requires a time-unit specifier (ms/s/min/h/d).");

	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private BlacklistOptions() {}
}
