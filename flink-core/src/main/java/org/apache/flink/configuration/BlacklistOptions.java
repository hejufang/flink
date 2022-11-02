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
import java.util.List;

/**
 * The set of configuration options relating to the BlacklistTracker.
 */
@PublicEvolving
public class BlacklistOptions {

	public static final ConfigOption<Boolean> TASKMANAGER_BLACKLIST_ENABLED = ConfigOptions
			.key("blacklist.taskmanager.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Enable blacklist mechanism if set to true.");

	public static final ConfigOption<Boolean> TASK_BLACKLIST_ENABLED = ConfigOptions
			.key("blacklist.task.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Enable blacklist mechanism if set to true.");

	public static final ConfigOption<Boolean> TASKMANAGER_BLACKLIST_CRITICAL_ERROR_ENABLED = ConfigOptions
		.key("blacklist.taskmanager.critical.error.enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("Enable blacklist critical error mechanism if set to true.");


	public static final ConfigOption<Integer> MAX_TASK_FAILURE_NUM_PER_HOST = ConfigOptions
			.key("blacklist.max-task-failure-num-per-host")
			.intType()
			.defaultValue(2)
			.withDescription("Maximum task failure of a same vertex before added the Host to (Job, Host) blacklist.");

	public static final ConfigOption<Integer> MAX_TASKMANAGER_FAILURE_NUM_PER_HOST = ConfigOptions
			.key("blacklist.max-taskmanager-failure-num-per-host")
			.intType()
			.defaultValue(2)
			.withDescription("Maximum TaskManager failure (launching failure or heartbeat timeout with ResourceManager) of " +
					"a same host before added the Host to (Session, Host) blacklist.");

	public static final ConfigOption<Integer> TASK_BLACKLIST_MAX_LENGTH = ConfigOptions
			.key("blacklist.task-blacklist-max-length")
			.intType()
			.defaultValue(10)
			.withDescription("Maximum length of (Job, Host) blacklist.");

	public static final ConfigOption<Integer> TASKMANAGER_BLACKLIST_MAX_LENGTH = ConfigOptions
			.key("blacklist.taskmanager-blacklist-max-length")
			.intType()
			.defaultValue(10)
			.withDescription("Maximum length of (Session, Host) blacklist.");

	public static final ConfigOption<String> FAILURE_TIMEOUT = ConfigOptions
			.key("blacklist.failure-timeout")
			.stringType()
			.defaultValue("60 min")
			.withDescription("Only failures in the time window are taken into consideration. " +
					"the timeout value requires a time-unit specifier (ms/s/min/h/d).");

	public static final ConfigOption<String> CHECK_INTERVAL = ConfigOptions
			.key("blacklist.check-interval")
			.stringType()
			.defaultValue("5 min")
			.withDescription("Interval to check failure is timeout." +
					"the timeout value requires a time-unit specifier (ms/s/min/h/d).");

	public static final ConfigOption<Duration> REPORTER_LIMITER_FAILURE_INTERVAL = ConfigOptions
			.key("blacklist.reporter-limiter.failure-interval")
			.durationType()
			.defaultValue(Duration.ofMinutes(20))
			.withDescription("Failure interval of reporter limiter.");

	public static final ConfigOption<Integer> REPORTER_LIMITER_MAX_FAILURES_PER_INTERVAL = ConfigOptions
			.key("blacklist.reporter-limiter.max-failures-per-interval")
			.intType()
			.defaultValue(100)
			.withDescription("Failures number exceed this value will stop report to blacklist tracker.");

	public static final ConfigOption<Integer> MAX_FAILURE_NUM = ConfigOptions
			.key("blacklist.max-failures-number")
			.intType()
			.defaultValue(100)
			.withDescription("Max maintained failures for one kind of FailureType." +
					"Earliest failure will dropped when exceed the max number.");

	public static final ConfigOption<Integer> MAX_HOST_PER_EXCEPTION_MIN_NUMBER = ConfigOptions
			.key("blacklist.exception-filter.max-host-per-exception.min-number")
			.intType()
			.defaultValue(5)
			.withDescription("The min number to judge whether exception occurs in too many hosts and should be filtered.");

	public static final ConfigOption<Double> MAX_HOST_PER_EXCEPTION_RATIO = ConfigOptions
			.key("blacklist.exception-filter.max-host-per-exception.ratio")
			.doubleType()
			.defaultValue(0.05)
			.withDescription("The ratio of total register task manager to judge whether exception occurs in too many hosts and should be filtered.");

	public static final ConfigOption<List<String>> IGNORED_EXCEPTION_CLASS_NAMES = ConfigOptions
			.key("blacklist.ignored-exception-class-names")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("List of ignored exception class name, split by ';'.");

	public static final ConfigOption<List<String>> ADDITIONAL_IGNORED_EXCEPTION_CLASS_NAMES = ConfigOptions
			.key("blacklist.ignored-exception-class-names.additional")
			.stringType()
			.asList()
			.noDefaultValue()
			.withDescription("List of additional ignored exception class name, split by ';'.");

	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private BlacklistOptions() {}
}
