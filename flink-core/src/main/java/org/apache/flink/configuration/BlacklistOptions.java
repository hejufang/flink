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

	public static final ConfigOption<String> EXCEPTION_EFFECTIVE_TIME = ConfigOptions
			.key("blacklist.exception-effective-time")
			.stringType()
			.defaultValue("30s")
			.withDescription("The effective time for an exception to calculating blacklist. The exception happened before this time should be ignored in some handlers.");

	public static final ConfigOption<String> NETWORK_EXCEPTION_EXPIRE_TIME = ConfigOptions
			.key("blacklist.network.exception-expire-time")
			.stringType()
			.defaultValue("10 min")
			.withDescription("The expire time of network failures. The exception exceeding this time will be removed from the exception list.");

	public static final ConfigOption<String> NETWORK_TIMESTAMP_RECORD_EXPIRE_TIME = ConfigOptions
			.key("blacklist.network.timestamp-record-expire-time")
			.stringType()
			.defaultValue("1 min")
			.withDescription("The expire time for timestamp record of network failures. NetworkFailureHandler will save " +
					"all the timestamp of network failures by task manager id and remote address. The network failures from " +
					"the same task manager to the same remote address within this expire time will be skipped.");

	public static final ConfigOption<Float> NETWORK_MEAN_SD_RATIO_ALL_BLOCKED_THRESHOLD = ConfigOptions
			.key("blacklist.network.mean-sd-ratio-all-blocked-threshold")
			.floatType()
			.defaultValue(0.5f)
			.withDescription("The threshold of ratio for mean and standard derivation (sd) of the number of failures" +
					" in hosts that all the host should be considered as blacked hosts. For example, if the threshold" +
					" is r and 'sd < mean * r', then all the hosts should be added to the blacklist.");

	public static final ConfigOption<Float> NETWORK_MEAN_SD_RATIO_SOME_BLOCKED_THRESHOLD = ConfigOptions
			.key("blacklist.network.mean-sd-ratio-some-blocked-threshold")
			.floatType()
			.defaultValue(1.0f)
			.withDescription("The threshold of ratio for 1. subtraction between number of failures of one host and mean" +
					" of the number of failures in all hosts and 2. the standard derivation (sd) of the number of" +
					" failures in all hosts that this host should be considered as a blacked host. For example, if the" +
					" threshold is r, then those hosts that have number of exceptions greater than 'mean + r * sd' should" +
					" be added to the blacklist.");

	public static final ConfigOption<Float> NETWORK_EXPECTED_BLOCKED_HOST_RATIO = ConfigOptions
			.key("blacklist.network.expected-blocked-host-ratio")
			.floatType()
			.defaultValue(0.2f)
			.withDescription("The ratio of maximum expected number of hosts that should be added to the blacklist.");

	public static final ConfigOption<Integer> NETWORK_EXPECTED_MIN_HOST = ConfigOptions
			.key("blacklist.network.expected-min-host")
			.intType()
			.defaultValue(5)
			.withDescription("The minimum expected number of hosts for one Flink job.");

	// ---------------------------------------------------------------------------------------------

	/** Not intended to be instantiated. */
	private BlacklistOptions() {}
}
