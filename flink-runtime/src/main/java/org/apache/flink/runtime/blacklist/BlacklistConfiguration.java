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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlacklistOptions;
import org.apache.flink.configuration.Configuration;

import scala.concurrent.duration.Duration;

/**
 * Configuration for blacklist.
 */
public class BlacklistConfiguration {
	private final boolean taskManagerBlacklistEnabled;
	private final boolean taskBlacklistEnabled;
	private final boolean blacklistCriticalEnable;
	private final int maxTaskFailureNumPerHost;
	private final int maxTaskManagerFailureNumPerHost;
	private final int taskBlacklistMaxLength;
	private final int taskManagerBlacklistMaxLength;
	private final Time failureTimeout;
	private final Time checkInterval;

	public BlacklistConfiguration(
			boolean taskManagerBlacklistEnabled,
			boolean taskBlacklistEnabled,
			boolean blacklistCriticalEnable,
			int maxTaskFailureNumPerHost,
			int maxTaskManagerFailureNumPerHost,
			int taskBlacklistMaxLength,
			int taskManagerBlacklistMaxLength,
			Time failureTimeout,
			Time checkInterval) {
		this.taskManagerBlacklistEnabled = taskManagerBlacklistEnabled;
		this.taskBlacklistEnabled = taskBlacklistEnabled;
		this.blacklistCriticalEnable = blacklistCriticalEnable;
		this.maxTaskFailureNumPerHost = maxTaskFailureNumPerHost;
		this.maxTaskManagerFailureNumPerHost = maxTaskManagerFailureNumPerHost;
		this.taskBlacklistMaxLength = taskBlacklistMaxLength;
		this.taskManagerBlacklistMaxLength = taskManagerBlacklistMaxLength;
		this.failureTimeout = failureTimeout;
		this.checkInterval = checkInterval;
	}

	public boolean isTaskManagerBlacklistEnabled() {
		return taskManagerBlacklistEnabled;
	}

	public boolean isTaskBlacklistEnabled() {
		return taskBlacklistEnabled;
	}

	public boolean getCriticalErrorEnabled() {
		return blacklistCriticalEnable;
	}

	public int getMaxTaskFailureNumPerHost() {
		return maxTaskFailureNumPerHost;
	}

	public int getMaxTaskManagerFailureNumPerHost() {
		return maxTaskManagerFailureNumPerHost;
	}

	public int getTaskBlacklistMaxLength() {
		return taskBlacklistMaxLength;
	}

	public int getTaskManagerBlacklistMaxLength() {
		return taskManagerBlacklistMaxLength;
	}

	public Time getFailureTimeout() {
		return failureTimeout;
	}

	public Time getCheckInterval() {
		return checkInterval;
	}

	@Override
	public String toString() {
		return "BlacklistConfiguration { " +
				"taskManagerBlacklistEnabled: " + taskManagerBlacklistEnabled +
				", taskBlacklistEnabled: " + taskBlacklistEnabled +
				", criticalErrorEnabled: " + blacklistCriticalEnable +
				", maxTaskFailureNumPerHost: " + maxTaskFailureNumPerHost +
				", maxTaskManagerFailureNumPerHost: " + maxTaskManagerFailureNumPerHost +
				", taskBlacklistMaxLength: " + taskBlacklistMaxLength +
				", taskManagerBlacklistMaxLength: " + taskManagerBlacklistMaxLength +
				", failureTimeout: " + failureTimeout +
				", checkInterval: " + checkInterval +
				"}";
	}

	public static BlacklistConfiguration fromConfiguration(Configuration configuration) {
		boolean taskManagerBlacklistEnabled = configuration.getBoolean(
				BlacklistOptions.TASKMANAGER_BLACKLIST_ENABLED);
		boolean taskBlacklistEnabled = configuration.getBoolean(
				BlacklistOptions.TASK_BLACKLIST_ENABLED);
		boolean blacklistCriticalEnable = configuration.getBoolean(
				BlacklistOptions.TASKMANAGER_BLACKLIST_CRITICAL_ERROR_ENABLED);
		int maxTaskFailureNumPerHost = configuration.getInteger(
				BlacklistOptions.MAX_TASK_FAILURE_NUM_PER_HOST);
		int maxTaskManagerFailureNumPerHost = configuration.getInteger(
				BlacklistOptions.MAX_TASKMANAGER_FAILURE_NUM_PER_HOST);
		int taskBlacklistMaxLength = configuration.getInteger(
				BlacklistOptions.TASK_BLACKLIST_MAX_LENGTH);
		int taskManagerBlacklistMaxLength = configuration.getInteger(
				BlacklistOptions.TASKMANAGER_BLACKLIST_MAX_LENGTH);
		Time failureTimeout = Time.milliseconds(
				Duration.apply(configuration.getString(BlacklistOptions.FAILURE_TIMEOUT)).toMillis());
		Time checkInterval = Time.milliseconds(
				Duration.apply(configuration.getString(BlacklistOptions.CHECK_INTERVAL)).toMillis());
		return new BlacklistConfiguration(
				taskManagerBlacklistEnabled,
				taskBlacklistEnabled,
				blacklistCriticalEnable,
				maxTaskFailureNumPerHost,
				maxTaskManagerFailureNumPerHost,
				taskBlacklistMaxLength,
				taskManagerBlacklistMaxLength,
				failureTimeout,
				checkInterval);
	}
}
