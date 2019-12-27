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

package org.apache.flink.runtime.blacklisttracker;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlacklistOptions;
import org.apache.flink.configuration.Configuration;

import scala.concurrent.duration.Duration;

/**
 * Configuration for blacklist.
 */
public class BlacklistConfiguration {
	private final boolean blacklistEnabled;
	private final int jobMaxTaskFailurePerHost;
	private final int sessionMaxJobFailurePerHost;
	private final int sessionMaxTaskmanagerFailurePerHost;
	private final int jobBlacklistLength;
	private final int sessionBlacklistLength;
	private final Time failureTimeout;
	private final Time checkInterval;

	public BlacklistConfiguration(
			boolean blacklistEnabled,
			int jobMaxTaskFailurePerHost,
			int sessionMaxJobFailurePerHost,
			int sessionMaxTaskmanagerFailurePerHost,
			int jobBlacklistLength,
			int sessionBlacklistLength,
			Time failureTimeout,
			Time checkInterval) {
		this.blacklistEnabled = blacklistEnabled;
		this.jobMaxTaskFailurePerHost = jobMaxTaskFailurePerHost;
		this.sessionMaxJobFailurePerHost = sessionMaxJobFailurePerHost;
		this.sessionMaxTaskmanagerFailurePerHost = sessionMaxTaskmanagerFailurePerHost;
		this.jobBlacklistLength = jobBlacklistLength;
		this.sessionBlacklistLength = sessionBlacklistLength;
		this.failureTimeout = failureTimeout;
		this.checkInterval = checkInterval;
	}

	public boolean isBlacklistEnabled() {
		return blacklistEnabled;
	}

	public int getJobMaxTaskFailurePerHost() {
		return jobMaxTaskFailurePerHost;
	}

	public int getSessionMaxJobFailurePerHost() {
		return sessionMaxJobFailurePerHost;
	}

	public int getSessionMaxTaskmanagerFailurePerHost() {
		return sessionMaxTaskmanagerFailurePerHost;
	}

	public int getJobBlacklistLength() {
		return jobBlacklistLength;
	}

	public int getSessionBlacklistLength() {
		return sessionBlacklistLength;
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
				"blacklistEnabled: " + blacklistEnabled +
				", jobMaxTaskFailurePerHost: " + jobMaxTaskFailurePerHost +
				", sessionMaxJobFailurePerHost: " + sessionMaxJobFailurePerHost +
				", sessionMaxTaskmanagerFailurePerHost: " + sessionMaxTaskmanagerFailurePerHost +
				", jobBlacklistLength: " + jobBlacklistLength +
				", sessionBlacklistLength: " + sessionBlacklistLength +
				", failureTimeout: " + failureTimeout +
				", checkInterval: " + checkInterval +
				"}";
	}

	public static BlacklistConfiguration fromConfiguration(Configuration configuration) {
		boolean blacklistEnabled = configuration.getBoolean(
				BlacklistOptions.BLACKLIST_ENABLED);
		int jobMaxTaskFailurePerHost = configuration.getInteger(
				BlacklistOptions.JOB_MAX_TASK_FAILURE_PER_HOST);
		int sessionMaxJobFailurePerHost = configuration.getInteger(
				BlacklistOptions.SESSION_MAX_JOB_FAILURE_PER_HOST);
		int sessionMaxTaskmanagerFailurePerHost = configuration.getInteger(
				BlacklistOptions.SESSION_MAX_TASKMANAGER_FAILURE_PER_HOST);
		int jobBlacklistLength = configuration.getInteger(
				BlacklistOptions.JOB_BLACKLIST_LENGTH);
		int sessionBlacklistLength = configuration.getInteger(
				BlacklistOptions.SESSION_BLACKLIST_LENGTH);
		Time failureTimeout = Time.milliseconds(
				Duration.apply(configuration.getString(BlacklistOptions.FAILURE_TIMEOUT)).toMillis());
		Time checkInterval = Time.milliseconds(
				Duration.apply(configuration.getString(BlacklistOptions.CHECK_INTERVAL)).toMillis());
		return new BlacklistConfiguration(
				blacklistEnabled,
				jobMaxTaskFailurePerHost,
				sessionMaxJobFailurePerHost,
				sessionMaxTaskmanagerFailurePerHost,
				jobBlacklistLength,
				sessionBlacklistLength,
				failureTimeout,
				checkInterval);
	}
}
