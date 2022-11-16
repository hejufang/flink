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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
	private final Duration limiterFailureInterval;
	private final int limiterMaxFailuresPerInterval;
	private final int maxFailureNum;
	private final int maxHostPerExceptionMinNumber;
	private final double maxHostPerExceptionRatio;

	private final List<String> ignoredExceptionClassNames;
	private final Time failureEffectiveTime;

	// network related
	private final Time networkFailureExpireTime;
	private final Time timestampRecordExpireTime;
	private final float meanSdRatioAllBlockedThreshold;
	private final float meanSdRatioSomeBlockedThreshold;
	private final int expectedMinHost;
	private final float expectedBlockedHostRatio;

	public BlacklistConfiguration(
			boolean taskManagerBlacklistEnabled,
			boolean taskBlacklistEnabled,
			boolean blacklistCriticalEnable,
			int maxTaskFailureNumPerHost,
			int maxTaskManagerFailureNumPerHost,
			int taskBlacklistMaxLength,
			int taskManagerBlacklistMaxLength,
			Time failureTimeout,
			Time checkInterval,
			Duration limiterFailureInterval,
			int limiterMaxFailuresPerInterval,
			int maxFailureNum,
			int maxHostPerExceptionMinNumber,
			double maxHostPerExceptionRatio,
			List<String> ignoredExceptionClassNames,
			Time failureEffectiveTime,
			Time networkFailureExpireTime,
			Time timestampRecordExpireTime,
			float meanSdRatioAllBlockedThreshold,
			float meanSdRatioThresholdSomeBlocked,
			int expectedMinHost,
			float expectedBlockedHostRatio) {
		this.taskManagerBlacklistEnabled = taskManagerBlacklistEnabled;
		this.taskBlacklistEnabled = taskBlacklistEnabled;
		this.blacklistCriticalEnable = blacklistCriticalEnable;
		this.maxTaskFailureNumPerHost = maxTaskFailureNumPerHost;
		this.maxTaskManagerFailureNumPerHost = maxTaskManagerFailureNumPerHost;
		this.taskBlacklistMaxLength = taskBlacklistMaxLength;
		this.taskManagerBlacklistMaxLength = taskManagerBlacklistMaxLength;
		this.failureTimeout = failureTimeout;
		this.checkInterval = checkInterval;
		this.limiterFailureInterval = limiterFailureInterval;
		this.limiterMaxFailuresPerInterval = limiterMaxFailuresPerInterval;
		this.maxFailureNum = maxFailureNum;
		this.maxHostPerExceptionMinNumber = maxHostPerExceptionMinNumber;
		this.maxHostPerExceptionRatio = maxHostPerExceptionRatio;
		this.ignoredExceptionClassNames = ignoredExceptionClassNames;
		this.failureEffectiveTime = failureEffectiveTime;
		this.networkFailureExpireTime = networkFailureExpireTime;
		this.timestampRecordExpireTime = timestampRecordExpireTime;
		this.meanSdRatioAllBlockedThreshold = meanSdRatioAllBlockedThreshold;
		this.meanSdRatioSomeBlockedThreshold = meanSdRatioThresholdSomeBlocked;
		this.expectedMinHost = expectedMinHost;
		this.expectedBlockedHostRatio = expectedBlockedHostRatio;
	}

	public boolean isTaskManagerBlacklistEnabled() {
		return taskManagerBlacklistEnabled;
	}

	public boolean isTaskBlacklistEnabled() {
		return taskBlacklistEnabled;
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

	public boolean isBlacklistCriticalEnable() {
		return blacklistCriticalEnable;
	}

	public Duration getLimiterFailureInterval() {
		return limiterFailureInterval;
	}

	public int getLimiterMaxFailuresPerInterval() {
		return limiterMaxFailuresPerInterval;
	}

	public int getMaxFailureNum() {
		return maxFailureNum;
	}

	public int getMaxHostPerExceptionMinNumber() {
		return maxHostPerExceptionMinNumber;
	}

	public double getMaxHostPerExceptionRatio() {
		return maxHostPerExceptionRatio;
	}

	public List<String> getIgnoredExceptionClassNames() {
		return ignoredExceptionClassNames;
	}

	public Time getFailureEffectiveTime() {
		return failureEffectiveTime;
	}

	public Time getNetworkFailureExpireTime() {
		return networkFailureExpireTime;
	}

	public Time getTimestampRecordExpireTime() {
		return timestampRecordExpireTime;
	}

	public float getMeanSdRatioAllBlockedThreshold() {
		return meanSdRatioAllBlockedThreshold;
	}

	public float getMeanSdRatioSomeBlockedThreshold() {
		return meanSdRatioSomeBlockedThreshold;
	}

	public int getExpectedMinHost() {
		return expectedMinHost;
	}

	public float getExpectedBlockedHostRatio() {
		return expectedBlockedHostRatio;
	}

	@Override
	public String toString() {
		return "BlacklistConfiguration{" +
				"taskManagerBlacklistEnabled=" + taskManagerBlacklistEnabled +
				", taskBlacklistEnabled=" + taskBlacklistEnabled +
				", blacklistCriticalEnable=" + blacklistCriticalEnable +
				", maxTaskFailureNumPerHost=" + maxTaskFailureNumPerHost +
				", maxTaskManagerFailureNumPerHost=" + maxTaskManagerFailureNumPerHost +
				", taskBlacklistMaxLength=" + taskBlacklistMaxLength +
				", taskManagerBlacklistMaxLength=" + taskManagerBlacklistMaxLength +
				", failureTimeout=" + failureTimeout +
				", checkInterval=" + checkInterval +
				", limiterFailureInterval=" + limiterFailureInterval +
				", limiterMaxFailuresPerInterval=" + limiterMaxFailuresPerInterval +
				", maxFailureNum=" + maxFailureNum +
				", maxHostPerExceptionMinNumber=" + maxHostPerExceptionMinNumber +
				", maxHostPerExceptionRatio=" + maxHostPerExceptionRatio +
				", ignoredExceptionClassNames=" + ignoredExceptionClassNames +
				", failureEffectiveTime=" + failureEffectiveTime +
				", networkFailureExpireTime=" + networkFailureExpireTime +
				", timestampRecordExpireTime=" + timestampRecordExpireTime +
				", meanSdRatioAllBlockedThreshold=" + meanSdRatioAllBlockedThreshold +
				", meanSdRatioSomeBlockedThreshold=" + meanSdRatioSomeBlockedThreshold +
				", expectedMinHost=" + expectedMinHost +
				", expectedBlockedHostRatio=" + expectedBlockedHostRatio +
				'}';
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
				scala.concurrent.duration.Duration.apply(configuration.getString(BlacklistOptions.FAILURE_TIMEOUT)).toMillis());
		Time checkInterval = Time.milliseconds(
				scala.concurrent.duration.Duration.apply(configuration.getString(BlacklistOptions.CHECK_INTERVAL)).toMillis());
		Duration limiterFailureInterval = configuration.get(BlacklistOptions.REPORTER_LIMITER_FAILURE_INTERVAL);
		int limiterMaxFailuresPerInterval = configuration.getInteger(BlacklistOptions.REPORTER_LIMITER_MAX_FAILURES_PER_INTERVAL);

		int maxFailureNum = configuration.getInteger(BlacklistOptions.MAX_FAILURE_NUM);
		int maxHostPerExceptionMinNumber = configuration.getInteger(BlacklistOptions.MAX_HOST_PER_EXCEPTION_MIN_NUMBER);
		double maxHostPerExceptionRatio = configuration.getDouble(BlacklistOptions.MAX_HOST_PER_EXCEPTION_RATIO);

		final List<String> ignoredExceptionClassNames = configuration.getOptional(BlacklistOptions.IGNORED_EXCEPTION_CLASS_NAMES)
				.orElse(Collections.emptyList());
		final List<String> additionalIgnoredExceptionClassNames = configuration.getOptional(BlacklistOptions.ADDITIONAL_IGNORED_EXCEPTION_CLASS_NAMES)
				.orElse(Collections.emptyList());
		final List<String> allIgnoredExceptionClassNames = new ArrayList<>(ignoredExceptionClassNames.size() + additionalIgnoredExceptionClassNames.size());
		allIgnoredExceptionClassNames.addAll(ignoredExceptionClassNames);
		allIgnoredExceptionClassNames.addAll(additionalIgnoredExceptionClassNames);
		Time failureEffectiveTime = Time.milliseconds(
				scala.concurrent.duration.Duration.apply(configuration.getString(BlacklistOptions.EXCEPTION_EFFECTIVE_TIME)).toMillis());
		// network configurations
		Time networkFailureExpireTime = Time.milliseconds(
				scala.concurrent.duration.Duration.apply(configuration.getString(BlacklistOptions.NETWORK_EXCEPTION_EXPIRE_TIME)).toMillis());
		Time timestampRecordExpireTime = Time.milliseconds(
				scala.concurrent.duration.Duration.apply(configuration.getString(BlacklistOptions.NETWORK_TIMESTAMP_RECORD_EXPIRE_TIME)).toMillis());
		float meanSdRatioAllBlockedThreshold = configuration.getFloat(BlacklistOptions.NETWORK_MEAN_SD_RATIO_ALL_BLOCKED_THRESHOLD);
		float meanSdRatioThresholdSomeBlocked = configuration.getFloat(BlacklistOptions.NETWORK_MEAN_SD_RATIO_SOME_BLOCKED_THRESHOLD);
		int expectedMinHost = configuration.getInteger(BlacklistOptions.NETWORK_EXPECTED_MIN_HOST);
		float expectedBlockedHostRatio = configuration.getFloat(BlacklistOptions.NETWORK_EXPECTED_BLOCKED_HOST_RATIO);
		return new BlacklistConfiguration(
				taskManagerBlacklistEnabled,
				taskBlacklistEnabled,
				blacklistCriticalEnable,
				maxTaskFailureNumPerHost,
				maxTaskManagerFailureNumPerHost,
				taskBlacklistMaxLength,
				taskManagerBlacklistMaxLength,
				failureTimeout,
				checkInterval,
				limiterFailureInterval,
				limiterMaxFailuresPerInterval,
				maxFailureNum,
				maxHostPerExceptionMinNumber,
				maxHostPerExceptionRatio,
				allIgnoredExceptionClassNames,
				failureEffectiveTime,
				networkFailureExpireTime,
				timestampRecordExpireTime,
				meanSdRatioAllBlockedThreshold,
				meanSdRatioThresholdSomeBlocked,
				expectedMinHost,
				expectedBlockedHostRatio);
	}
}
