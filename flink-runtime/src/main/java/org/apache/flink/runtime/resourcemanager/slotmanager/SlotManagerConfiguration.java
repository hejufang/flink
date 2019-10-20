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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.duration.Duration;

/**
 * Configuration for the {@link SlotManager}.
 */
public class SlotManagerConfiguration {

	private static final Logger LOGGER = LoggerFactory.getLogger(SlotManagerConfiguration.class);

	private final Time taskManagerRequestTimeout;
	private final Time slotRequestTimeout;
	private final Time taskManagerTimeout;
	private final boolean waitResultConsumedBeforeRelease;

	private final int numInitialTaskManagers;
	private final boolean initialTaskManager;
	private final int extraInitialTaskManagerNumbers;
	private final float extraInitialTaskManagerFraction;

	public SlotManagerConfiguration(
		Time taskManagerRequestTimeout,
		Time slotRequestTimeout,
		Time taskManagerTimeout,
		boolean waitResultConsumedBeforeRelease) {
		this(taskManagerRequestTimeout,
			slotRequestTimeout,
			taskManagerTimeout,
			waitResultConsumedBeforeRelease,
			0,
			false,
			0,
			0);
	}

	public SlotManagerConfiguration(
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout,
			boolean waitResultConsumedBeforeRelease,
			int numInitialTaskManagers,
			boolean initialTaskManager,
			int extraInitialTaskManagerNumbers,
			float extraInitialTaskManagerFraction) {

		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
		this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
		this.numInitialTaskManagers = numInitialTaskManagers;
		this.initialTaskManager = initialTaskManager;
		this.extraInitialTaskManagerNumbers = extraInitialTaskManagerNumbers;
		this.extraInitialTaskManagerFraction = extraInitialTaskManagerFraction;
	}

	public Time getTaskManagerRequestTimeout() {
		return taskManagerRequestTimeout;
	}

	public Time getSlotRequestTimeout() {
		return slotRequestTimeout;
	}

	public Time getTaskManagerTimeout() {
		return taskManagerTimeout;
	}

	public boolean isWaitResultConsumedBeforeRelease() {
		return waitResultConsumedBeforeRelease;
	}

	public int getNumInitialTaskManagers() {
		return numInitialTaskManagers;
	}

	public boolean isInitialTaskManager() {
		return initialTaskManager;
	}

	public int getExtraInitialTaskManagerNumbers() {
		return extraInitialTaskManagerNumbers;
	}

	public float getExtraInitialTaskManagerFraction() {
		return extraInitialTaskManagerFraction;
	}

	public static SlotManagerConfiguration fromConfiguration(Configuration configuration) throws ConfigurationException {
		final String strTimeout = configuration.getString(AkkaOptions.ASK_TIMEOUT);
		final Time rpcTimeout;

		try {
			rpcTimeout = Time.milliseconds(Duration.apply(strTimeout).toMillis());
		} catch (NumberFormatException e) {
			throw new ConfigurationException("Could not parse the resource manager's timeout " +
				"value " + AkkaOptions.ASK_TIMEOUT + '.', e);
		}

		final Time slotRequestTimeout = getSlotRequestTimeout(configuration);
		final Time taskManagerTimeout = Time.milliseconds(
				configuration.getLong(ResourceManagerOptions.TASK_MANAGER_TIMEOUT));

		boolean waitResultConsumedBeforeRelease =
			configuration.getBoolean(ResourceManagerOptions.TASK_MANAGER_RELEASE_WHEN_RESULT_CONSUMED);

		int numInitialTaskManagers = configuration.getInteger(TaskManagerOptions.NUM_INITIAL_TASK_MANAGERS);

		boolean initialTaskManager = configuration.getBoolean(TaskManagerOptions.INITIAL_TASK_MANAGER_ON_START);

		int extraInitialTaskManagerNumbers = configuration.getInteger(TaskManagerOptions.NUM_EXTRA_INITIAL_TASK_MANAGERS);
		float extraInitialTaskManagerFraction = configuration.getFloat(TaskManagerOptions.EXTRA_INITIAL_TASK_MANAGERS_FRACTION);

		return new SlotManagerConfiguration(
			rpcTimeout, slotRequestTimeout, taskManagerTimeout, waitResultConsumedBeforeRelease,
			numInitialTaskManagers, initialTaskManager, extraInitialTaskManagerNumbers, extraInitialTaskManagerFraction);
	}

	private static Time getSlotRequestTimeout(final Configuration configuration) {
		final long slotRequestTimeoutMs;
		if (configuration.contains(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT)) {
			LOGGER.warn("Config key {} is deprecated; use {} instead.",
				ResourceManagerOptions.SLOT_REQUEST_TIMEOUT,
				JobManagerOptions.SLOT_REQUEST_TIMEOUT);
			slotRequestTimeoutMs = configuration.getLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT);
		} else {
			slotRequestTimeoutMs = configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT);
		}
		return Time.milliseconds(slotRequestTimeoutMs);
	}
}
