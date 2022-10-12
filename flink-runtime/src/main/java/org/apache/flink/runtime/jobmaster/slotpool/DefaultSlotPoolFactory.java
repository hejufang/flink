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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nonnull;

/**
 * Default slot pool factory.
 */
public class DefaultSlotPoolFactory implements SlotPoolFactory {

	@Nonnull
	private final Clock clock;

	@Nonnull
	private final Time rpcTimeout;

	@Nonnull
	private final Time slotIdleTimeout;

	@Nonnull
	private final Time batchSlotTimeout;

	private final boolean jobLogDetailDisable;

	private final boolean batchRequestSlotsEnable;

	private final boolean requestSlotFromResourceManagerDirectEnable;

	private final boolean useMainScheduledExecutorEnable;

	public DefaultSlotPoolFactory(
			@Nonnull Clock clock,
			@Nonnull Time rpcTimeout,
			@Nonnull Time slotIdleTimeout,
			@Nonnull Time batchSlotTimeout,
			boolean jobLogDetailDisable,
			boolean batchRequestSlotsEnable,
			boolean requestSlotFromResourceManagerDirectEnable,
			boolean useMainScheduledExecutorEnable) {
		this.clock = clock;
		this.rpcTimeout = rpcTimeout;
		this.slotIdleTimeout = slotIdleTimeout;
		this.batchSlotTimeout = batchSlotTimeout;
		this.jobLogDetailDisable = jobLogDetailDisable;
		this.batchRequestSlotsEnable = batchRequestSlotsEnable;
		this.requestSlotFromResourceManagerDirectEnable = requestSlotFromResourceManagerDirectEnable;
		this.useMainScheduledExecutorEnable = useMainScheduledExecutorEnable;
	}

	@Override
	@Nonnull
	public SlotPool createSlotPool(@Nonnull JobID jobId) {
		return createSlotPool(jobId, 0, false);
	}

	@Nonnull
	@Override
	public SlotPool createSlotPool(@Nonnull JobID jobId, int taskCount, boolean minResourceSlotPoolSimplifyEnabled) {
		return new SlotPoolImpl(
			jobId,
			clock,
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout,
			jobLogDetailDisable,
			batchRequestSlotsEnable,
			requestSlotFromResourceManagerDirectEnable,
			useMainScheduledExecutorEnable,
			taskCount);
	}

	public static DefaultSlotPoolFactory fromConfiguration(@Nonnull Configuration configuration) {

		final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		final Time slotIdleTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
		final Time batchSlotTimeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));
		final boolean jobLogDetailDisable = configuration.getBoolean(CoreOptions.FLINK_JOB_LOG_DETAIL_DISABLE);
		final boolean batchRequestSlotsEnable = configuration.getBoolean(JobManagerOptions.JOBMANAGER_BATCH_REQUEST_SLOTS_ENABLE);
		final boolean requestSlotFromResourceManagerDirectEnable = configuration.getBoolean(JobManagerOptions.JOBMANAGER_REQUEST_SLOT_FROM_RESOURCEMANAGER_ENABLE);
		if (requestSlotFromResourceManagerDirectEnable && !batchRequestSlotsEnable) {
			throw new IllegalArgumentException("JobMaster can request slots from resource manager directly only when batch request is open.");
		}
		final boolean useMainScheduledExecutorEnable = configuration.getBoolean(CoreOptions.ENDPOINT_USE_MAIN_SCHEDULED_EXECUTOR_ENABLE);

		return new DefaultSlotPoolFactory(
			SystemClock.getInstance(),
			rpcTimeout,
			slotIdleTimeout,
			batchSlotTimeout,
			jobLogDetailDisable,
			batchRequestSlotsEnable,
			requestSlotFromResourceManagerDirectEnable,
			useMainScheduledExecutorEnable);
	}
}