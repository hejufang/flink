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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Testing extension of the {@link RoundRobinSlotPoolImpl} which adds additional methods
 * for testing.
 */
public class TestingRoundRobinSlotPoolImpl extends RoundRobinSlotPoolImpl {

	private ResourceProfile lastRequestedSlotResourceProfile;

	public TestingRoundRobinSlotPoolImpl(JobID jobId, boolean batchRequestEnable) {
		this(
			jobId,
			SystemClock.getInstance(),
			AkkaUtils.getDefaultTimeout(),
			AkkaUtils.getDefaultTimeout(),
			AkkaUtils.getDefaultTimeout(),
			Time.milliseconds(JobManagerOptions.SLOT_IDLE_TIMEOUT.defaultValue()),
			batchRequestEnable,
			false);
	}

	public TestingRoundRobinSlotPoolImpl(
			JobID jobId,
			Clock clock,
			Time rpcTimeout,
			Time slotRequestTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout,
			boolean batchRequestEnable,
			boolean satisfyCheckerSimplifyEnabled) {
		super(jobId, clock, rpcTimeout, slotRequestTimeout, idleSlotTimeout, batchSlotTimeout, false, batchRequestEnable, false, false, 0, satisfyCheckerSimplifyEnabled);
	}

	void triggerCheckIdleSlot() {
		runAsync(this::checkIdleSlot);
	}

	@Override
	public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
			final SlotRequestId slotRequestId,
			final ResourceProfile resourceProfile,
			final Collection<TaskManagerLocation> bannedLocations,
			final Time timeout) {

		this.lastRequestedSlotResourceProfile = resourceProfile;

		return super.requestNewAllocatedSlot(slotRequestId, resourceProfile, bannedLocations, timeout);
	}

	public ResourceProfile getLastRequestedSlotResourceProfile() {
		return lastRequestedSlotResourceProfile;
	}
}