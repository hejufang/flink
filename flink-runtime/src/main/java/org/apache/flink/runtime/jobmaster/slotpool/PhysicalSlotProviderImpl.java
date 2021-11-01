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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The provider serves physical slot requests.
 */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
	private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

	private final SlotSelectionStrategy slotSelectionStrategy;

	private final SlotPool slotPool;

	private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

	private final Counter allocateFromAvailableFailedCounter = new SimpleCounter();

	public PhysicalSlotProviderImpl(SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool, JobManagerJobMetricGroup jobManagerJobMetricGroup) {
		this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
		this.slotPool = checkNotNull(slotPool);
		this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
		registerMetrics();
	}

	@Override
	public void setRequiredResources(Map<ResourceProfile, Integer> requiredResources) {
		slotPool.setRequiredResourceNumber(requiredResources);
	}

	@Override
	public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest, Time timeout) {
		SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
		SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
		ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

		LOG.debug("Received slot request [{}] with resource requirements: {}", slotRequestId, resourceProfile);

		CompletableFuture<Optional<PhysicalSlot>> availablePhysicalSlot = tryAllocateFromAvailable(slotRequestId, slotProfile, timeout);

		CompletableFuture<PhysicalSlot> slotFuture;
		slotFuture = availablePhysicalSlot.thenCompose(
				physicalSlot -> physicalSlot
						.map(CompletableFuture::completedFuture)
						.orElseGet(() -> requestNewSlot(
								slotRequestId,
								resourceProfile,
								physicalSlotRequest.getSlotProfile().getBannedLocations(),
								physicalSlotRequest.willSlotBeOccupiedIndefinitely(),
								timeout)));

		return slotFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
	}

	private CompletableFuture<Optional<PhysicalSlot>> tryAllocateFromAvailable(SlotRequestId slotRequestId, SlotProfile slotProfile, Time timeout) {
		return slotPool.getRequiredResourceSatisfiedFutureWithTimeout(timeout).thenApply(
				ignore -> tryAllocateFromAvailableInternal(slotRequestId, slotProfile));
	}

	private Optional<PhysicalSlot> tryAllocateFromAvailableInternal(SlotRequestId slotRequestId, SlotProfile slotProfile) {
		try {
			Optional<SlotSelectionStrategy.SlotInfoAndLocality> selectedAvailableSlot =
					slotSelectionStrategy.selectBestSlotForProfile(slotProfile, slotPool);

			return selectedAvailableSlot.flatMap(
					slotInfoAndLocality -> slotPool.allocateAvailableSlot(
							slotRequestId,
							slotInfoAndLocality.getSlotInfo().getAllocationId())
			);
		} catch (Exception e) {
			LOG.error("Allocate slot from available slots failed, this is a bug of scheduler.", e);
			allocateFromAvailableFailedCounter.inc();
			return Optional.empty();
		}
	}

	private CompletableFuture<PhysicalSlot> requestNewSlot(
			SlotRequestId slotRequestId,
			ResourceProfile resourceProfile,
			Collection<TaskManagerLocation> bannedLocations,
			boolean willSlotBeOccupiedIndefinitely,
			Time timeout) {
		return willSlotBeOccupiedIndefinitely && timeout != null ?
			slotPool.requestNewAllocatedSlot(slotRequestId, resourceProfile, bannedLocations, timeout) :
			slotPool.requestNewAllocatedBatchSlot(slotRequestId, resourceProfile, bannedLocations);
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
		slotPool.releaseSlot(slotRequestId, cause);
	}

	private void registerMetrics() {
		jobManagerJobMetricGroup.counter("allocateFromAvailableFailedNumber", allocateFromAvailableFailedCounter);
	}
}
