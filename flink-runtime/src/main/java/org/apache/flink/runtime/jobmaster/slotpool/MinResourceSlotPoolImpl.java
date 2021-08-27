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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SlotPool which allocate min resources before offer slots.
 */
public abstract class MinResourceSlotPoolImpl extends SlotPoolImpl {
	private final Map<ResourceProfile, Integer> requiredResourceNumber = new HashMap<>();

	// requested ResourceProfile
	private final Map<ResourceProfile, Set<PendingRequest>> pendingRequiredResources = new HashMap<>();

	// allocated ResourceProfile
	private final Map<ResourceProfile, Set<AllocatedSlot>> allocatedRequiredResources = new HashMap<>();
	private final Map<AllocatedSlot, ResourceProfile> allocatedSlotRequestedProfile = new HashMap<>();

	// requested ResourceProfile
	private final Map<ResourceProfile, CompletableFuture<Collection<Acknowledge>>> requiredResourceSatisfiedFutureByResourceProfile = new HashMap<>();
	private CompletableFuture<Acknowledge> requiredResourceSatisfiedFuture = CompletableFuture.completedFuture(Acknowledge.get());

	private boolean running = false;

	public MinResourceSlotPoolImpl(
			JobID jobId,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout) {
		super(jobId, clock, rpcTimeout, idleSlotTimeout, batchSlotTimeout);
	}

	@Override
	public void start(@Nonnull JobMasterId jobMasterId, @Nonnull String newJobManagerAddress, @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {
		super.start(jobMasterId, newJobManagerAddress, componentMainThreadExecutor);
		running = true;
		requestRequiredResources();
	}

	@Override
	public void suspend() {
		running = false;
		super.suspend();
		clear();
	}

	@Override
	public void markWillBeClosed() {
		running = false;
	}

	@Override
	public void close() {
		running = false;
		super.close();
		clear();
	}

	private void clear() {
		this.pendingRequiredResources.clear();
		this.allocatedRequiredResources.clear();
		this.allocatedSlotRequestedProfile.clear();
		this.requiredResourceSatisfiedFutureByResourceProfile.clear();
		if (!this.requiredResourceNumber.isEmpty()) {
			requiredResourceSatisfiedFuture = new CompletableFuture<>();
		} else {
			requiredResourceSatisfiedFuture = CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	@Override
	public void setRequiredResourceNumber(Map<ResourceProfile, Integer> requiredResourceNumber) {
		Preconditions.checkState(this.requiredResourceNumber.isEmpty(), "requiredResourceNumber can only set once.");
		if (!requiredResourceNumber.isEmpty()) {
			log.info("Set required resource number as {}", requiredResourceNumber);
			requiredResourceSatisfiedFuture = new CompletableFuture<>();
			this.requiredResourceNumber.putAll(requiredResourceNumber);
		}
	}

	@Override
	public CompletableFuture<Acknowledge> getRequiredResourceSatisfiedFuture() {
		return requiredResourceSatisfiedFuture;
	}

	@Override
	public CompletableFuture<Acknowledge> getRequiredResourceSatisfiedFutureWithTimeout(Time timeout) {
		componentMainThreadExecutor.assertRunningInMainThread();
		CompletableFuture<Acknowledge> futureWithTimeout = requiredResourceSatisfiedFuture.thenApply(Function.identity());
		return FutureUtils.orTimeout(futureWithTimeout, timeout.toMilliseconds(), TimeUnit.MILLISECONDS, componentMainThreadExecutor);
	}

	private void requestRequiredResources() {
		componentMainThreadExecutor.assertRunningInMainThread();
		log.info("Start to request required resources.");

		for (Map.Entry<ResourceProfile, Integer> entry : requiredResourceNumber.entrySet()) {
			ResourceProfile resourceProfile = entry.getKey();
			int requiredNumber = entry.getValue();

			for (int i = 0; i < requiredNumber; i++) {
				requestNewAllocatedSlotForRequiredResource(resourceProfile);
			}
			updateRequiredResourceFutures(resourceProfile);
		}
	}

	private void updateRequiredResourceFutures(ResourceProfile resourceProfile) {
		if (requiredResourceSatisfiedFutureByResourceProfile.containsKey(resourceProfile)) {
			requiredResourceSatisfiedFutureByResourceProfile.get(resourceProfile).cancel(true);
		}

		CompletableFuture<Collection<Acknowledge>> slotFutures = FutureUtils.combineAll(
				pendingRequiredResources.get(resourceProfile).stream()
						.map(pendingRequest -> pendingRequest.getAllocatedSlotFuture().thenApply(allocatedSlot -> Acknowledge.get()))
						.collect(Collectors.toSet()));

		requiredResourceSatisfiedFutureByResourceProfile.put(resourceProfile, slotFutures);

		if (requiredResourceSatisfiedFuture.isDone()) {
			requiredResourceSatisfiedFuture = new CompletableFuture<>();
		}

		slotFutures.whenComplete((acknowledges, throwable) -> {
			if (throwable == null) {
				for (CompletableFuture<Collection<Acknowledge>> f : requiredResourceSatisfiedFutureByResourceProfile.values()) {
					if (!f.isDone() || f.isCancelled() || f.isCompletedExceptionally()) {
						return;
					}
				}
				log.info("Required resources satisfied.");
				requiredResourceSatisfiedFutureByResourceProfile.clear();
				requiredResourceSatisfiedFuture.complete(Acknowledge.get());
			}
		});
	}

	private void requestNewAllocatedSlotForRequiredResourceAndUpdateFutures(ResourceProfile resourceProfile) {
		if (!running) {
			log.debug("SlotPool is not running currently, ignore request for required resource.");
			return;
		}
		requestNewAllocatedSlotForRequiredResource(resourceProfile);
		updateRequiredResourceFutures(resourceProfile);
	}

	private void requestNewAllocatedSlotForRequiredResource(ResourceProfile resourceProfile) {
		SlotRequestId slotRequestId = new SlotRequestId();
		PendingRequest pendingRequest = new PendingRequest(slotRequestId, resourceProfile, false, Collections.emptyList());
		requestNewAllocatedSlotInternal(pendingRequest);
		log.debug("Request new slot request {} for required resources.", pendingRequest);

		pendingRequiredResources.computeIfAbsent(resourceProfile, r -> new HashSet<>()).add(pendingRequest);
		pendingRequest.getAllocatedSlotFuture().whenComplete((allocatedSlot, throwable) -> {
			pendingRequiredResources.get(resourceProfile).remove(pendingRequest);
			if (throwable != null) {
				requestNewAllocatedSlotForRequiredResourceAndUpdateFutures(resourceProfile);
			} else {
				log.debug("PendingRequest {} for required resource fulfilled by {}", pendingRequest, allocatedSlot);
				allocatedRequiredResources.computeIfAbsent(allocatedSlot.getResourceProfile(), r -> new HashSet<>()).add(allocatedSlot);
				allocatedSlotRequestedProfile.put(allocatedSlot, resourceProfile);
			}
		});
	}

	@Override
	protected boolean tryFulFillPendingRequiredResources(PendingRequest pendingRequest, AllocatedSlot allocatedSlot) {
		componentMainThreadExecutor.assertRunningInMainThread();
		ResourceProfile resourceProfile = pendingRequest.getResourceProfile();
		if (allocatedRequiredResources.getOrDefault(resourceProfile, Collections.emptySet()).contains(allocatedSlot)) {
			log.debug("Allocated slot {} already in allocatedRequiredResources, ignore.", allocatedSlot);
			return false;
		}
		if (pendingRequiredResources.getOrDefault(resourceProfile, Collections.emptySet()).contains(pendingRequest)) {
			markSlotAvailable(allocatedSlot);
			pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot);
			return true;
		}
		return false;
	}

	@Override
	protected boolean tryFulFillPendingRequiredResources(AllocatedSlot allocatedSlot) {
		componentMainThreadExecutor.assertRunningInMainThread();
		ResourceProfile resourceProfile = allocatedSlot.getResourceProfile();
		if (allocatedRequiredResources.getOrDefault(resourceProfile, Collections.emptySet()).contains(allocatedSlot)) {
			log.debug("Allocated slot {} already in allocatedRequiredResources, ignore.", allocatedSlot);
			return false;
		}

		Optional<PendingRequest> pendingRequest = pollMatchingPendingRequiredRequest(allocatedSlot);
		return pendingRequest
				.filter(request -> tryFulFillPendingRequiredResources(request, allocatedSlot))
				.isPresent();
	}

	private Optional<PendingRequest> pollMatchingPendingRequiredRequest(final AllocatedSlot slot) {
		ResourceProfile resourceProfile = slot.getResourceProfile();
		for (Map.Entry<ResourceProfile, Set<PendingRequest>> entry : pendingRequiredResources.entrySet()) {
			if (resourceProfile.isMatching(entry.getKey()) && !entry.getValue().isEmpty()) {
				return entry.getValue().stream().findFirst();
			}
		}
		return Optional.empty();
	}

	@Override
	protected PendingRequest pollMatchingPendingRequest(AllocatedSlot slot) {
		return pollMatchingPendingRequest(slot, p -> !pendingRequiredResources.getOrDefault(p.getResourceProfile(), Collections.emptySet()).contains(p));
	}

	@Override
	protected void tryRemoveAllocatedSlot(AllocatedSlot allocatedSlot) {
		componentMainThreadExecutor.assertRunningInMainThread();
		ResourceProfile resourceProfile = allocatedSlot.getResourceProfile();
		if (allocatedRequiredResources.getOrDefault(resourceProfile, Collections.emptySet()).remove(allocatedSlot)) {
			log.info("Remove allocatedSlot {} from allocatedRequiredResources.", allocatedSlot);
			ResourceProfile requestedResourceProfile = allocatedSlotRequestedProfile.remove(allocatedSlot);
			requestNewAllocatedSlotForRequiredResourceAndUpdateFutures(requestedResourceProfile);
		}
	}

	@Override
	protected boolean slotCanRelease(AllocatedSlot slot) {
		ResourceProfile resourceProfile = slot.getResourceProfile();
		return !allocatedRequiredResources.getOrDefault(resourceProfile, Collections.emptySet()).contains(slot);
	}
}
