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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.strategy.AllocateTaskManagerStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.strategy.RandomTaskManagerStrategy;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Virtual TaskManager SlotPool, there is no real slot for TaskManager.
 */
public final class VirtualTaskManagerSlotPool implements SlotPool {
	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<ResourceID, Set<AllocationID>> slotMapping = new HashMap<>();
	private final Map<AllocationID, AllocatedSlot> allocatedSlots = new HashMap<>();

	private final Map<ResourceID, ResolvedTaskManagerTopology> taskManagers;
	private final boolean requestSlotFromResourceManagerDirectEnable;
	private final JobID jobID;
	private final int totalTaskCount;
	private final List<ResourceID> allocatedTaskManagers;
	private final AllocateTaskManagerStrategy allocateTaskManagerStrategy;

	private JobMasterId jobMasterId;
	private String jobMasterAddress;

	@VisibleForTesting
	public VirtualTaskManagerSlotPool(JobID jobId, boolean requestSlotFromResourceManagerDirectEnable, Map<ResourceID, ResolvedTaskManagerTopology> taskManagers, int totalTaskCount) {
		this(jobId, requestSlotFromResourceManagerDirectEnable, taskManagers, totalTaskCount, RandomTaskManagerStrategy.getInstance());
	}

	public VirtualTaskManagerSlotPool(
			JobID jobId,
			boolean requestSlotFromResourceManagerDirectEnable,
			Map<ResourceID, ResolvedTaskManagerTopology> taskManagers,
			int totalTaskCount,
			AllocateTaskManagerStrategy allocateTaskManagerStrategy) {
		this.jobID = jobId;
		this.requestSlotFromResourceManagerDirectEnable = requestSlotFromResourceManagerDirectEnable;
		this.taskManagers = new HashMap<>(taskManagers);
		this.totalTaskCount = totalTaskCount;
		this.allocatedTaskManagers = new ArrayList<>();
		this.allocateTaskManagerStrategy = allocateTaskManagerStrategy;
	}

	public AllocatedSlot allocatedSlot(AllocationID allocationID, ResourceID resourceID) {
		Preconditions.checkNotNull(jobMasterId, "JobMasterId is null, that means SlotPool worked before started.");
		Preconditions.checkNotNull(taskManagers.get(resourceID), "Allocate " + allocationID + " from " + resourceID + " failed, task manager not found.");
		TaskManagerLocation taskManagerLocation = taskManagers.get(resourceID).getTaskManagerLocation();
		TaskExecutorGateway taskExecutorGateway = taskManagers.get(resourceID).getTaskExecutorGateway();
		TaskManagerGateway taskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, jobMasterId, requestSlotFromResourceManagerDirectEnable, jobMasterAddress);
		AllocatedSlot allocatedSlot = new AllocatedSlot(allocationID, taskManagerLocation, 0, ResourceProfile.UNKNOWN, taskManagerGateway);

		slotMapping.computeIfAbsent(resourceID, r -> new HashSet<>()).add(allocationID);
		allocatedSlots.put(allocationID, allocatedSlot);
		log.debug("allocated slot {} on resource {}, Job {}.", allocatedSlot, resourceID, jobID);
		return allocatedSlot;
	}

	public void releaseAllocatedSlot(AllocationID allocationID) {
		AllocatedSlot removedSlot = allocatedSlots.remove(allocationID);
		if (removedSlot != null) {
			removedSlot.releasePayload(new Exception("Slot " + allocationID + " is released."));
			ResourceID resourceID = removedSlot.getTaskManagerLocation().getResourceID();
			Set<AllocationID> slots = slotMapping.get(resourceID);
			if (slots != null) {
				slots.remove(allocationID);
				if (slots.isEmpty()) {
					slotMapping.remove(resourceID);
				}
			} else {
				log.error("Slot {} in allocatedSlots, but not in slotMapping, it is a bug.", allocationID);
			}
		}
	}

	@Override
	public void start(JobMasterId jobMasterId, String newJobManagerAddress, ComponentMainThreadExecutor jmMainThreadScheduledExecutor) throws Exception {
		this.jobMasterId = jobMasterId;
		this.jobMasterAddress = newJobManagerAddress;
		log.debug("VirtualTaskManagerSlotPool start with JobMasterId {} address {}, Job {}", jobMasterId, newJobManagerAddress, jobID);
	}

	@Override
	public void suspend() {
		this.jobMasterId = null;
	}

	@Override
	public void close() {
		allocateTaskManagerStrategy.releaseTaskManagers(allocatedTaskManagers, taskManagers);
		this.jobMasterId = null;
	}

	@Override
	public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
	}

	@Override
	public void disconnectResourceManager() {
	}

	@Override
	public boolean registerTaskManager(ResourceID resourceID) {
		return false;
	}

	@Override
	public boolean registerTaskManager(ResourceID resourceID, ResolvedTaskManagerTopology resolvedTaskManagerTopology) {
		this.taskManagers.put(resourceID, resolvedTaskManagerTopology);
		log.debug("TaskManager {} registered to slotPool for Job {}", resourceID, jobID);
		return true;
	}

	@Override
	public boolean releaseTaskManager(final ResourceID resourceId, final Exception cause) {
		Set<AllocationID> slots = slotMapping.remove(resourceId);
		if (slots != null) {
			for (AllocationID slotId : slots) {
				AllocatedSlot slot = allocatedSlots.remove(slotId);
				if (slot != null) {
					slot.releasePayload(cause);
				} else {
					log.error("Slot {} in slotMapping but not in allocatedSlots, it is a bug.", slotId);
				}
			}
		}
		log.debug("TaskManager {} released from slotPool for Job {}", resourceId, jobID, cause);

		return taskManagers.remove(resourceId) != null;
	}

	@Override
	public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
		return new AllocatedSlotReport(jobID, Collections.emptySet());
	}

	@Override
	public Set<ResourceID> allocateTaskManagers(int taskManagerCount) {
		Set<ResourceID> resourceIds = allocateTaskManagerStrategy.allocateTaskManagers(taskManagers, taskManagerCount);
		allocatedTaskManagers.addAll(resourceIds);
		return resourceIds;
	}

	@Override
	public Set<ResourceID> getUsedTaskManagers() {
		return slotMapping.keySet();
	}

	public JobID getJobID() {
		return jobID;
	}

	public int getTotalTaskCount() {
		return totalTaskCount;
	}

	//-------------------
	//-- unsupported ----
	//-------------------

	@Override
	public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<SlotOffer> offerSlots(TaskManagerLocation taskManagerLocation, TaskManagerGateway taskManagerGateway, Collection<SlotOffer> offers) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<ResourceID> failAllocation(AllocationID allocationID, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Nonnull
	@Override
	public Collection<SlotInfoWithUtilization> getAvailableSlotsInformation() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<PhysicalSlot> allocateAvailableSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull AllocationID allocationID) {
		throw new UnsupportedOperationException();
	}

	@Nonnull
	@Override
	public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile, @Nonnull Collection<TaskManagerLocation> bannedLocations, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Nonnull
	@Override
	public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(@Nonnull SlotRequestId slotRequestId, @Nonnull ResourceProfile resourceProfile, @Nonnull Collection<TaskManagerLocation> bannedLocations) {
		throw new UnsupportedOperationException();
	}

	public int getTotalTaskManagerCount() {
		return taskManagers.size();
	}

	/**
	 * Factory for {@link VirtualTaskManagerSlotPool}.
	 */
	public static class VirtualTaskManagerSlotPoolFactory implements SlotPoolFactory {
		private final Map<ResourceID, ResolvedTaskManagerTopology> taskManagers;
		private final boolean requestSlotFromResourceManagerDirectEnable;
		private final AllocateTaskManagerStrategy allocateTaskManagerStrategy;

		public VirtualTaskManagerSlotPoolFactory(
				Map<ResourceID, ResolvedTaskManagerTopology> taskManagers,
				boolean requestSlotFromResourceManagerDirectEnable,
				AllocateTaskManagerStrategy allocateTaskManagerStrategy) {
			this.taskManagers = taskManagers;
			this.requestSlotFromResourceManagerDirectEnable = requestSlotFromResourceManagerDirectEnable;
			this.allocateTaskManagerStrategy = allocateTaskManagerStrategy;
		}

		@Override
		@Nonnull
		public SlotPool createSlotPool(@Nonnull JobID jobId) {
			return createSlotPool(jobId, 0, false);
		}

		@Nonnull
		@Override
		public SlotPool createSlotPool(@Nonnull JobID jobId, int taskCount, boolean minResourceSlotPoolSimplifyEnabled) {
			return new VirtualTaskManagerSlotPool(jobId, requestSlotFromResourceManagerDirectEnable, this.taskManagers, taskCount, allocateTaskManagerStrategy);
		}
	}
}
