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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.clock.Clock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * SlotPool with RoundRobin slot selection strategy.
 */
public class RoundRobinSlotPoolImpl extends MinResourceSlotPoolImpl {
	private RoundRobinAvailableSlots roundRobinAvailableSlots;

	public RoundRobinSlotPoolImpl(
			JobID jobId,
			Clock clock,
			Time rpcTimeout,
			Time idleSlotTimeout,
			Time batchSlotTimeout) {
		super(jobId, clock, rpcTimeout, idleSlotTimeout, batchSlotTimeout);
	}

	@Override
	protected AvailableSlots createAvailableSlots() {
		roundRobinAvailableSlots = new RoundRobinAvailableSlots();
		return roundRobinAvailableSlots;
	}

	@Override
	public Optional<AllocatedSlot> getNextAvailableSlot(ResourceProfile resourceProfile, Predicate<AllocatedSlot> predicate) {
		return roundRobinAvailableSlots.getNextAvailableSlot(resourceProfile, predicate);
	}

	@Override
	public Optional<AllocatedSlot> getByAllocationID(ResourceProfile resourceProfile, AllocationID allocationID) {
		return roundRobinAvailableSlots.getByAllocationId(resourceProfile, allocationID);
	}

	@Override
	public Optional<AllocatedSlot> getByTaskManagerLocation(ResourceProfile resourceProfile, TaskManagerLocation taskManagerLocation, Predicate<AllocatedSlot> predicate) {
		return roundRobinAvailableSlots.getByTaskManagerLocation(resourceProfile, taskManagerLocation, predicate);
	}

	@Override
	public Optional<AllocatedSlot> getByHost(ResourceProfile resourceProfile, String host, Predicate<AllocatedSlot> predicate) {
		return roundRobinAvailableSlots.getByHost(resourceProfile, host, predicate);
	}

	// ------------------------------------------------------------------------

	/**
	 * Available slots with RoundRobin selection strategy.
	 */
	protected static class RoundRobinAvailableSlots extends AvailableSlots {
		Map<ResourceProfile, RoundRobinAvailableSlotsByResourceProfile> allSlots;

		RoundRobinAvailableSlots() {
			super();
			allSlots = new HashMap<>();
		}

		@Override
		void add(AllocatedSlot slot, long timestamp) {
			super.add(slot, timestamp);
			ResourceProfile resourceProfile = slot.getResourceProfile();
			allSlots.computeIfAbsent(resourceProfile, r -> new RoundRobinAvailableSlotsByResourceProfile()).addSlot(slot);
		}

		@Override
		Set<AllocatedSlot> removeAllForTaskManager(ResourceID taskManager) {
			Set<AllocatedSlot> removedSlots = super.removeAllForTaskManager(taskManager);
			for (AllocatedSlot slot : removedSlots) {
				ResourceProfile resourceProfile = slot.getResourceProfile();
				allSlots.get(resourceProfile).removeSlot(slot);
			}
			return removedSlots;
		}

		@Override
		AllocatedSlot tryRemove(AllocationID slotId) {
			AllocatedSlot slot = super.tryRemove(slotId);
			if (slot != null) {
				ResourceProfile resourceProfile = slot.getResourceProfile();
				allSlots.get(resourceProfile).removeSlot(slot);
			}
			return slot;
		}

		@Override
		void clear() {
			super.clear();
			for (RoundRobinAvailableSlotsByResourceProfile roundRobinAvailableSlotsByResourceProfile : allSlots.values()) {
				roundRobinAvailableSlotsByResourceProfile.clear();
			}
			allSlots.clear();
		}

		public Optional<AllocatedSlot> getNextAvailableSlot(ResourceProfile requestResourceProfile, Predicate<AllocatedSlot> predicate) {
			return getSlot(requestResourceProfile, r -> r.getNextAvailable(predicate));
		}

		public Optional<AllocatedSlot> getByAllocationId(ResourceProfile requestResourceProfile, AllocationID allocationID) {
			return getSlot(requestResourceProfile, r -> r.getByAllocateId(allocationID));
		}

		public Optional<AllocatedSlot> getByTaskManagerLocation(ResourceProfile requestResourceProfile, TaskManagerLocation taskManagerLocation, Predicate<AllocatedSlot> predicate) {
			return getSlot(requestResourceProfile, r -> r.getByTaskManagerLocation(taskManagerLocation, predicate));
		}

		public Optional<AllocatedSlot> getByHost(ResourceProfile requestResourceProfile, String host, Predicate<AllocatedSlot> predicate) {
			return getSlot(requestResourceProfile, r -> r.getByHost(host, predicate));
		}

		public Optional<AllocatedSlot> getSlot(
				ResourceProfile requestResourceProfile,
				Function<RoundRobinAvailableSlotsByResourceProfile, Optional<AllocatedSlot>> function) {

			Optional<AllocatedSlot> slot;
			for (ResourceProfile resourceProfile : allSlots.keySet()) {
				if (resourceProfile.isMatching(requestResourceProfile)) {
					RoundRobinAvailableSlotsByResourceProfile roundRobinAvailableSlotsByResourceProfile = allSlots.get(resourceProfile);
					slot = function.apply(roundRobinAvailableSlotsByResourceProfile);
					if (slot.isPresent()) {
						return slot;
					}
				}
			}
			return Optional.empty();
		}
	}

	/**
	 * Available slots with RoundRobin selection strategy by Resource Profile.
	 */
	protected static class RoundRobinAvailableSlotsByResourceProfile {
		// an index for check whether slot is useful.
		final Map<AllocationID, AllocatedSlot> slots;

		final ResourceWithCounter<TaskManagerLocation> taskManagers;
		final Map<String, Set<TaskManagerLocation>> taskManagersByHost;
		final Map<TaskManagerLocation, ResourceWithCounter<AllocatedSlot>> slotsByTaskManager;

		boolean allSlotOutOfBound = true;

		public RoundRobinAvailableSlotsByResourceProfile() {
			taskManagersByHost = new HashMap<>();
			taskManagers = new ResourceWithCounter<>();
			slotsByTaskManager = new HashMap<>();
			slots = new HashMap<>();
		}

		public void clear() {
			slots.clear();
			taskManagersByHost.clear();
			taskManagers.clear();
			slotsByTaskManager.clear();
		}

		public void addSlot(AllocatedSlot slot) {
			slots.put(slot.getAllocationId(), slot);
			TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();
			String host = taskManagerLocation.getFQDNHostname();
			taskManagersByHost
					.computeIfAbsent(host, h -> new HashSet<>())
					.add(taskManagerLocation);
			taskManagers.addResource(taskManagerLocation);
			slotsByTaskManager.computeIfAbsent(
					taskManagerLocation, t -> new ResourceWithCounter<>())
					.addResource(slot);
		}

		public boolean removeSlot(AllocatedSlot slot) {
			if (slots.remove(slot.getAllocationId()) == null) {
				return false;
			}
			TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();
			String host = taskManagerLocation.getFQDNHostname();
			ResourceWithCounter<AllocatedSlot> slotResourceWithCounter = slotsByTaskManager.get(taskManagerLocation);
			// must not null.
			slotResourceWithCounter.markRemove(slot);
			if (!slotResourceWithCounter.hasNext()) {
				slotsByTaskManager.remove(taskManagerLocation);
				taskManagers.markRemove(taskManagerLocation);
				taskManagersByHost.get(host).remove(taskManagerLocation);
				if (taskManagersByHost.get(host).isEmpty()) {
					taskManagersByHost.remove(host);
				}
			}
			return true;
		}

		/**
		 * Get next available AllocatedSlot by RoundRobin strategy.
		 * @return Optional slot. empty if no available slot.
		 */
		public Optional<AllocatedSlot> getNextAvailable(Predicate<AllocatedSlot> predicate) {
			if (slots.isEmpty()) {
				return Optional.empty();
			}
			Set<AllocatedSlot> unavailableSlots = new HashSet<>();

			AllocatedSlot allocatedSlot = null;

			while (taskManagers.hasNext()) {
				Optional<TaskManagerLocation> optionalTaskManagerLocation = taskManagers.getNext();
				if (optionalTaskManagerLocation.isPresent()) {
					TaskManagerLocation taskManagerLocation = optionalTaskManagerLocation.get();
					ResourceWithCounter<AllocatedSlot> allocatedSlotResourceWithCounter = slotsByTaskManager.get(taskManagerLocation);
					// must not null.
					if (allocatedSlotResourceWithCounter.hasNext()) {
						Optional<AllocatedSlot> optionalAllocatedSlot = allocatedSlotResourceWithCounter.getNext();
						allocatedSlot = optionalAllocatedSlot.orElse(null);
						if (!allocatedSlotResourceWithCounter.isIndexOutOfBound()) {
							allSlotOutOfBound = false;
						}
					} else {
						slotsByTaskManager.remove(taskManagerLocation);
					}
				}
				if (taskManagers.isIndexOutOfBound()) {
					taskManagers.resetIndex();
					if (allSlotOutOfBound) {
						for (ResourceWithCounter<AllocatedSlot> r : slotsByTaskManager.values()) {
							r.resetIndex();
						}
					}
					allSlotOutOfBound = true;
				}

				if (allocatedSlot != null) {
					if (predicate.test(allocatedSlot)) {
						return Optional.of(allocatedSlot);
					} else {
						if (unavailableSlots.size() == slots.size()) {
							return Optional.empty();
						} else {
							unavailableSlots.add(allocatedSlot);
						}
					}
				}
			}

			return Optional.empty();
		}

		// ------------------------------------------------------------------------
		// For location preferred.
		// ------------------------------------------------------------------------

		/**
		 * Get AllocatedSlot by allocation id, will mark the specified slot removed, but will not remove from iterator.
		 *
		 * @param allocationID allocation id of Slot.
		 * @return Optional slot. empty is this slot not exist or not available.
		 */
		public Optional<AllocatedSlot> getByAllocateId(AllocationID allocationID) {
			AllocatedSlot allocatedSlot = slots.get(allocationID);
			return Optional.ofNullable(allocatedSlot);
		}

		/**
		 * Get next available AllocatedSlot by TaskManagerLocation,
		 * This will mark the specified slot removed, but will not remove from iterator.
		 *
		 * @param taskManagerLocation task manager location of Slot.
		 * @return Optional slot. empty if no available slots on this TaskManager.
		 */
		public Optional<AllocatedSlot> getByTaskManagerLocation(TaskManagerLocation taskManagerLocation, Predicate<AllocatedSlot> predicate) {
			if (slots.isEmpty() || !slotsByTaskManager.containsKey(taskManagerLocation)) {
				return Optional.empty();
			}
			return slotsByTaskManager
					.get(taskManagerLocation)
					// must not null.
					.getNextAvailableNotIncreaseIndex(predicate);
		}

		/**
		 * Get next available AllocatedSlot by HostName,
		 * This will mark the specified slot removed, but will not remove from iterator.
		 *
		 * @param host host name of Slot.
		 * @return Optional slot. empty if no available slots on this host.
		 */
		public Optional<AllocatedSlot> getByHost(String host, Predicate<AllocatedSlot> predicate) {
			if (slots.isEmpty() || !taskManagersByHost.containsKey(host)) {
				return Optional.empty();
			}
			Set<TaskManagerLocation> taskManagerLocations = taskManagersByHost.get(host);
			for (TaskManagerLocation taskManagerLocation : taskManagerLocations) {
				ResourceWithCounter<AllocatedSlot> allocatedSlotResourceWithCounter = slotsByTaskManager.get(taskManagerLocation);
				Optional<AllocatedSlot> optionalAllocatedSlot = allocatedSlotResourceWithCounter
						.getNextAvailableNotIncreaseIndex(predicate);
				if (optionalAllocatedSlot.isPresent()) {
					return optionalAllocatedSlot;
				}
			}
			return Optional.empty();
		}
	}

	private static class ResourceWithCounter<T> {
		List<T> resources = new ArrayList<>();
		Set<T> availableResourceSet = new HashSet<>();
		int index = 0;

		public void clear() {
			resources.clear();
			availableResourceSet.clear();
			index = 0;
		}

		public boolean isIndexOutOfBound() {
			return index >= resources.size();
		}

		public void resetIndex() {
			index = 0;
		}

		public void addResource(T r) {
			if (!availableResourceSet.contains(r)) {
				// remove resource which is mark removed.
				remove(r);
				resources.add(r);
				availableResourceSet.add(r);
			}
		}

		public boolean hasNext() {
			return availableResourceSet.size() > 0;
		}

		public Optional<T> getNextAvailableNotIncreaseIndex(Predicate<T> predicate) {
			if (hasNext()) {
				if (isIndexOutOfBound()) {
					resetIndex();
				}
				int i = index;
				do {
					T resource = resources.get(i);
					if (availableResourceSet.contains(resource) && predicate.test(resource)) {
						return Optional.of(resource);
					}
					if (++i == resources.size()) {
						i = 0;
					}
				} while (i != index);
			}
			return Optional.empty();
		}

		public Optional<T> getNext() {
			if (hasNext() && index < resources.size()) {
				T resource = resources.get(index++);
				if (availableResourceSet.contains(resource)) {
					return Optional.of(resource);
				}
			}
			return Optional.empty();
		}

		public boolean remove(T resource) {
			int i = resources.indexOf(resource);
			if (i < 0) {
				return false;
			}

			resources.remove(i);

			if (i < index) {
				index--;
			}

			return availableResourceSet.remove(resource);
		}

		public boolean markRemove(T resource) {
			boolean result = availableResourceSet.remove(resource);
			if (availableResourceSet.isEmpty()) {
				clear();
			}
			return result;
		}

		public Set<T> getResources() {
			return availableResourceSet;
		}
	}
}
