/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class implements a {@link SlotSelectionStrategy} that is based on location preference hints.
 */
public class RoundRobinLocationPreferenceSlotSelectionStrategy extends DefaultLocationPreferenceSlotSelectionStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(RoundRobinLocationPreferenceSlotSelectionStrategy.class);

	RoundRobinLocationPreferenceSlotSelectionStrategy() {}

	@Override
	public Optional<SlotInfoAndLocality> selectBestSlotForProfile(@Nonnull SlotProfile slotProfile, @Nonnull SlotPool slotPool) {
		if (!(slotPool instanceof RoundRobinSlotPoolImpl)) {
			LOG.error("RoundRobinLocationPreferenceSlotSelectionStrategy only can be used with RoundRobinSlotPoolImpl, " +
					"fall back to DefaultLocationPreferenceSlotSelectionStrategy.");
			return super.selectBestSlotForProfile(slotProfile, slotPool);
		}

		Collection<TaskManagerLocation> preferredLocations = slotProfile.getPreferredLocations();
		Collection<TaskManagerLocation> bannedLocations = slotProfile.getBannedLocations();
		final Set<ResourceID> bannedResourceIDs = bannedLocations.stream().map(
				TaskManagerLocation::getResourceID).collect(Collectors.toSet());
		final Set<String> bannedHostnames = bannedLocations.stream().map(
				TaskManagerLocation::getFQDNHostname).collect(Collectors.toSet());

		final ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

		Optional<AllocatedSlot> optionalSlot;
		Collection<AllocationID> allocationIDS = slotProfile.getPreferredAllocations();
		for (AllocationID allocationID : allocationIDS) {
			optionalSlot = slotPool.getByAllocationID(resourceProfile, allocationID);
			if (optionalSlot.isPresent()) {
				AllocatedSlot slot = optionalSlot.get();
				if (!bannedResourceIDs.contains(slot.getTaskManagerLocation().getResourceID())
						&& !bannedHostnames.contains(slot.getTaskManagerLocation().getFQDNHostname())) {
					LOG.debug("allocate {} for allocated id {}.", slot.getAllocationId(), allocationID);
					return Optional.of(SlotInfoAndLocality.of(optionalSlot.get(), Locality.LOCAL));
				}
			}
		}

		Locality noMatchLocality = preferredLocations.isEmpty() && allocationIDS.isEmpty() ? Locality.UNCONSTRAINED : Locality.NON_LOCAL;

		Set<AllocationID> blackListedAllocations = slotProfile.getPreviousExecutionGraphAllocations();

		Map<TaskManagerLocation, Integer> preferredLocationWithScore = new HashMap<>();
		Map<String, Integer> preferredHostWithScore = new HashMap<>();
		for (TaskManagerLocation location : preferredLocations) {
			if (bannedResourceIDs.contains(location.getResourceID())
					|| bannedHostnames.contains(location.getFQDNHostname())) {
				continue;
			}
			preferredLocationWithScore.merge(location, 1, Integer::sum);
			preferredHostWithScore.merge(location.getFQDNHostname(), 1, Integer::sum);
		}

		final List<TaskManagerLocation> taskManagerLocations = new ArrayList<>(preferredLocationWithScore.keySet());
		taskManagerLocations.sort((o1, o2) -> preferredLocationWithScore.get(o2) - preferredLocationWithScore.get(o1));

		for (TaskManagerLocation location : taskManagerLocations) {
			optionalSlot = slotPool.getByTaskManagerLocation(
				resourceProfile,
				location,
				slot -> !blackListedAllocations.contains(slot.getAllocationId()));
			if (optionalSlot.isPresent()) {
				LOG.debug("allocate {} by TaskManager {}.", optionalSlot.get().getAllocationId(), location);
				return Optional.of(SlotInfoAndLocality.of(optionalSlot.get(), Locality.LOCAL));
			}
		}

		final List<String> hosts = new ArrayList<>(preferredHostWithScore.keySet());
		hosts.sort(((o1, o2) -> preferredHostWithScore.get(o2) - preferredHostWithScore.get(o1)));

		for (String host : hosts) {
			optionalSlot = slotPool.getByHost(
				resourceProfile,
				host,
				slot -> !blackListedAllocations.contains(slot.getAllocationId()));
			if (optionalSlot.isPresent()) {
				LOG.debug("allocate {} by host {}.", optionalSlot.get().getAllocationId(), host);
				return Optional.of(SlotInfoAndLocality.of(optionalSlot.get(), Locality.HOST_LOCAL));
			}
		}

		optionalSlot = slotPool.getNextAvailableSlot(
			resourceProfile,
			slot -> !blackListedAllocations.contains(slot.getAllocationId())
				&& !bannedLocations.contains(slot.getTaskManagerLocation())
				&& !bannedHostnames.contains(slot.getTaskManagerLocation().getFQDNHostname()));

		return optionalSlot.map(allocatedSlot -> SlotInfoAndLocality.of(allocatedSlot, noMatchLocality));
	}
}
