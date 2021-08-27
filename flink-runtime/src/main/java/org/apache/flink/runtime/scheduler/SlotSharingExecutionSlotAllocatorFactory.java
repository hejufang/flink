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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory for {@link SlotSharingExecutionSlotAllocator}.
 */
public class SlotSharingExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
	private final PhysicalSlotProvider slotProvider;

	// It means streaming slot currently.
	private final boolean slotWillBeOccupiedIndefinitely;

	private final Time allocationTimeout;

	public SlotSharingExecutionSlotAllocatorFactory(
			PhysicalSlotProvider slotProvider,
			boolean slotWillBeOccupiedIndefinitely,
			Time allocationTimeout) {
		this.slotProvider = slotProvider;
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.allocationTimeout = allocationTimeout;
	}

	@Override
	public ExecutionSlotAllocator createInstance(final InputsLocationsRetriever inputsLocationsRetriever, final ExecutionSlotAllocationContext context) {
		SlotSharingStrategy slotSharingStrategy = new LocalInputPreferredSlotSharingStrategy(
			context.getSchedulingTopology(),
			context.getLogicalSlotSharingGroups(),
			context.getCoLocationGroups(),
			context::getResourceProfile);

		if (slotWillBeOccupiedIndefinitely) {
			Map<ResourceProfile, Integer> requiredResources = slotSharingStrategy
					.getExecutionSlotSharingGroupMapByResourceProfile()
					.entrySet()
					.stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size()));
			slotProvider.setRequiredResources(requiredResources);
		}

		PreferredLocationsRetriever preferredLocationsRetriever = new DefaultPreferredLocationsRetriever(
			context.getStateLocationRetriever(),
			context.getInputsLocationsRetriever(),
			slotSharingStrategy::getPreferredExecutionSlotSharingGroups);
		SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory = new MergingSharedSlotProfileRetrieverFactory(
			preferredLocationsRetriever,
			context::getResourceProfile,
			context::getPriorAllocationId);
		return new SlotSharingExecutionSlotAllocator(
			slotProvider,
			slotWillBeOccupiedIndefinitely,
			slotSharingStrategy,
			sharedSlotProfileRetrieverFactory,
			allocationTimeout);
	}
}
