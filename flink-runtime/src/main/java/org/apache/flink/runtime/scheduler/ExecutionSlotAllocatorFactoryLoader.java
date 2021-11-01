/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loader for {@link ExecutionSlotAllocator}.
 */
public class ExecutionSlotAllocatorFactoryLoader {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutionSlotAllocatorFactoryLoader.class);

	public static ExecutionSlotAllocatorFactory loadExecutionSlotAllocatorFactory(
			Configuration config, SlotProvider slotProvider, ScheduleMode scheduleMode, Time slotRequestTimeout, SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool, JobManagerJobMetricGroup jobManagerJobMetricGroup) {
		final boolean enabled = config.getBoolean(JobManagerOptions.SLOT_SHARING_EXECUTION_SLOT_ALLOCATOR_ENABLED);
		if (enabled) {
			LOG.info("Using {} as ExecutionSlotAllocatorFactory.", SlotSharingExecutionSlotAllocatorFactory.class.getName());
			final PhysicalSlotProvider physicalSlotProvider = new PhysicalSlotProviderImpl(slotSelectionStrategy, slotPool, jobManagerJobMetricGroup);
			boolean slotWillBeOccupiedIndefinitely;
			switch (scheduleMode) {
				case LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST:
					slotWillBeOccupiedIndefinitely = false;
					break;
				case LAZY_FROM_SOURCES:
				case EAGER:
					slotWillBeOccupiedIndefinitely = true;
					break;
				default:
					throw new IllegalArgumentException(String.format("Unknown scheduling mode: %s", scheduleMode));
			}
			return new SlotSharingExecutionSlotAllocatorFactory(physicalSlotProvider, slotWillBeOccupiedIndefinitely, slotRequestTimeout);
		} else {
			final SlotProviderStrategy slotProviderStrategy = SlotProviderStrategy.from(
					scheduleMode,
					slotProvider,
					slotRequestTimeout);
			LOG.info("Using {} as ExecutionSlotAllocatorFactory.", DefaultExecutionSlotAllocatorFactory.class.getName());
			return new DefaultExecutionSlotAllocatorFactory(slotProviderStrategy);
		}
	}
}
