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

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.jobgraph.ScheduleMode;

import java.util.HashMap;
import java.util.Map;

/**
 * Loader for {@link SlotPoolFactory}.
 */
public class SlotPoolFactoryLoader {
	public static SlotPoolFactory createSlotPoolFactory(Configuration configuration, ScheduleMode scheduleMode) {
		if (configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED)) {
			throw new IllegalArgumentException("Must provide TaskManagers when enable " + ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED.key());
		}
		return createSlotPoolFactory(configuration, scheduleMode, new HashMap<>());
	}

	public static SlotPoolFactory createSlotPoolFactory(Configuration configuration, ScheduleMode scheduleMode, Map<ResourceID, ResolvedTaskManagerTopology> taskManagers) {
		if (configuration.getBoolean(ClusterOptions.JM_RESOURCE_ALLOCATION_ENABLED)) {
			return new VirtualTaskManagerSlotPool.VirtualTaskManagerSlotPoolFactory(
					taskManagers, configuration.getBoolean(JobManagerOptions.JOBMANAGER_REQUEST_SLOT_FROM_RESOURCEMANAGER_ENABLE));
		}

		if (scheduleMode.equals(ScheduleMode.EAGER)) {
			if (configuration.getBoolean(JobManagerOptions.SLOT_POOL_ROUND_ROBIN)) {
				return RoundRobinSlotPoolFactory.fromConfiguration(configuration);
			}
		}
		return DefaultSlotPoolFactory.fromConfiguration(configuration);
	}
}
