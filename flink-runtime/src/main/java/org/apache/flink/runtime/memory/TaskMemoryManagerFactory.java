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

package org.apache.flink.runtime.memory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import java.time.Duration;

/**
 * Factory of TaskMemoryManager.
 */
public class TaskMemoryManagerFactory {
	private final boolean slotBasedEnable;
	private final long managedMemorySize;
	private final int pageSize;
	private final int slotCount;
	private final Duration requestMemorySegmentsTimeout;
	private final boolean lazyAllocate;
	private final boolean cacheEnable;
	private final boolean checkSegmentOwnerEnable;
	private final int batchSize;
	private final boolean releaseSegmentsFinallyEnable;

	private TaskMemoryManagerFactory(
			boolean slotBasedEnable,
			long managedMemorySize,
			int pageSize,
			int slotCount,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			boolean cacheEnable,
			boolean checkSegmentOwnerEnable,
			int batchSize,
			boolean releaseSegmentsFinallyEnable) {
		this.slotBasedEnable = slotBasedEnable;
		this.managedMemorySize = managedMemorySize;
		this.pageSize = pageSize;
		this.slotCount = slotCount;
		this.requestMemorySegmentsTimeout = requestMemorySegmentsTimeout;
		this.lazyAllocate = lazyAllocate;
		this.cacheEnable = cacheEnable;
		this.checkSegmentOwnerEnable = checkSegmentOwnerEnable;
		this.batchSize = batchSize;
		this.releaseSegmentsFinallyEnable = releaseSegmentsFinallyEnable;
	}

	public TaskMemoryManager buildTaskMemoryManager() {
		if (slotBasedEnable) {
			return new TaskSlotMemoryManager(managedMemorySize, pageSize, slotCount, requestMemorySegmentsTimeout, lazyAllocate, cacheEnable, checkSegmentOwnerEnable, batchSize, releaseSegmentsFinallyEnable);
		} else {
			return new TaskGlobalMemoryManager(managedMemorySize, pageSize, requestMemorySegmentsTimeout, lazyAllocate, slotCount, cacheEnable, checkSegmentOwnerEnable, batchSize, releaseSegmentsFinallyEnable);
		}
	}

	public static TaskMemoryManagerFactory fromConfiguration(Configuration configuration, long managedMemorySize, int pageSize) {
		return new TaskMemoryManagerFactory(
			configuration.getBoolean(TaskManagerOptions.MEMORY_MANAGER_SLOT_BASED_ENABLE),
			managedMemorySize,
			pageSize,
			configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS),
			configuration.get(TaskManagerOptions.ALLOCATE_MEMORY_SEGMENTS_TIMEOUT),
			configuration.getBoolean(TaskManagerOptions.MEMORY_POOL_MANAGER_SEGMENT_ALLOCATE_LAZY_ENABLE),
			configuration.getBoolean(TaskManagerOptions.MEMORY_POOL_MANAGER_ENABLE),
			configuration.getBoolean(TaskManagerOptions.MEMORY_POOL_SEGMENT_OWNER_CHECKER_ENABLE),
			configuration.getInteger(TaskManagerOptions.MEMORY_POOL_MANAGER_BATCH_SIZE),
			configuration.getBoolean(TaskManagerOptions.MEMORY_BATCH_POOL_MANAGER_RELEASE_SEGMENTS_FINALLY_ENABLE));
	}
}
