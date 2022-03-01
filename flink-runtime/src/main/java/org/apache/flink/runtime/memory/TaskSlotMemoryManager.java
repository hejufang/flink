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

import org.apache.flink.annotation.VisibleForTesting;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * All tasks from different jobs with the same slot index share the same memory manager.
 */
public class TaskSlotMemoryManager implements TaskMemoryManager {
	private final Map<Integer, MemoryManager> slotMemoryManagers = new HashMap<>();
	private final int slotCount;

	@VisibleForTesting
	public TaskSlotMemoryManager(
			long memorySize,
			int pageSize,
			int slotCount,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			boolean cacheEnable) {
		this(memorySize, pageSize, slotCount, requestMemorySegmentsTimeout, lazyAllocate, cacheEnable, false);
	}

	public TaskSlotMemoryManager(
			long memorySize,
			int pageSize,
			int slotCount,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			boolean cacheEnable,
			boolean checkSegmentOwnerEnable) {
		this.slotCount = slotCount;
		long slotMemorySize = memorySize / slotCount;
		for (int i = 0; i < slotCount; i++) {
			slotMemoryManagers.put(
				i,
				cacheEnable ? new MemoryPoolManager(
					slotMemorySize,
					pageSize,
					requestMemorySegmentsTimeout,
					lazyAllocate,
					1,
					checkSegmentOwnerEnable) : MemoryManager.create(slotMemorySize, pageSize));
		}
	}

	@Override
	public MemoryManager getMemoryManager(int slotIndex) {
		return checkNotNull(slotMemoryManagers.get(ThreadLocalRandom.current().nextInt(0, slotCount)));
	}

	@Override
	public Collection<MemoryManager> getMemoryManagers() {
		return slotMemoryManagers.values();
	}

	@Override
	public void close() {
		for (MemoryManager memoryManager : slotMemoryManagers.values()) {
			memoryManager.shutdown();
		}
		slotMemoryManagers.clear();
	}
}
