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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

/**
 * All tasks from different jobs share the memory manager.
 */
public class TaskGlobalMemoryManager implements TaskMemoryManager {
	private final MemoryManager memoryManager;

	public TaskGlobalMemoryManager(
			long memorySize,
			int pageSize,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			int slotCount,
			boolean cacheEnable) {
		memoryManager = cacheEnable ? new CacheMemoryManager(
							memorySize,
							pageSize,
							requestMemorySegmentsTimeout,
							lazyAllocate,
							slotCount) : MemoryManager.create(memorySize, pageSize);
	}

	@Override
	public MemoryManager getMemoryManager(int slotIndex) {
		return memoryManager;
	}

	@Override
	public Collection<MemoryManager> getMemoryManagers() {
		return Collections.singleton(memoryManager);
	}

	@Override
	public void close() {
		memoryManager.shutdown();
	}
}
