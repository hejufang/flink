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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.runtime.state.cache.memory.MemoryManager;
import org.apache.flink.runtime.state.cache.monitor.CacheStatusMonitor;
import org.apache.flink.runtime.state.cache.monitor.HeapStatusMonitor;
import org.apache.flink.runtime.state.cache.scale.ScalingManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for managing all registered {@link Cache} in {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
public class CacheManager {
	private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);
	/** A manager that manages the memory used by the cache. */
	private final MemoryManager memoryManager;
	/** A monitor that monitors the status of the heap. */
	private final HeapStatusMonitor heapStatusMonitor;
	/** A monitor that monitors the running status of the cache. */
	private final CacheStatusMonitor cacheStatusMonitor;
	/** A manager that manages the cache to scale. */
	private final ScalingManager scalingManager;

	public CacheManager() {
		//TODO initialize all internal components.
		this.memoryManager = null;
		this.cacheStatusMonitor = null;
		this.scalingManager = null;
		this.heapStatusMonitor = null;
	}

	public void startService() {
		//TODO start all monitoring services.
	}

	public Cache registerCache() {
		//TODO create a new cache and return.
		return null;
	}

	public void unRegisterCache(Cache cache) {
		//TODO release the cache and clear the reference.
	}

	public void shutDown() {
		//TODO stop all monitoring services, release all caches, and clean up memory.
	}
}
