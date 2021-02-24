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

package org.apache.flink.yarn.slowcontainer;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.yarn.YarnResourceManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Slow container manager that do nothing.
 */
public class NoOpSlowContainerManager implements SlowContainerManager {
	public static final long CONTAINER_NOT_START_TIME_MS = -1;

	// Start duration of all managed containers. -1 when container not started.
	private final Map<ResourceID, Long> allContainers;
	// Allocated timestamp of containers which not started yet.
	private final Map<ResourceID, Long> startingContainers;

	public NoOpSlowContainerManager() {
		allContainers = new ConcurrentHashMap<>();
		startingContainers = new ConcurrentHashMap<>();
	}

	@Override
	public void setYarnResourceManager(YarnResourceManager yarnResourceManager) {}

	@Override
	public void containerAllocated(ResourceID resourceID, long ts, int numberPendingRequests) {
		startingContainers.put(resourceID, ts);
		allContainers.put(resourceID, CONTAINER_NOT_START_TIME_MS);
	}

	@Override
	public void containerStarted(ResourceID resourceID, int numberPendingRequests) {
		if (startingContainers.containsKey(resourceID)) {
			long startTime = startingContainers.remove(resourceID);
			allContainers.put(resourceID, System.currentTimeMillis() - startTime);
		}
	}

	@Override
	public void containerRemoved(ResourceID resourceID) {
		startingContainers.remove(resourceID);
		allContainers.remove(resourceID);
	}

	@Override
	public void checkSlowContainer() {}

	@Override
	public long getContainerStartTime(ResourceID resourceID) {
		return allContainers.getOrDefault(resourceID, CONTAINER_NOT_START_TIME_MS);
	}

	@Override
	public long getSpeculativeSlowContainerTimeoutMs() {
		return -1;
	}

	@Override
	public int getTotalRedundantContainersNum() {
		return 0;
	}

	@Override
	public int getStartingRedundantContainerSize() {
		return 0;
	}

	@Override
	public int getPendingRedundantContainersNum() {
		return 0;
	}

	@Override
	public int getStartingContainerSize() {
		return startingContainers.size();
	}

	@Override
	public Map<ResourceID, Long> getStartingContainers() {
		return startingContainers;
	}

	@Override
	public int getSlowContainerSize() {
		return 0;
	}
}
