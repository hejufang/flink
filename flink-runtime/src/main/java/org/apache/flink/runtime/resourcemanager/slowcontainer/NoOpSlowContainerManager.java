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

package org.apache.flink.runtime.resourcemanager.slowcontainer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

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

	private final Counter releaseTimeoutContainerNumber = new SimpleCounter();

	public NoOpSlowContainerManager() {
		allContainers = new ConcurrentHashMap<>();
		startingContainers = new ConcurrentHashMap<>();
	}

	@Override
	public void setSlowContainerActions(SlowContainerActions slowContainerActions) {}

	@Override
	public void notifyWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		startingContainers.put(resourceID, System.currentTimeMillis());
		allContainers.put(resourceID, CONTAINER_NOT_START_TIME_MS);
	}

	@Override
	public void notifyWorkerStarted(ResourceID resourceID) {
		if (startingContainers.containsKey(resourceID)) {
			long startTime = startingContainers.remove(resourceID);
			allContainers.put(resourceID, System.currentTimeMillis() - startTime);
		}
	}

	@Override
	public void notifyWorkerStopped(ResourceID resourceID) {
		startingContainers.remove(resourceID);
		allContainers.remove(resourceID);
	}

	@Override
	public void notifyRecoveredWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		startingContainers.put(resourceID, System.currentTimeMillis());
		allContainers.put(resourceID, CONTAINER_NOT_START_TIME_MS);
	}

	@Override
	public void notifyPendingWorkerFailed(WorkerResourceSpec workerResourceSpec) {
	}

	@Override
	public void checkSlowContainer() {}

	@Override
	public int getRedundantContainerTotalNum() {
		return 0;
	}

	@Override
	public int getRedundantContainerNum(WorkerResourceSpec workerResourceSpec) {
		return 0;
	}

	@Override
	public long getSpeculativeSlowContainerTimeoutMs() {
		return -1;
	}

	@Override
	public int getStartingRedundantContainerTotalNum() {
		return 0;
	}

	@Override
	public int getStartingRedundantContainerNum(WorkerResourceSpec workerResourceSpec) {
		return 0;
	}

	@Override
	public int getPendingRedundantContainersTotalNum() {
		return 0;
	}

	@Override
	public int getPendingRedundantContainersNum(WorkerResourceSpec workerResourceSpec) {
		return 0;
	}

	@Override
	public int getSlowContainerTotalNum() {
		return 0;
	}

	@Override
	public Map<ResourceID, Long> getStartingContainerWithTimestamp() {
		return startingContainers;
	}

	@Override
	public int getStartingContainerTotalNum() {
		return startingContainers.size();
	}

	@Override
	public Counter getReleaseTimeoutContainerNumber() {
		return releaseTimeoutContainerNumber;
	}
}
