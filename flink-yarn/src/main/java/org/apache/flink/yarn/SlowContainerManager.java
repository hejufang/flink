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

package org.apache.flink.yarn;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Check and manager slow containers.
 * Allocate redundant containers for slow containers and release them when enough containers is started.
 */
class SlowContainerManager {

	private final Logger log = LoggerFactory.getLogger(SlowContainerManager.class);

	public static final long CONTAINER_NOT_START_TIME_MS = -1;
	private static final int RELEASE_CONTAINER_CODE = 30001;

	private final double slowContainersQuantile;
	private final double slowContainerThresholdFactor;
	private final long slowContainerTimeoutMs;

	// Start duration of all managed containers. -1 when container not started.
	private final Map<ResourceID, Long> allContainers;
	// Allocated timestamp of containers which not started yet.
	private final Map<ResourceID, Long> startingContainers;
	// Slow containers.
	private final Set<ResourceID> slowContainers;
	// allocated containers for slow container.
	private final Set<ResourceID> redundantContainers;
	private int totalRedundantContainersNum = 0;
	private int pendingRedundantContainersNum = 0;
	private long speculativeSlowContainerTimeoutMs;

	private YarnResourceManager yarnResourceManager;

	SlowContainerManager(long slowContainerTimeoutMs, double slowContainersQuantile, double slowContainerThresholdFactor) {
		startingContainers = new ConcurrentHashMap<>();
		allContainers = new ConcurrentHashMap<>();
		slowContainers = ConcurrentHashMap.newKeySet();
		redundantContainers = ConcurrentHashMap.newKeySet();

		this.slowContainerTimeoutMs = slowContainerTimeoutMs;
		this.slowContainersQuantile = slowContainersQuantile;
		this.slowContainerThresholdFactor = slowContainerThresholdFactor;
		this.speculativeSlowContainerTimeoutMs = slowContainerTimeoutMs;
		log.info("start checkSlowAllocation with slowContainerTimeoutMs: {}, slowContainerThresholdQuantile: {}, slowContainerThresholdQuantileTimes: {}.",
				slowContainerTimeoutMs, slowContainersQuantile, slowContainerThresholdFactor);
	}

	public void setYarnResourceManager(YarnResourceManager yarnResourceManager) {
		this.yarnResourceManager = yarnResourceManager;
	}

	public void containerAllocated(ResourceID resourceID, long ts) {
		this.containerAllocated(resourceID, ts, 0);
	}

	public void containerAllocated(ResourceID resourceID, long ts, int numberPendingRequests) {
		startingContainers.put(resourceID, ts);
		allContainers.put(resourceID, CONTAINER_NOT_START_TIME_MS);
		// RedundantContainers has the lowest priority.
		// Only when there are no other pending requests, the allocated container can be marked as redundant
		// the numberPendingRequests is not include this container, so the value can not be equals to wantRedundantContainers.
		if (numberPendingRequests < pendingRedundantContainersNum) {
			log.info("mark container {} as redundant container.", resourceID);
			redundantContainers.add(resourceID);
			pendingRedundantContainersNum--;
		}
	}

	public void containerStarted(ResourceID resourceID, int numberPendingRequests) {
		if (startingContainers.containsKey(resourceID)) {
			long startTime = startingContainers.remove(resourceID);
			allContainers.put(resourceID, System.currentTimeMillis() - startTime);
			slowContainers.remove(resourceID);
			redundantContainers.remove(resourceID);

			// only redundant container left.
			if (startingContainers.size() + numberPendingRequests <= totalRedundantContainersNum) {
				log.info("Only totalRedundantContainersNum({}) containers not start, this means all needed container are started. " +
								"release all starting containers {} and {} pending requests",
						totalRedundantContainersNum, startingContainers.keySet(), numberPendingRequests);
				yarnResourceManager.removePendingRequests(numberPendingRequests);
				totalRedundantContainersNum = 0;
				pendingRedundantContainersNum = 0;
				for (ResourceID rID : startingContainers.keySet()) {
					releaseContainer(rID);
				}
			}
		}
	}

	public void containerRemoved(ResourceID resourceID) {
		startingContainers.remove(resourceID);
		allContainers.remove(resourceID);
		slowContainers.remove(resourceID);
		if (redundantContainers.remove(resourceID)) {
			totalRedundantContainersNum--;
		}
	}

	private long getContainerStartQuantile(double quantile) {
		int quantileIndex = (int) Math.ceil(allContainers.size() * quantile) - 1;
		if (quantileIndex > 0) {
			return allContainers.values().stream()
					.sorted()
					.collect(Collectors.toList())
					.get(quantileIndex);
		} else {
			return CONTAINER_NOT_START_TIME_MS;
		}
	}

	public void checkSlowContainer() {
		if (startingContainers.isEmpty()) {
			return;
		}

		long containerStartQuantile = getContainerStartQuantile(slowContainersQuantile);
		if (containerStartQuantile != CONTAINER_NOT_START_TIME_MS) {
			long updatedTimeout = (long) (containerStartQuantile * slowContainerThresholdFactor);
			if (speculativeSlowContainerTimeoutMs != updatedTimeout) {
				speculativeSlowContainerTimeoutMs = updatedTimeout;
				log.info("Update slow container threshold to {} ms.", speculativeSlowContainerTimeoutMs);
			}
		}

		for (Map.Entry<ResourceID, Long> startingContainer : startingContainers.entrySet()) {
			ResourceID resourceID = startingContainer.getKey();
			long waitedTimeMillis = System.currentTimeMillis() - startingContainer.getValue();
			if (waitedTimeMillis > speculativeSlowContainerTimeoutMs || waitedTimeMillis > slowContainerTimeoutMs) {
				if (redundantContainers.contains(resourceID)) {
					log.info("{} not started in {} milliseconds, but this container if redundant, will not allocate new.",
							resourceID, waitedTimeMillis);
				} else if (!slowContainers.contains(resourceID)) {
					log.info("{} not started in {} milliseconds, try to allocate new container.", resourceID, waitedTimeMillis);
					slowContainers.add(resourceID);
					startNewContainer(resourceID);
				} else if (slowContainers.size() > totalRedundantContainersNum) {
					log.info("{} not started in {} milliseconds and slow container number more than redundant container number, " +
							"this may be redundant container failed. try to allocate new container.", resourceID, waitedTimeMillis);
					startNewContainer(resourceID);
				} else {
					log.info("{} not started in {} milliseconds.", resourceID, waitedTimeMillis);
				}
			}
		}
		log.debug("current slow containers: {}", slowContainers);
		log.debug("current redundant containers: {}", redundantContainers);
	}

	public long getContainerStartTime(ResourceID resourceID) {
		return allContainers.getOrDefault(resourceID, CONTAINER_NOT_START_TIME_MS);
	}

	public long getSpeculativeSlowContainerTimeoutMs() {
		return speculativeSlowContainerTimeoutMs;
	}

	public int getTotalRedundantContainersNum() {
		return totalRedundantContainersNum;
	}

	public int getRedundantContainerSize() {
		return redundantContainers.size();
	}

	public int getPendingRedundantContainersNum() {
		return pendingRedundantContainersNum;
	}

	public int getSlowContainerSize() {
		return slowContainers.size();
	}

	public int getStartingContainerSize() {
		return startingContainers.size();
	}

	private void startNewContainer(ResourceID oldResourceId) {
		if (yarnResourceManager != null) {
			log.info("try to start new container for slow container {}.", oldResourceId);
			totalRedundantContainersNum++;
			pendingRedundantContainersNum++;
			yarnResourceManager.startNewWorker(oldResourceId);
		}
	}

	private void releaseContainer(ResourceID resourceID) {
		if (yarnResourceManager != null) {
			log.info("try to release container {} because of slow container.", resourceID);
			yarnResourceManager.stopWorker(resourceID, RELEASE_CONTAINER_CODE);
		}
	}
}
