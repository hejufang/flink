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
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpecCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Check and manager slow containers.
 * Allocate redundant containers for slow containers and release them when enough containers is started.
 */
public class SlowContainerManagerImpl implements SlowContainerManager {

	private final Logger log = LoggerFactory.getLogger(SlowContainerManagerImpl.class);

	public static final long CONTAINER_NOT_START_TIME_MS = -1;

	private final double slowContainersQuantile;
	private final double slowContainerThresholdFactor;
	private final long slowContainerTimeoutMs;

	private long speculativeSlowContainerTimeoutMs;

	// all containers.
	private final Map<ResourceID, StartingResource> containers;
	// slow containers.
	private final WorkerResourceSpecMap<ResourceID> slowContainers;
	// allocated not started containers for redundant.
	private final WorkerResourceSpecMap<ResourceID> startingRedundantContainers;
	// requested not allocated containers for redundant.
	private final WorkerResourceSpecCounter pendingRedundantContainers;
	// all containers for redundant.
	private final WorkerResourceSpecCounter allRedundantContainers;

	private SlowContainerActions slowContainerActions;

	private boolean running = true;

	public SlowContainerManagerImpl(long slowContainerTimeoutMs, double slowContainersQuantile, double slowContainerThresholdFactor) {
		containers = new ConcurrentHashMap<>();
		pendingRedundantContainers = new WorkerResourceSpecCounter();
		allRedundantContainers = new WorkerResourceSpecCounter();
		slowContainers = new WorkerResourceSpecMap<>();
		startingRedundantContainers = new WorkerResourceSpecMap<>();

		this.slowContainerTimeoutMs = slowContainerTimeoutMs;
		this.slowContainersQuantile = slowContainersQuantile;
		this.slowContainerThresholdFactor = slowContainerThresholdFactor;
		this.speculativeSlowContainerTimeoutMs = slowContainerTimeoutMs;
		log.info("start checkSlowContainers with slowContainerTimeoutMs: {}, slowContainerThresholdQuantile: {}, slowContainerThresholdQuantileTimes: {}.",
				slowContainerTimeoutMs, slowContainersQuantile, slowContainerThresholdFactor);
	}

	@Override
	public void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public void setSlowContainerActions(SlowContainerActions slowContainerActions) {
		this.slowContainerActions = slowContainerActions;
	}

	public void notifyWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		long ts = System.currentTimeMillis();
		boolean isRedundant = false;

		// RedundantContainers has the lowest priority.
		// Only when there are no other pending requests, the allocated container can be marked as redundant
		if (slowContainerActions.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec) <= pendingRedundantContainers.getNum(workerResourceSpec)) {
			log.info("mark container {} as redundant container.", resourceID);
			startingRedundantContainers.add(workerResourceSpec, resourceID);
			pendingRedundantContainers.decreaseAndGet(workerResourceSpec);
			isRedundant = true;
		}

		containers.put(resourceID, new StartingResource(resourceID, workerResourceSpec, ts, isRedundant));
	}

	public void notifyWorkerStarted(ResourceID resourceID) {
		if (containers.containsKey(resourceID) && !containers.get(resourceID).isRegistered()) {
			StartingResource startingResource = containers.get(resourceID);
			startingResource.registered(System.currentTimeMillis());
			WorkerResourceSpec workerResourceSpec = startingResource.getWorkerResourceSpec();
			slowContainers.remove(workerResourceSpec, resourceID);
			startingRedundantContainers.remove(workerResourceSpec, resourceID);

			List<StartingResource> startingResourceList = containers.values().stream()
					.filter(resource -> resource.getWorkerResourceSpec().equals(workerResourceSpec) && !resource.isRegistered())
					.collect(Collectors.toList());

			// only redundant container left.
			int requestedNotStartedContainerNum = startingResourceList.size() + slowContainerActions.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec);
			if (requestedNotStartedContainerNum > 0 && requestedNotStartedContainerNum <= allRedundantContainers.getNum(workerResourceSpec)) {
				log.info("Only totalRedundantContainersNum({}) containers not start, this means all needed container are started. " +
								"release all starting containers {} and {} pending requests",
						allRedundantContainers.getNum(workerResourceSpec), startingResourceList, slowContainerActions.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec));
				// release all starting redundant containers.
				for (StartingResource resource: startingResourceList) {
					releaseContainer(resource.getResourceID());
				}
				if (slowContainers.getNum(workerResourceSpec) != 0) {
					log.error("slow containers not empty after release all starting containers, {}. it is a bug, clear it forced",
							slowContainers.get(workerResourceSpec));
					slowContainers.clear(workerResourceSpec);
				}
				if (startingRedundantContainers.getNum(workerResourceSpec) != 0) {
					log.error("redundant containers not empty after release all starting containers, {}. it is a bug, clear it forced",
							startingRedundantContainers.get(workerResourceSpec));
					startingRedundantContainers.clear(workerResourceSpec);
				}

				// mark all redundant containers is not redundant.
				for (StartingResource resource : containers.values()) {
					if (resource.isRedundant()) {
						allRedundantContainers.decreaseAndGet(workerResourceSpec);
						resource.setRedundant(false);
					}
				}

				// release all pending requests.
				slowContainerActions.releasePendingRequests(workerResourceSpec, slowContainerActions.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec));

				// clear all state.
				if (pendingRedundantContainers.getNum(workerResourceSpec) != 0) {
					log.error("pending redundant container not empty after release all, {}", pendingRedundantContainers.getNum(workerResourceSpec));
					pendingRedundantContainers.setNum(workerResourceSpec, 0);
				}
				if (allRedundantContainers.getNum(workerResourceSpec) != 0) {
					log.error("all redundant container not empty after release all. {}", allRedundantContainers.getNum(workerResourceSpec));
					allRedundantContainers.setNum(workerResourceSpec, 0);
				}
			}
		}
	}

	@Override
	public void notifyWorkerStopped(ResourceID resourceID) {
		StartingResource startingResource = containers.remove(resourceID);
		if (startingResource != null) {
			WorkerResourceSpec workerResourceSpec = startingResource.getWorkerResourceSpec();
			slowContainers.remove(workerResourceSpec, resourceID);
			startingRedundantContainers.remove(workerResourceSpec, resourceID);
			if (startingResource.isRedundant()) {
				allRedundantContainers.decreaseAndGet(workerResourceSpec);
			}
		}
	}

	@Override
	public void notifyPendingWorkerFailed(WorkerResourceSpec workerResourceSpec) {
		if (pendingRedundantContainers.getNum(workerResourceSpec) > 0) {
			pendingRedundantContainers.decreaseAndGet(workerResourceSpec);
			allRedundantContainers.decreaseAndGet(workerResourceSpec);
			log.debug("Remove pending redundant workers.");
		}
	}

	private long getContainerStartQuantile(double quantile) {
		int quantileIndex = (int) Math.ceil(containers.size() * quantile) - 1;
		List<Long> startedContainerDuration = containers.values().stream()
				.filter(StartingResource::isRegistered)
				.map(StartingResource::getStartDuration)
				.sorted()
				.collect(Collectors.toList());
		if (quantileIndex > 0 && quantileIndex < startedContainerDuration.size()) {
			return startedContainerDuration.get(quantileIndex);
		} else {
			return CONTAINER_NOT_START_TIME_MS;
		}
	}

	@Override
	public void checkSlowContainer() {
		if (!running) {
			return;
		}

		List<StartingResource> startingResources =  containers.values().stream()
				.filter(startingResource -> !startingResource.isRegistered())
				.collect(Collectors.toList());

		if (startingResources.isEmpty()) {
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

		for (StartingResource startingContainer : startingResources) {
			ResourceID resourceID = startingContainer.getResourceID();
			WorkerResourceSpec workerResourceSpec = startingContainer.getWorkerResourceSpec();
			long waitedTimeMillis = System.currentTimeMillis() - startingContainer.getStartTimestamp();
			if (waitedTimeMillis > speculativeSlowContainerTimeoutMs || waitedTimeMillis > slowContainerTimeoutMs) {
				if (startingRedundantContainers.contains(workerResourceSpec, resourceID)) {
					log.info("{} not started in {} milliseconds, but this container if redundant, will not allocate new.",
							resourceID, waitedTimeMillis);
				} else if (!slowContainers.contains(workerResourceSpec, resourceID)) {
					log.info("{} not started in {} milliseconds, try to allocate new container.", resourceID, waitedTimeMillis);
					slowContainers.add(workerResourceSpec, resourceID);
					startNewContainer(resourceID, startingContainer.getWorkerResourceSpec());
				} else if (slowContainers.getNum(workerResourceSpec) > allRedundantContainers.getNum(workerResourceSpec)) {
					log.info("{} not started in {} milliseconds and slow container number more than redundant container number, " +
							"this may be redundant container failed. try to allocate new container.", resourceID, waitedTimeMillis);
					startNewContainer(resourceID, startingContainer.getWorkerResourceSpec());
				} else {
					log.info("{} not started in {} milliseconds.", resourceID, waitedTimeMillis);
				}
			}
		}
		log.debug("current slow containers: {}", slowContainers);
		log.debug("current redundant containers: {}", startingRedundantContainers);
	}

	@Override
	public long getSpeculativeSlowContainerTimeoutMs() {
		return speculativeSlowContainerTimeoutMs;
	}

	@Override
	public int getRedundantContainerTotalNum() {
		return allRedundantContainers.getTotalNum();
	}

	@Override
	public int getRedundantContainerNum(WorkerResourceSpec workerResourceSpec) {
		return allRedundantContainers.getNum(workerResourceSpec);
	}

	@Override
	public int getStartingRedundantContainerTotalNum() {
		return startingRedundantContainers.getTotalNum();
	}

	@Override
	public int getStartingRedundantContainerNum(WorkerResourceSpec workerResourceSpec) {
		return startingRedundantContainers.getNum(workerResourceSpec);
	}

	@Override
	public int getPendingRedundantContainersTotalNum() {
		return pendingRedundantContainers.getTotalNum();
	}

	@Override
	public int getPendingRedundantContainersNum(WorkerResourceSpec workerResourceSpec) {
		return pendingRedundantContainers.getNum(workerResourceSpec);
	}

	@Override
	public int getSlowContainerTotalNum() {
		return slowContainers.getTotalNum();
	}

	@Override
	public int getStartingContainerTotalNum() {
		return (int) containers.values().stream()
				.filter(startingResource -> !startingResource.isRegistered())
				.count();
	}

	@Override
	public Map<ResourceID, Long> getStartingContainerWithTimestamp() {
		return containers.values().stream()
				.filter(startingResource -> !startingResource.isRegistered())
				.collect(Collectors.toMap(StartingResource::getResourceID, StartingResource::getStartTimestamp));
	}

	private void startNewContainer(ResourceID oldResourceId, WorkerResourceSpec workerResourceSpec) {
		if (slowContainerActions != null) {
			log.info("try to start new container for slow container {}.", oldResourceId);
			if (slowContainerActions.startNewWorker(workerResourceSpec)) {
				allRedundantContainers.increaseAndGet(workerResourceSpec);
				pendingRedundantContainers.increaseAndGet(workerResourceSpec);
			}
		}
	}

	private void releaseContainer(ResourceID resourceID) {
		if (slowContainerActions != null) {
			log.info("try to release container {} because of slow container.", resourceID);
			slowContainerActions.stopWorker(resourceID, WorkerExitCode.SLOW_CONTAINER);
		}
	}

	static class StartingResource {
		final ResourceID resourceID;
		final WorkerResourceSpec workerResourceSpec;
		final long startTimestamp;
		boolean isRedundant;
		long registerTimestamp = CONTAINER_NOT_START_TIME_MS;

		public StartingResource(ResourceID resourceID, WorkerResourceSpec workerResourceSpec, long startTimestamp, boolean isRedundant) {
			this.resourceID = resourceID;
			this.workerResourceSpec = workerResourceSpec;
			this.startTimestamp = startTimestamp;
			this.isRedundant = isRedundant;
		}

		void registered(long ts) {
			this.registerTimestamp = ts;
		}

		public ResourceID getResourceID() {
			return resourceID;
		}

		public long getStartTimestamp() {
			return startTimestamp;
		}

		public WorkerResourceSpec getWorkerResourceSpec() {
			return workerResourceSpec;
		}

		public boolean isRedundant() {
			return isRedundant;
		}

		public void setRedundant(boolean redundant) {
			isRedundant = redundant;
		}

		public long getStartDuration() {
			if (registerTimestamp != CONTAINER_NOT_START_TIME_MS) {
				return registerTimestamp - startTimestamp;
			} else {
				return CONTAINER_NOT_START_TIME_MS;
			}
		}

		public boolean isRegistered() {
			return registerTimestamp != CONTAINER_NOT_START_TIME_MS;
		}

		@Override
		public String toString() {
			return resourceID.getResourceIdString();
		}
	}

	/**
	 * WorkerResourceSpecMap save resources with WorkerResourceSpec.
	 * @param <T>
	 */
	public static class WorkerResourceSpecMap<T> {
		Map<WorkerResourceSpec, Set<T>> workerResourceSpecSetMap;

		public WorkerResourceSpecMap() {
			this.workerResourceSpecSetMap = new ConcurrentHashMap<>();
		}

		public void add(WorkerResourceSpec workerResourceSpec, T t) {
			if (!workerResourceSpecSetMap.containsKey(workerResourceSpec)) {
				workerResourceSpecSetMap.put(workerResourceSpec, ConcurrentHashMap.newKeySet());
			}
			workerResourceSpecSetMap.get(workerResourceSpec).add(t);
		}

		public boolean remove(WorkerResourceSpec workerResourceSpec, T t) {
			if (workerResourceSpecSetMap.containsKey(workerResourceSpec)) {
				return workerResourceSpecSetMap.get(workerResourceSpec).remove(t);
			} else {
				return false;
			}
		}

		public void clear(WorkerResourceSpec workerResourceSpec) {
			if (workerResourceSpecSetMap.containsKey(workerResourceSpec)) {
				workerResourceSpecSetMap.get(workerResourceSpec).clear();
			}
		}

		public boolean contains(WorkerResourceSpec workerResourceSpec, T t) {
			if (workerResourceSpecSetMap.containsKey(workerResourceSpec)) {
				return workerResourceSpecSetMap.get(workerResourceSpec).contains(t);
			} else {
				return false;
			}
		}

		public int getNum(WorkerResourceSpec workerResourceSpec) {
			return workerResourceSpecSetMap.getOrDefault(workerResourceSpec, Collections.emptySet()).size();
		}

		public int getTotalNum() {
			return workerResourceSpecSetMap.values().stream().map(Set::size).reduce(0, Integer::sum);
		}

		public Set<T> get(WorkerResourceSpec workerResourceSpec) {
			return workerResourceSpecSetMap.get(workerResourceSpec);
		}

		@Override
		public String toString() {
			return "WorkerResourceSpecMap{" +
					"workerResourceSpecSetMap=" + workerResourceSpecSetMap +
					'}';
		}
	}
}
