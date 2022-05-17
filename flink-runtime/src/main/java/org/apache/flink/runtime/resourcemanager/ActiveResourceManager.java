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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.failurerate.FailureRater;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slowcontainer.NoOpSlowContainerManager;
import org.apache.flink.runtime.resourcemanager.slowcontainer.SlowContainerActions;
import org.apache.flink.runtime.resourcemanager.slowcontainer.SlowContainerManager;
import org.apache.flink.runtime.resourcemanager.slowcontainer.SlowContainerManagerImpl;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Base class for {@link ResourceManager} implementations which contains some common variables and methods.
 */
public abstract class ActiveResourceManager <WorkerType extends ResourceIDRetrievable>
		extends ResourceManager<WorkerType> {

	/** The process environment variables. */
	protected final Map<String, String> env;

	/**
	 * The updated Flink configuration. The client uploaded configuration may be updated before passed on to
	 * {@link ResourceManager}. For example, {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
	 */
	protected final Configuration flinkConfig;

	/** Flink configuration uploaded by client. */
	protected final Configuration flinkClientConfig;

	private final WorkerResourceSpecCounter requestedNotAllocatedWorkerCounter;
	private final WorkerResourceSpecCounter requestedNotRegisteredWorkerCounter;

	/** Counter for all allocated worker. This includes allocated not registered workers and registered workers. */
	private final WorkerResourceSpecCounter allocatedWorkerCounter;

	/** Maps from worker's resource id to its resource spec. Only record allocated but not registered. */
	private final Map<ResourceID, WorkerResourceSpec> allocatedNotRegisteredWorkerResourceSpecs;
	/** Maps from worker's resource id to its resource spec. Only record registered. */
	private final Map<ResourceID, WorkerResourceSpec> registeredWorkerResourceSpecs;

	/** The allocated timestamp of worker. Only record allocated but not registred. */
	private final Map<ResourceID, Long> workerAllocatedTime;

	private final SlowContainerManager slowContainerManager;
	private final long slowContainerCheckIntervalMs;

	public ActiveResourceManager(
			Configuration flinkConfig,
			Map<String, String> env,
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			FailureRater failureRater) {
		super(
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			clusterPartitionTrackerFactory,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			AkkaUtils.getTimeoutAsTime(flinkConfig),
			failureRater,
			flinkConfig);

		this.flinkConfig = flinkConfig;
		this.env = env;

		// Load the flink config uploaded by flink client
		this.flinkClientConfig = loadClientConfiguration();

		requestedNotAllocatedWorkerCounter = new WorkerResourceSpecCounter();
		requestedNotRegisteredWorkerCounter = new WorkerResourceSpecCounter();
		allocatedWorkerCounter = new WorkerResourceSpecCounter();
		allocatedNotRegisteredWorkerResourceSpecs = new HashMap<>();
		registeredWorkerResourceSpecs = new HashMap<>();
		workerAllocatedTime = new HashMap<>();

		if (flinkConfig.getBoolean(ResourceManagerOptions.SLOW_CONTAINER_ENABLED)) {
			long slowContainerTimeoutMs = flinkConfig.getLong(ResourceManagerOptions.SLOW_CONTAINER_TIMEOUT_MS);
			double slowContainersQuantile = flinkConfig.getDouble(ResourceManagerOptions.SLOW_CONTAINERS_QUANTILE);
			Preconditions.checkArgument(
					slowContainersQuantile < 1.0,
					"%s must less than 1.0", ResourceManagerOptions.SLOW_CONTAINERS_QUANTILE.key());
			double slowContainerThresholdFactor = flinkConfig.getDouble(ResourceManagerOptions.SLOW_CONTAINER_THRESHOLD_FACTOR);
			Preconditions.checkArgument(
					slowContainerThresholdFactor > 1.0,
					"%s must great than 1.0", ResourceManagerOptions.SLOW_CONTAINER_THRESHOLD_FACTOR.key());
			double slowContainerRedundantMaxFactor = flinkConfig.getDouble(ResourceManagerOptions.SLOW_CONTAINER_REDUNDANT_MAX_FACTOR);
			int slowContainerRedundantMinNumber = flinkConfig.getInteger(ResourceManagerOptions.SLOW_CONTAINER_REDUNDANT_MIN_NUMBER);
			boolean slowContainerReleaseTimeoutEnabled = flinkConfig.getBoolean(ResourceManagerOptions.SLOW_CONTAINER_RELEASE_TIMEOUT_ENABLED);
			long slowContainerReleaseTimeoutMs = flinkConfig.getLong(ResourceManagerOptions.SLOW_CONTAINER_RELEASE_TIMEOUT_MS);
			this.slowContainerManager = new SlowContainerManagerImpl(
					slowContainerTimeoutMs,
					slowContainersQuantile,
					slowContainerThresholdFactor,
					slowContainerRedundantMaxFactor,
					slowContainerRedundantMinNumber,
					slowContainerReleaseTimeoutEnabled,
					slowContainerReleaseTimeoutMs);
		} else {
			this.slowContainerManager = new NoOpSlowContainerManager();
		}
		this.slowContainerCheckIntervalMs = flinkConfig.getLong(ResourceManagerOptions.SLOW_CONTAINER_CHECK_INTERVAL_MS);
	}

	protected CompletableFuture<Void> getStopTerminationFutureOrCompletedExceptionally(@Nullable Throwable exception) {
		final CompletableFuture<Void> terminationFuture = super.onStop();

		if (exception != null) {
			return FutureUtils.completedExceptionally(new FlinkException(
				"Error while shutting down resource manager", exception));
		} else {
			return terminationFuture;
		}
	}

	protected abstract Configuration loadClientConfiguration();

	@Override
	protected void onTaskManagerRegistration(WorkerRegistration<WorkerType> workerTypeWorkerRegistration) {
		notifyAllocatedWorkerRegistered(workerTypeWorkerRegistration.getResourceID());
	}

	protected int getNumRequestedNotAllocatedWorkers() {
		return requestedNotAllocatedWorkerCounter.getTotalNum();
	}

	protected int getNumRequestedNotAllocatedWorkersFor(WorkerResourceSpec workerResourceSpec) {
		return requestedNotAllocatedWorkerCounter.getNum(workerResourceSpec);
	}

	protected Map<WorkerResourceSpec, Integer> getNumRequestedNotAllocatedWorkersDetail() {
		return requestedNotAllocatedWorkerCounter.getWorkerNums();
	}

	protected int getNumRequestedNotRegisteredWorkers() {
		return requestedNotRegisteredWorkerCounter.getTotalNum();
	}

	protected int getNumRequestedNotRegisteredWorkersFor(WorkerResourceSpec workerResourceSpec) {
		return requestedNotRegisteredWorkerCounter.getNum(workerResourceSpec);
	}

	protected Map<ResourceID, Long> getWorkerAllocatedTime() {
		return workerAllocatedTime;
	}

	protected int getNumAllocatedWorkersFor(WorkerResourceSpec workerResourceSpec) {
		return allocatedWorkerCounter.getNum(workerResourceSpec);
	}

	protected int getNumAllocatedWorkers() {
		return allocatedWorkerCounter.getTotalNum();
	}

	protected Map<WorkerResourceSpec, Integer> getNumAllocatedWorkersDetail() {
		return allocatedWorkerCounter.getWorkerNums();
	}

	/**
	 * Notify that a worker with the given resource spec has been requested.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerRequested(WorkerResourceSpec workerResourceSpec) {
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.increaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.increaseAndGet(workerResourceSpec));
	}

	/**
	 * Notify that a worker with the given resource spec has been allocated.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @param resourceID id of the allocated resource
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		slowContainerManager.notifyWorkerAllocated(workerResourceSpec, resourceID);
		allocatedNotRegisteredWorkerResourceSpecs.put(resourceID, workerResourceSpec);
		workerAllocatedTime.put(resourceID, System.currentTimeMillis());
		allocatedWorkerCounter.increaseAndGet(workerResourceSpec);
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.decreaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.getNum(workerResourceSpec));
	}

	/**
	 * Notify that allocation of a worker with the given resource spec has failed.
	 * @param workerResourceSpec resource spec of the requested worker
	 * @return updated number of pending workers for the given resource spec
	 */
	protected PendingWorkerNums notifyNewWorkerAllocationFailed(WorkerResourceSpec workerResourceSpec) {
		slowContainerManager.notifyPendingWorkerFailed(workerResourceSpec);
		return new PendingWorkerNums(
			requestedNotAllocatedWorkerCounter.decreaseAndGet(workerResourceSpec),
			requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec));
	}

	/**
	 * Notify that a worker with the given resource spec has been registered.
	 * @param resourceID id of the registered worker resource
	 */
	protected void notifyAllocatedWorkerRegistered(ResourceID resourceID) {
		slowContainerManager.notifyWorkerStarted(resourceID);
		WorkerResourceSpec workerResourceSpec = allocatedNotRegisteredWorkerResourceSpecs.remove(resourceID);
		workerAllocatedTime.remove(resourceID);
		if (workerResourceSpec == null) {
			// ignore workers from previous attempt
			return;
		}
		registeredWorkerResourceSpecs.put(resourceID, workerResourceSpec);
		requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec);
	}

	/**
	 * Notify that a worker with the given resource spec has been stopped.
	 * @param resourceID id of the stopped worker resource
	 */
	protected void notifyAllocatedWorkerStopped(ResourceID resourceID) {
		slowContainerManager.notifyWorkerStopped(resourceID);
		WorkerResourceSpec workerResourceSpec = allocatedNotRegisteredWorkerResourceSpecs.remove(resourceID);
		workerAllocatedTime.remove(resourceID);
		if (workerResourceSpec == null) {
			workerResourceSpec = registeredWorkerResourceSpecs.remove(resourceID);
			if (workerResourceSpec != null) {
				allocatedWorkerCounter.decreaseAndGet(workerResourceSpec);
			}
			return;
		}
		allocatedWorkerCounter.decreaseAndGet(workerResourceSpec);
		requestedNotRegisteredWorkerCounter.decreaseAndGet(workerResourceSpec);
	}

	/**
	 * Notify that a recovered worker with the given resource spec has been allocated.
	 * @param resourceID id of the allocated worker resource
	 * @param workerResourceSpec resource spec of the requested worker
	 */
	protected void notifyRecoveredWorkerAllocated(WorkerResourceSpec workerResourceSpec, ResourceID resourceID) {
		slowContainerManager.notifyRecoveredWorkerAllocated(workerResourceSpec, resourceID);
		allocatedNotRegisteredWorkerResourceSpecs.put(resourceID, workerResourceSpec);
		workerAllocatedTime.put(resourceID, System.currentTimeMillis());
		allocatedWorkerCounter.increaseAndGet(workerResourceSpec);
		requestedNotRegisteredWorkerCounter.increaseAndGet(workerResourceSpec);
	}

	/**
	 * Number of workers pending for allocation/registration.
	 */
	protected static class PendingWorkerNums {

		private final int numNotAllocated;
		private final int numNotRegistered;

		private PendingWorkerNums(int numNotAllocated, int numNotRegistered) {
			this.numNotAllocated = numNotAllocated;
			this.numNotRegistered = numNotRegistered;
		}

		public int getNumNotAllocated() {
			return numNotAllocated;
		}

		public int getNumNotRegistered() {
			return numNotRegistered;
		}
	}

	// ------------------------------------------------------------------------
	//	Slow start container
	// ------------------------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		super.onStart();
		registerMetrics();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		slowContainerManager.setSlowContainerActions(new SlowContainerActionsImpl());
	}

	@Override
	protected void startServicesOnLeadership() {
		scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		super.startServicesOnLeadership();
	}

	public SlowContainerManager getSlowContainerManager() {
		return slowContainerManager;
	}

	private void checkSlowContainers() {
		validateRunsInMainThread();
		try {
			slowContainerManager.checkSlowContainer();
		} catch (Exception e) {
			log.warn("Error while checkSlowContainers.", e);
		} finally {
			scheduleRunAsync(this::checkSlowContainers, slowContainerCheckIntervalMs, TimeUnit.MILLISECONDS);
		}
	}

	private void registerMetrics() {
		resourceManagerMetricGroup.gauge("slowContainerNum", slowContainerManager::getSlowContainerTotalNum);
		resourceManagerMetricGroup.gauge("totalRedundantContainerNum", slowContainerManager::getRedundantContainerTotalNum);
		resourceManagerMetricGroup.gauge("pendingRedundantContainerNum", slowContainerManager::getPendingRedundantContainersTotalNum);
		resourceManagerMetricGroup.gauge("startingRedundantContainerNum", slowContainerManager::getStartingRedundantContainerTotalNum);
		resourceManagerMetricGroup.gauge("speculativeSlowContainerTimeoutMs", slowContainerManager::getSpeculativeSlowContainerTimeoutMs);
		resourceManagerMetricGroup.counter("releaseTimeoutContainerNum", slowContainerManager.getReleaseTimeoutContainerNumber());
	}

	/**
	 * Actions for slow container manager.
	 */
	private class SlowContainerActionsImpl implements SlowContainerActions {

		@Override
		public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
			validateRunsInMainThread();
			return ActiveResourceManager.this.startNewWorker(workerResourceSpec);
		}

		@Override
		public boolean stopWorker(ResourceID resourceID, int exitCode) {
			validateRunsInMainThread();
			return ActiveResourceManager.this.stopWorker(resourceID, exitCode);
		}

		@Override
		public boolean stopWorkerAndStartNewIfRequired(ResourceID resourceID, int exitCode) {
			validateRunsInMainThread();
			return ActiveResourceManager.this.stopWorkerAndStartNewIfRequired(resourceID, exitCode);
		}

		@Override
		public void releasePendingRequests(WorkerResourceSpec workerResourceSpec, int expectedNum) {
			validateRunsInMainThread();
			ActiveResourceManager.this.removePendingContainerRequest(workerResourceSpec, expectedNum);
		}

		@Override
		public int getNumRequestedNotAllocatedWorkersFor(WorkerResourceSpec workerResourceSpec) {
			validateRunsInMainThread();
			return ActiveResourceManager.this.getNumRequestedNotAllocatedWorkersFor(workerResourceSpec);
		}
	}

}
