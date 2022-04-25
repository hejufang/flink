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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobSlotRequestList;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.JobInfo;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskmanager.TaskManagerAddressLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of {@link SlotManager} which only maintain containers.
 */
public class NoSlotWorkerManagerImpl implements SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(NoSlotWorkerManagerImpl.class);

	/** All currently registered task managers. */
	private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Callbacks for resource (de-)allocations. */
	private ResourceActions resourceActions;

	/** True iff the component has been started. */
	private boolean started;

	/** Defines the max limitation of the total number of workers. */
	private final int maxWorkerNumber;

	/** Defines the min limitation of the total number of workers. */
	private final int minWorkerNumber;

	/** Initial task managers on slot manager start. */
	private final boolean initialTaskManager;

	/** Number of task managers. */
	private int numInitialTaskManagers = 0;

	/** Number of taskmanagers need to be ready. */
	private int pendingTaskManagers;

	/**
	 * The default resource spec of workers to request.
	 */
	private final WorkerResourceSpec defaultWorkerResourceSpec;

	private final ResourceProfile defaultWorkerResourceProfile;

	private final SlotManagerMetricGroup slotManagerMetricGroup;

	public NoSlotWorkerManagerImpl(
			SlotManagerConfiguration slotManagerConfiguration,
			SlotManagerMetricGroup slotManagerMetricGroup) {

		checkNotNull(slotManagerConfiguration);
		this.defaultWorkerResourceSpec = slotManagerConfiguration.getDefaultWorkerResourceSpec();
		this.defaultWorkerResourceProfile =  ResourceProfile.newBuilder()
			.setCpuCores(defaultWorkerResourceSpec.getCpuCores())
			.setTaskHeapMemory(defaultWorkerResourceSpec.getTaskHeapSize())
			.setTaskOffHeapMemory(defaultWorkerResourceSpec.getTaskOffHeapSize())
			.setManagedMemory(defaultWorkerResourceSpec.getManagedMemSize())
			.setNetworkMemory(defaultWorkerResourceSpec.getNetworkMemSize())
			.build();
		this.slotManagerMetricGroup = checkNotNull(slotManagerMetricGroup);

		this.numInitialTaskManagers = slotManagerConfiguration.getNumInitialTaskManagers();
		this.initialTaskManager = slotManagerConfiguration.isInitialTaskManager();
		this.minWorkerNumber = slotManagerConfiguration.getMinWorkerNum();
		this.maxWorkerNumber = slotManagerConfiguration.getMaxWorkerNum();
		checkArgument(minWorkerNumber <= maxWorkerNumber,
			"minimal workers should be small or equal to maximum workers.");

		taskManagerRegistrations = new HashMap<>(4);

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		pendingTaskManagers = 0;

		started = false;
	}

	@Override
	public int getNumberRegisteredSlots() {
		return 0;
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		return 0;
	}

	@Override
	public int getNumberFreeSlots() {
		return 0;
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		return 0;
	}

	@Override
	public int getNumSlotsPerWorker() {
		return 0;
	}

	@Override
	public int numTaskManagersNeedRequest() {
		if (initialTaskManager) {
			return Math.min(Math.max(minWorkerNumber, numInitialTaskManagers), maxWorkerNumber);
		} else {
			return minWorkerNumber;
		}
	}

	@Override
	public Map<WorkerResourceSpec, Integer> getRequiredResources() {
		return pendingTaskManagers > 0 ?
			Collections.singletonMap(defaultWorkerResourceSpec, pendingTaskManagers) :
			Collections.emptyMap();
	}

	@Override
	public ResourceProfile getRegisteredResource() {
		return defaultWorkerResourceProfile.multiply(taskManagerRegistrations.size());
	}

	@Override
	public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
		return getDefaultResource();
	}

	@Override
	public ResourceProfile getFreeResource() {
		return ResourceProfile.ZERO;
	}

	@Override
	public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
		return ResourceProfile.ZERO;
	}

	@Override
	public ResourceProfile getDefaultResource() {
		return defaultWorkerResourceProfile;
	}

	@Override
	public WorkerResourceSpec getDefaultWorkerResourceSpec() {
		return defaultWorkerResourceSpec;
	}

	@Override
	public int getNumberPendingSlotRequests() {
		return 0;
	}

	@Override
	public void initializeJobResources(JobID jobID, JobInfo jobInfo) {
		// nothing to do
	}

	@Override
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the NoSlotWorkerManager.");

		this.resourceManagerId = checkNotNull(newResourceManagerId);
		mainThreadExecutor = checkNotNull(newMainThreadExecutor);
		resourceActions = checkNotNull(newResourceActions);

		pendingTaskManagers = numTaskManagersNeedRequest();

		resourceActions.allocateResources(defaultWorkerResourceSpec, pendingTaskManagers);

		started = true;

		registerSlotManagerMetrics();
	}

	private void registerSlotManagerMetrics() {
		// TODO
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	@Override
	public void suspend() {
		LOG.info("Suspending the SlotManager.");

		ArrayList<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

		for (InstanceID registeredTaskManager : registeredTaskManagers) {
			unregisterTaskManager(registeredTaskManager, new SlotManagerException("The slot manager is being suspended."), false);
		}

		resourceManagerId = null;
		resourceActions = null;
		started = false;
	}

	@Override
	public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
		// True for register slot request successfully; otherwise false
		return false;
	}

	@Override
	public boolean registerJobSlotRequests(JobSlotRequestList jobSlotRequestList) throws ResourceManagerException {
		return false;
	}

	@Override
	public boolean registerJobSlotRequests(JobMasterGateway jobMasterGateway, JobSlotRequestList jobSlotRequestList) throws ResourceManagerException {
		// True for register job slot request successfully; otherwise false
		return false;
	}

	@Override
	public boolean registerJobSlotRequests(JobMasterGateway jobMasterGateway, JobSlotRequestList jobSlotRequestList, int jobWorkerCount) throws ResourceManagerException {
		return false;
	}

	@Override
	public void cancelAllPendingSlotRequests(Exception cause) {
		// nothing to do
	}

	@Override
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		// True if a pending slot request was found; otherwise false
		return false;
	}

	/**
	 * Registers a new task manager at the slot manager.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 * @return True if the task manager has not been registered before and is registered successfully; otherwise false
	 */
	@Override
	public boolean registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		return registerTaskManager(taskExecutorConnection, initialSlotReport, null);
	}

	@Override
	public boolean registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport, TaskManagerAddressLocation taskManagerAddressLocation) {
		checkInit();

		LOG.debug("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			return false;
		} else {
			if (pendingTaskManagers == 0) {
				LOG.info("Release the excess resource.");
				resourceActions.releaseResource(taskExecutorConnection.getInstanceID(), new FlinkException("The total number of taskmanager exceeds the max limitation."), WorkerExitCode.EXCESS_CONTAINER);
				return false;
			}

			// first register the TaskManager
			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
				taskExecutorConnection,
				Collections.emptyList(),
				taskManagerAddressLocation);

			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);
			pendingTaskManagers--;
			return true;
		}
	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
		return unregisterTaskManager(instanceId, cause, true);
	}

	public boolean unregisterTaskManager(InstanceID instanceId, Exception cause, boolean tryAllocateNewResource) {
		checkInit();

		LOG.debug("Unregister TaskManager {} from the SlotManager due to {}.", instanceId, cause.getMessage());

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

		if (null != taskManagerRegistration) {
			resourceActions.releaseResource(instanceId, cause, WorkerExitCode.IDLE_TIMEOUT);
			LOG.info("the register container number {}, job need min number of container {}", taskManagerRegistrations.size(), numTaskManagersNeedRequest());
			if (tryAllocateNewResource && taskManagerRegistrations.size() < numTaskManagersNeedRequest()) {
				allocateResource();
				pendingTaskManagers++;
			}
			return true;
		} else {
			LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

			return false;
		}
	}

	public void requestNewTaskManagers(int numNewTaskManagerRequests) {
		checkInit();

		LOG.debug("request new TM: {}, current registered TM: {}", numNewTaskManagerRequests, taskManagerRegistrations.size());
		while (!isMaxTaskManagersNumExceededAfterAdding()) {
			if (numNewTaskManagerRequests > 0) {
				allocateResource();
				numNewTaskManagerRequests--;
				pendingTaskManagers++;
			} else {
				break;
			}
		}
	}

	@Override
	public void unregisterJobMaster(JobInfo jobInfo) {
		// nothing to do
	}

	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		// True if the slot status has been updated successfully; otherwise false
		return false;
	}

	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		// nothing to do
	}

	@Override
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		// nothing to do
	}

	@Override
	public void unregisterTaskManagersAndReleaseResources() {
		Iterator<Map.Entry<InstanceID, TaskManagerRegistration>> taskManagerRegistrationIterator =
			taskManagerRegistrations.entrySet().iterator();

		while (taskManagerRegistrationIterator.hasNext()) {
			TaskManagerRegistration taskManagerRegistration =
				taskManagerRegistrationIterator.next().getValue();

			taskManagerRegistrationIterator.remove();

			final FlinkException cause = new FlinkException("Triggering of SlotManager#unregisterTaskManagersAndReleaseResources.");
			resourceActions.releaseResource(taskManagerRegistration.getInstanceId(), cause, WorkerExitCode.UNKNOWN);
		}
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the SlotManager.");

		suspend();
		slotManagerMetricGroup.close();
	}

	private boolean isMaxTaskManagersNumExceededAfterAdding() {
		return taskManagerRegistrations.size() + pendingTaskManagers  + 1 > maxWorkerNumber;
	}

	private boolean allocateResource() {
		if (isMaxTaskManagersNumExceededAfterAdding()) {
			LOG.warn("Could not allocate more TaskManagers. The number of registered and pending TaskManagers is {}, while the maximum is {}."
				, taskManagerRegistrations.size() + pendingTaskManagers, maxWorkerNumber);
			return false;
		}

		return resourceActions.allocateResource(defaultWorkerResourceSpec);
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------
	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}
}
