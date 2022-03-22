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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link NoSlotWorkerManagerImpl}.
 */
public class NoSlotWorkerManagerImplTest extends TestLogger {

	private static final FlinkException TEST_EXCEPTION = new FlinkException("Test exception");

	private static final WorkerResourceSpec WORKER_RESOURCE_SPEC = new WorkerResourceSpec.Builder()
		.setCpuCores(100.0)
		.setTaskHeapMemoryMB(10000)
		.setTaskOffHeapMemoryMB(10000)
		.setNetworkMemoryMB(10000)
		.setManagedMemoryMB(10000)
		.build();

	private static final int MIN_WORKER_NUM = 2;
	private static final int MAX_WORKER_NUM = 3;

	private static final ResourceProfile WORKER_RESOURCE_PROFILE = ResourceProfile.newBuilder()
		.setCpuCores(WORKER_RESOURCE_SPEC.getCpuCores())
		.setTaskHeapMemory(WORKER_RESOURCE_SPEC.getTaskHeapSize())
		.setTaskOffHeapMemory(WORKER_RESOURCE_SPEC.getTaskOffHeapSize())
		.setManagedMemory(WORKER_RESOURCE_SPEC.getManagedMemSize())
		.setNetworkMemory(WORKER_RESOURCE_SPEC.getNetworkMemSize())
		.build();

	/**
	 * Tests that we can register task manager.
	 */
	@Test
	public void testTaskManagerRegistration() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotReport slotReport = new SlotReport();
		try (NoSlotWorkerManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertEquals(WORKER_RESOURCE_PROFILE, slotManager.getRegisteredResource());
		}
	}

	/**
	 * Tests that un-registration of task managers will free.
	 */
	@Test
	public void testTaskManagerUnregistration() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotReport slotReport = new SlotReport();

		try (NoSlotWorkerManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			assertEquals(Collections.singletonMap(WORKER_RESOURCE_SPEC, MIN_WORKER_NUM), slotManager.getRequiredResources());

			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertEquals(WORKER_RESOURCE_PROFILE, slotManager.getRegisteredResource());
			assertEquals(Collections.singletonMap(WORKER_RESOURCE_SPEC, MIN_WORKER_NUM - 1), slotManager.getRequiredResources());

			slotManager.unregisterTaskManager(taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

			assertEquals(ResourceProfile.ZERO, slotManager.getRegisteredResource());
			assertEquals(Collections.singletonMap(WORKER_RESOURCE_SPEC, MIN_WORKER_NUM), slotManager.getRequiredResources());
		}
	}

	@Test
	public void testDuplicateTaskManagerUnregistration() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		final ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

		final ResourceID resourceId = ResourceID.generate();
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(resourceId, taskExecutorGateway);

		final SlotReport slotReport = new SlotReport();

		try (NoSlotWorkerManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);
			slotManager.unregisterTaskManager(taskManagerConnection.getInstanceID(), TEST_EXCEPTION);

			assertFalse("Should ignore this duplicate unregistration request",
				slotManager.unregisterTaskManager(taskManagerConnection.getInstanceID(), TEST_EXCEPTION));
		}
	}

	@Test
	public void testMaintainMinContainNum() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		TaskExecutorConnection[] taskExecutorConnections = new TaskExecutorConnection[MIN_WORKER_NUM];
		for (int i = 0; i < MIN_WORKER_NUM; ++i) {
			ResourceID resourceID = ResourceID.generate();
			TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
			taskExecutorConnections[i] = new TaskExecutorConnection(resourceID, taskExecutorGateway);
		}

		final AtomicInteger taskManagerCounter = new AtomicInteger(0);
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceConsumer(ignored -> {
				taskManagerCounter.incrementAndGet();
			}).setAllocateResourceConsumer((ignoredA, num) -> {
				for (int i = 0; i < num; i++) {
					taskManagerCounter.incrementAndGet();
				}
			}).setReleaseResourceConsumer((instanceId, ignored) -> {
				taskManagerCounter.decrementAndGet();
			}).build();

		final SlotReport initialSlotReport = new SlotReport();

		try (NoSlotWorkerManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions)) {
			for (int i = 0; i < MIN_WORKER_NUM; ++i) {
				slotManager.registerTaskManager(taskExecutorConnections[i], initialSlotReport);
			}

			assertEquals("TM number before release", MIN_WORKER_NUM, taskManagerCounter.get());
			slotManager.unregisterTaskManager(taskExecutorConnections[0].getInstanceID(), TEST_EXCEPTION);

			assertEquals("TM number after unregisterTaskManager", MIN_WORKER_NUM, taskManagerCounter.get());
		}
	}

	@Test
	public void testMaintainMaxContainNum() throws Exception {
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
		int testMaxWorkers = MAX_WORKER_NUM + 1;

		TaskExecutorConnection[] taskExecutorConnections = new TaskExecutorConnection[testMaxWorkers];
		for (int i = 0; i < testMaxWorkers; ++i) {
			ResourceID resourceID = ResourceID.generate();
			TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
			taskExecutorConnections[i] = new TaskExecutorConnection(resourceID, taskExecutorGateway);
		}

		final AtomicInteger taskManagerCounter = new AtomicInteger(0);
		ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
			.setAllocateResourceConsumer(ignored -> {
				taskManagerCounter.incrementAndGet();
			}).setAllocateResourceConsumer((ignoredA, num) -> {
				for (int i = 0; i < num; i++) {
					taskManagerCounter.incrementAndGet();
				}
			}).setReleaseResourceConsumer((instanceId, ignored) -> {
				taskManagerCounter.decrementAndGet();
			}).build();

		final SlotReport initialSlotReport = new SlotReport();

		try (NoSlotWorkerManagerImpl slotManager = createSlotManager(resourceManagerId, resourceManagerActions, testMaxWorkers)) {
			for (int i = 0; i < MIN_WORKER_NUM; ++i) {
				slotManager.registerTaskManager(taskExecutorConnections[i], initialSlotReport);
			}

			slotManager.requestNewTaskManagers(testMaxWorkers - MIN_WORKER_NUM);
			for (int i = MIN_WORKER_NUM; i < testMaxWorkers; ++i) {
				slotManager.registerTaskManager(taskExecutorConnections[i], initialSlotReport);
			}

			assertEquals("TM number after request", testMaxWorkers, taskManagerCounter.get());
			slotManager.unregisterTaskManager(taskExecutorConnections[0].getInstanceID(), TEST_EXCEPTION);

			assertEquals("TM number after unregisterTaskManager", testMaxWorkers - 1, taskManagerCounter.get());

			slotManager.requestNewTaskManagers(testMaxWorkers);
			assertEquals("TM number keep maxWorkerNum after request", testMaxWorkers, taskManagerCounter.get());
		}
	}

	private NoSlotWorkerManagerImpl createSlotManager(
			ResourceManagerId resourceManagerId,
			ResourceActions resourceManagerActions,
			int maxWorkerNum) {
		NoSlotWorkerManagerImpl slotManager = createSlotManagerBuilder()
			.setMaxWorkerNum(maxWorkerNum)
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions);
		return slotManager;
	}

	private NoSlotWorkerManagerImpl createSlotManager(ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
		NoSlotWorkerManagerImpl slotManager = createSlotManagerBuilder()
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions);
		return slotManager;
	}

	private NoSlotWorkerManagerBuilder createSlotManagerBuilder() {
		return NoSlotWorkerManagerBuilder.newBuilder()
			.setDefaultWorkerResourceSpec(WORKER_RESOURCE_SPEC)
			.setMinWorkerNum(MIN_WORKER_NUM)
			.setMaxWorkerNum(MAX_WORKER_NUM);
	}
}
