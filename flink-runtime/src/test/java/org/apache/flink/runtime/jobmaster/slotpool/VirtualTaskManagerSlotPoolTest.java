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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.strategy.JobSpreadTaskManagerStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.strategy.RandomTaskManagerStrategy;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link VirtualTaskManagerSlotPool}.
 */
public class VirtualTaskManagerSlotPoolTest {

	@Test
	public void testAllocateSlot() throws Exception {
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(new JobID(), true, Collections.emptyMap(), 0);
		AllocatedSlot slot;
		AllocationID allocationID = new AllocationID();
		ResourceID resourceID = ResourceID.generate();
		try {
			slot = slotPool.allocatedSlot(allocationID, resourceID);
		} catch (Throwable t) {
			checkExceptionAndMsg(t, NullPointerException.class, "JobMasterId is null, that means SlotPool worked before started.");
		}
		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("Should not use"));
		try {
			slot = slotPool.allocatedSlot(allocationID, resourceID);
		} catch (Throwable t) {
			checkExceptionAndMsg(t, NullPointerException.class, String.format("Allocate %s from %s failed, task manager not found.", allocationID, resourceID));
		}

		slotPool.registerTaskManager(resourceID, new ResolvedTaskManagerTopology(new TestingTaskExecutorGatewayBuilder().setAddress("127.0.0.1").createTestingTaskExecutorGateway(), new LocalTaskManagerLocation()));
		slot = slotPool.allocatedSlot(allocationID, resourceID);
		assertNotNull(slot);
		Set<ResourceID> usedResources = slotPool.getUsedTaskManagers();
		assertEquals(1, usedResources.size());
		assertTrue(usedResources.contains(resourceID));

		assertEquals(1, slotPool.getTotalTaskManagerCount());
	}

	@Test
	public void testAllocatedSlotReleased() throws Exception {
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(new JobID(), true, Collections.emptyMap(), 0);
		AllocatedSlot slot;
		AllocationID allocationID = new AllocationID();
		ResourceID resourceID = ResourceID.generate();
		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().setAddress("127.0.0.1").createTestingTaskExecutorGateway();
		TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("Should not use"));

		slotPool.registerTaskManager(resourceID, new ResolvedTaskManagerTopology(taskExecutorGateway, taskManagerLocation));
		slot = slotPool.allocatedSlot(allocationID, resourceID);
		assertNotNull(slot);
		CompletableFuture<Throwable> releaseFuture = new CompletableFuture<>();
		slot.tryAssignPayload(new TestPayload(releaseFuture));
		Exception exception = new Exception("expected.");
		slotPool.releaseTaskManager(resourceID, exception);
		assertTrue(releaseFuture.isDone());
		assertEquals(exception, releaseFuture.get());
	}

	@Test
	public void testRandomAllocateTaskManagersFromPool() {
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		for (int i = 0; i < 100; i ++) {
			taskManagers.put(ResourceID.generate(), new ResolvedTaskManagerTopology(null, null));
		}
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(new JobID(), true, taskManagers, 0);
		Set<ResourceID> allocateResourceIds1 = slotPool.allocateTaskManagers(10);
		Set<ResourceID> allocateResourceIds2 = slotPool.allocateTaskManagers(10);
		assertNotEquals(allocateResourceIds1, allocateResourceIds2);

		Set<ResourceID> allResourceIds1 = slotPool.allocateTaskManagers(200);
		assertEquals(taskManagers.keySet(), allResourceIds1);

		Set<ResourceID> allResourceIds2 = slotPool.allocateTaskManagers(0);
		assertEquals(taskManagers.keySet(), allResourceIds2);
	}

	@Test
	public void testJobSpreadAllocateTaskManagersFromPool() throws Exception {
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		InetAddress local = InetAddress.getLocalHost();
		for (int i = 0; i < 100; i ++) {
			ResourceID resourceId = ResourceID.generate();
			taskManagers.put(resourceId, new ResolvedTaskManagerTopology(
				null,
				new TaskManagerLocation(resourceId, local, -1)));
		}
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(
				new JobID(),
				true,
				taskManagers,
				0,
				JobSpreadTaskManagerStrategy.getInstance());

		for (ResolvedTaskManagerTopology taskManagerTopology : taskManagers.values()) {
			assertEquals(0, taskManagerTopology.getRunningJobCount());
		}
		Set<ResourceID> allAllocatedResourceIds = new HashSet<>();
		for (int i = 0; i < 10; i++) {
			Set<ResourceID> resourceIds = slotPool.allocateTaskManagers(10);
			assertEquals(10, resourceIds.size());
			allAllocatedResourceIds.addAll(resourceIds);
		}
		assertEquals(allAllocatedResourceIds, taskManagers.keySet());
		for (ResolvedTaskManagerTopology taskManagerTopology : taskManagers.values()) {
			assertEquals(1, taskManagerTopology.getRunningJobCount());
		}

		slotPool.close();
		for (ResolvedTaskManagerTopology taskManagerTopology : taskManagers.values()) {
			assertEquals(0, taskManagerTopology.getRunningJobCount());
		}
	}

	@Test
	public void testReleaseSlotShareExceptionEnable() throws Exception {
		VirtualTaskManagerSlotPool slotPool;
		slotPool = new VirtualTaskManagerSlotPool(new JobID(), true, Collections.emptyMap(), 0, RandomTaskManagerStrategy.getInstance(), true);
		ResourceID resourceID = ResourceID.generate();
		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().setAddress("127.0.0.1").createTestingTaskExecutorGateway();
		TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("Should not use"));

		slotPool.registerTaskManager(resourceID, new ResolvedTaskManagerTopology(taskExecutorGateway, taskManagerLocation));

		Set<AllocationID> allocationIds = new HashSet<>();
		for (int i = 0; i < 20; ++i) {
			AllocationID allocationId = new AllocationID();
			if (allocationIds.contains(allocationId)) {
				continue;
			}
			allocationIds.add(allocationId);
			AllocatedSlot slot = slotPool.allocatedSlot(allocationId, resourceID);
			slot.tryAssignPayload(new TestReleaseSlotShareExceptionEnablePayload());
		}
		for (AllocationID allocationId: allocationIds) {
			slotPool.releaseAllocatedSlot(allocationId);
		}
	}

	private static void checkExceptionAndMsg(Throwable t, Class<?> exceptionClass, String msg) {
		assertEquals(exceptionClass, t.getClass());
		assertEquals(msg, t.getMessage());
	}

	private static class TestPayload implements PhysicalSlot.Payload {
		CompletableFuture<Throwable> releaseFuture;

		public TestPayload(CompletableFuture<Throwable> releaseFuture) {
			this.releaseFuture = releaseFuture;
		}

		@Override
		public void release(Throwable cause) {
			releaseFuture.complete(cause);
		}

		@Override
		public boolean willOccupySlotIndefinitely() {
			return false;
		}
	}

	private static class TestReleaseSlotShareExceptionEnablePayload implements PhysicalSlot.Payload {

		public TestReleaseSlotShareExceptionEnablePayload() {}

		@Override
		public void release(Throwable cause) {
			assertEquals("Slot is released.", cause.getMessage());
		}

		@Override
		public boolean willOccupySlotIndefinitely() {
			return false;
		}
	}

}
