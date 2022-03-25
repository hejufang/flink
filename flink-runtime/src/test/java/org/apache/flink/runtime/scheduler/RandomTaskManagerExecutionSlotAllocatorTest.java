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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.VirtualTaskManagerSlotPool;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link RandomTaskManagerExecutionSlotAllocator}.
 */
public class RandomTaskManagerExecutionSlotAllocatorTest {

	@Test
	public void testGetShuffledTaskManager() {
		Set<ResourceID> resources = new HashSet<>();
		try {
			RandomTaskManagerExecutionSlotAllocator.getShuffledTaskManager(resources, 1);
		} catch (IllegalArgumentException e) {
			assertEquals("Excepted task manager number need less than task managers size.", e.getMessage());
		}

		for (int i = 0; i < 10; i++) {
			resources.add(ResourceID.generate());
		}

		List<ResourceID> shuffledResource = RandomTaskManagerExecutionSlotAllocator.getShuffledTaskManager(resources, 5);
		assertEquals(5, shuffledResource.size());
		List<ResourceID> shuffledResource2 = RandomTaskManagerExecutionSlotAllocator.getShuffledTaskManager(resources, 5);
		assertEquals(5, shuffledResource2.size());
		assertNotEquals(shuffledResource, shuffledResource2);
	}

	@Test
	public void testAllocateSlotsForWithNoTaskManager() throws Exception {
		JobID jobID = new JobID();
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(jobID, true, Collections.emptyMap(), 0);
		RandomTaskManagerExecutionSlotAllocator randomTaskManagerExecutionSlotAllocator = new RandomTaskManagerExecutionSlotAllocator(slotPool, 0, 0);

		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("can not use"));

		List<SlotExecutionVertexAssignment> result;
		List<ExecutionVertexSchedulingRequirements> requests = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			requests.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).build());
		}

		result = randomTaskManagerExecutionSlotAllocator.allocateSlotsFor(requests);
		assertEquals(10, result.size());
		result.stream().map(SlotExecutionVertexAssignment::getLogicalSlotFuture).forEach(
				logicalSlotFuture -> assertTrue(logicalSlotFuture.isCompletedExceptionally()));
	}

	@Test
	public void testAllocateSlotsFor() throws Exception {
		JobID jobID = new JobID();
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(jobID, true, Collections.emptyMap(), 0);
		RandomTaskManagerExecutionSlotAllocator randomTaskManagerExecutionSlotAllocator = new RandomTaskManagerExecutionSlotAllocator(slotPool, 0, 0);

		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().setAddress("127.0.0.1").createTestingTaskExecutorGateway();

		for (int i = 0; i < 10; i++) {
			ResourceID resourceID = ResourceID.generate();
			TaskManagerLocation taskManagerLocation = new TaskManagerLocation(resourceID, InetAddress.getLoopbackAddress(), 1);
			slotPool.registerTaskManager(ResourceID.generate(), new ResolvedTaskManagerTopology(taskExecutorGateway, taskManagerLocation));
		}
		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("can not use"));

		List<SlotExecutionVertexAssignment> result;
		List<ExecutionVertexSchedulingRequirements> requests = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			requests.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).build());
		}

		result = randomTaskManagerExecutionSlotAllocator.allocateSlotsFor(requests);
		assertEquals(10, result.size());
		List<LogicalSlot> slots = checkAllFutureDoneAndGetResult(result.stream().map(SlotExecutionVertexAssignment::getLogicalSlotFuture).collect(Collectors.toList()));
		Map<ResourceID, Long> resourceCount = slots.stream().collect(
				Collectors.groupingBy(
						s -> s.getTaskManagerLocation().getResourceID(),
						Collectors.counting()
				));
		assertEquals(10, resourceCount.size());
		assertThat(resourceCount.values(), everyItem(equalTo(1L)));

		requests.clear();
		for (int i = 0; i < 5; i++) {
			requests.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).build());
		}
		result = randomTaskManagerExecutionSlotAllocator.allocateSlotsFor(requests);
		assertEquals(5, result.size());
		slots = checkAllFutureDoneAndGetResult(result.stream().map(SlotExecutionVertexAssignment::getLogicalSlotFuture).collect(Collectors.toList()));
		resourceCount = slots.stream().collect(
				Collectors.groupingBy(
						s -> s.getTaskManagerLocation().getResourceID(),
						Collectors.counting()
				));

		assertEquals(5, resourceCount.size());
		assertThat(resourceCount.values(), everyItem(equalTo(1L)));

		requests.clear();
		for (int i = 0; i < 25; i++) {
			requests.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).build());
		}
		result = randomTaskManagerExecutionSlotAllocator.allocateSlotsFor(requests);
		assertEquals(25, result.size());
		slots = checkAllFutureDoneAndGetResult(result.stream().map(SlotExecutionVertexAssignment::getLogicalSlotFuture).collect(Collectors.toList()));
		resourceCount = slots.stream().collect(
				Collectors.groupingBy(
						s -> s.getTaskManagerLocation().getResourceID(),
						Collectors.counting()
				));

		assertEquals(10, resourceCount.size());
		assertThat(resourceCount.values(), everyItem(lessThanOrEqualTo(3L)));
	}

	@Test
	public void testAllocatedSlotReleased() throws Exception {
		JobID jobID = new JobID();
		VirtualTaskManagerSlotPool slotPool = new VirtualTaskManagerSlotPool(jobID, true, Collections.emptyMap(), 0);
		RandomTaskManagerExecutionSlotAllocator randomTaskManagerExecutionSlotAllocator = new RandomTaskManagerExecutionSlotAllocator(slotPool, 0, 0);

		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().setAddress("127.0.0.1").createTestingTaskExecutorGateway();

		for (int i = 0; i < 10; i++) {
			ResourceID resourceID = ResourceID.generate();
			TaskManagerLocation taskManagerLocation = new TaskManagerLocation(resourceID, InetAddress.getLoopbackAddress(), 1);
			slotPool.registerTaskManager(resourceID, new ResolvedTaskManagerTopology(taskExecutorGateway, taskManagerLocation));
		}
		slotPool.start(JobMasterId.generate(), "foo_bar", new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor("can not use"));

		List<SlotExecutionVertexAssignment> result;
		List<ExecutionVertexSchedulingRequirements> requests = new ArrayList<>();
		for (int i = 0; i < 5; i++) {
			requests.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).build());
		}
		result = randomTaskManagerExecutionSlotAllocator.allocateSlotsFor(requests);
		assertEquals(5, result.size());
		List<LogicalSlot> slots = checkAllFutureDoneAndGetResult(result.stream().map(SlotExecutionVertexAssignment::getLogicalSlotFuture).collect(Collectors.toList()));
		for (LogicalSlot slot : slots) {
			slot.releaseSlot(new Exception("excepted."));
		}
		assertTrue(slotPool.getUsedTaskManagers().isEmpty());
	}

	@Test
	public void testComputeJobWorkerCount() {
		assertEquals(
				10,
				RandomTaskManagerExecutionSlotAllocator.computeJobWorkerCount(128, 129, 30, 10, 100));
		assertEquals(
				10,
				RandomTaskManagerExecutionSlotAllocator.computeJobWorkerCount(128, 257, 30, 10, 100));
		assertEquals(
				25,
				RandomTaskManagerExecutionSlotAllocator.computeJobWorkerCount(128, 768, 30, 10, 100));
		assertEquals(
				100,
				RandomTaskManagerExecutionSlotAllocator.computeJobWorkerCount(128, 10000, 30, 10, 100));
	}

	private static <C> List<C> checkAllFutureDoneAndGetResult(List<CompletableFuture<C>> futures) throws ExecutionException, InterruptedException {
		List<C> results = new ArrayList<>();
		for (CompletableFuture<C> future : futures) {
			assertTrue(future.isDone());
			results.add(future.get());
		}
		return results;
	}
}
