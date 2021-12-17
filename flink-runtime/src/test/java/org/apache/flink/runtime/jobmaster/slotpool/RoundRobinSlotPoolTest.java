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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link RoundRobinSlotPoolImpl}.
 */
public class RoundRobinSlotPoolTest extends TestLogger {
	private final Time timeout = Time.seconds(10L);

	private JobID jobId;

	private TaskManagerLocation taskManagerLocation;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	private final ComponentMainThreadExecutor mainThreadExecutor =
			ComponentMainThreadExecutorServiceAdapter.forMainThread();

	Predicate<AllocatedSlot> alwaysTure = slot -> true;

	@Before
	public void setUp() throws Exception {
		this.jobId = new JobID();

		taskManagerLocation = new LocalTaskManagerLocation();
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	@Nonnull
	private TestingRoundRobinSlotPoolImpl createRoundRobinSlotPoolImpl() {
		return new TestingRoundRobinSlotPoolImpl(jobId, false);
	}

	@Nonnull
	private TestingRoundRobinSlotPoolImpl createRoundRobinSlotPoolImpl(ManualClock clock) {
		return new TestingRoundRobinSlotPoolImpl(
				jobId,
				clock,
				TestingUtils.infiniteTime(),
				timeout,
				TestingUtils.infiniteTime(),
				false);
	}

	private static void setupSlotPool(
			SlotPoolImpl slotPool,
			ResourceManagerGateway resourceManagerGateway,
			ComponentMainThreadExecutor mainThreadExecutable) throws Exception {
		final String jobManagerAddress = "foobar";

		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutable);

		slotPool.connectToResourceManager(resourceManagerGateway);
	}

	// ------------------------------------
	// Test MinResource slot allocation
	// ------------------------------------

	@Test
	public void testRequiredResource() throws Exception {
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl();
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());

		final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(10);
		resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		final List<SlotOffer> slotOffers = new ArrayList<>(10);

		for (int i = 0; i < 10; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());
	}

	@Test
	public void testRequiredResourceFulFilledByUnknownSlots() throws Exception {
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl();
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());

		final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(10);
		resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		final List<SlotOffer> slotOffers = new ArrayList<>(10);

		for (int i = 0; i < 9; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}
		slotOffers.add(new SlotOffer(new AllocationID(), 9, ResourceProfile.ANY));

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());
		assertEquals(0, slotPool.getPendingRequests().size());

		slotOffers.clear();
		slotOffers.add(new SlotOffer(new AllocationID(), 9, ResourceProfile.ANY));
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);
		assertEquals(11, slotPool.getAvailableSlotsInformation().size());
		assertEquals(0, slotPool.getPendingRequests().size());
		assertEquals(0, slotPool.getAllocatedSlots().size());
	}

	@Test
	public void testRequestRequiredResourceWhenFailAllocation() throws Exception {
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl();
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());

		final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(10);
		resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		// pending request failed.
		slotPool.failAllocation(allocationIds.take(), new Exception("Expected."));

		assertEquals(10, allocationIds.size());

		final List<SlotOffer> slotOffers = new ArrayList<>(10);

		AllocationID allocationID = allocationIds.peek();

		for (int i = 0; i < 3; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);
		assertEquals(3, slotPool.getAvailableSlotsInformation().size());

		// allocated slot failed.
		slotPool.failAllocation(allocationID, new Exception("Expected."));

		assertEquals(8, allocationIds.size());
		assertEquals(2, slotPool.getAvailableSlotsInformation().size());

		slotOffers.clear();
		for (int i = 0; i < 8; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}

		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(0, allocationIds.size());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());

		// task manager released.
		slotPool.releaseTaskManager(taskManagerLocation.getResourceID(), new Exception("Expected."));
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, allocationIds.size());
		assertEquals(0, slotPool.getAvailableSlotsInformation().size());
	}

	@Test
	public void testRequiredResourceSlotPoolIdleCheck() throws Exception {
		ManualClock clock = new ManualClock();
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl(clock);
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());

		final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(10);
		resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		final List<SlotOffer> slotOffers = new ArrayList<>(10);

		for (int i = 0; i < 10; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());

		slotPool.offerSlot(taskManagerLocation, taskManagerGateway, new SlotOffer(new AllocationID(), 10, ResourceProfile.ANY));
		assertEquals(11, slotPool.getAvailableSlotsInformation().size());

		clock.advanceTime(11, TimeUnit.SECONDS);
		slotPool.triggerCheckIdleSlot();

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());
	}

	@Test
	public void testRequireResourceSlotPoolSuspendAndClose() throws Exception {
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl();
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(10);
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));
		resourceManagerGateway.setCancelSlotConsumer(allocationIds::remove);
		final ArrayBlockingQueue<AllocationID> freeAllocations = new ArrayBlockingQueue<>(10);
		taskManagerGateway.setFreeSlotFunction((allocationId, t) -> {
			freeAllocations.offer(allocationId);
			return CompletableFuture.completedFuture(Acknowledge.get());
		});
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		// suspend slot pool will cancel all requests.
		slotPool.suspend();
		assertEquals(0, allocationIds.size());

		// start slot pool will request 10 slot for required resources.
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());

		final List<SlotOffer> slotOffers = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
		}

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

		assertTrue(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());

		// suspend slot pool will not call taskmanager.free
		slotPool.suspend();
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(0, slotPool.getAvailableSlotsInformation().size());
		assertEquals(0, freeAllocations.size());

		// start slot pool again, and old taskmanager slots will offer to this slotpool
		setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
		assertEquals(10, allocationIds.size());
		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);
		assertEquals(10, slotPool.getAvailableSlotsInformation().size());
		assertEquals(0, allocationIds.size());

		// close slot pool will free taskmanager slots.
		slotPool.close();
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());
		assertEquals(0, slotPool.getAvailableSlotsInformation().size());
		assertEquals(10, freeAllocations.size());
	}

	@Test
	public void testSlotPoolMinResourceFutureTimeout() throws Exception {
		TestingRoundRobinSlotPoolImpl slotPool = createRoundRobinSlotPoolImpl();
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		slotPool.setRequiredResourceNumber(requiredResource);
		assertFalse(slotPool.getRequiredResourceSatisfiedFuture().isDone());

		final ScheduledExecutorService singleThreadExecutor = Executors.newSingleThreadScheduledExecutor();
		final ComponentMainThreadExecutor componentMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadExecutor);
		componentMainThreadExecutor.execute(
				() -> {
					try {
						setupSlotPool(slotPool, resourceManagerGateway, componentMainThreadExecutor);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
		);

		final CompletableFuture<Acknowledge> minResourceFuture = CompletableFuture
				.supplyAsync(() -> slotPool.getRequiredResourceSatisfiedFutureWithTimeout(Time.milliseconds(1000)), componentMainThreadExecutor)
				.thenCompose(Function.identity());
		try {
			minResourceFuture.get();
			fail("Expected that the future completes with a TimeoutException.");
		} catch (Exception e) {
			assertThat(ExceptionUtils.stripExecutionException(e), instanceOf(TimeoutException.class));
		}
	}

	// ------------------------------------
	// Test RoundRobin slot selection
	// ------------------------------------

	@Test
	public void testRoundRobinAvailableSlots() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s6 = createAllocatedSlot(tm3, 0);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);

		List<AllocatedSlot> allocatedSlots = new ArrayList<>();
		while (true) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s1, s3, s6, s2, s4, s5));
		assertTrue(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.isEmpty());
	}

	@Test
	public void testRoundRobinAvailableSlotsWithPredict() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s6 = createAllocatedSlot(tm3, 0);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);

		Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, s -> s == s5);
		assertTrue(optionalAllocatedSlot.isPresent());
	}

	@Test
	public void testRoundRobinAvailableSlotsByTaskManagerAfterRound() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s6 = createAllocatedSlot(tm3, 0);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);

		Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, s -> s == s5);
		assertTrue(optionalAllocatedSlot.isPresent());
		roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());

		optionalAllocatedSlot = roundRobinAvailableSlots.getByTaskManagerLocation(ResourceProfile.ANY, tm3, s -> s == s6);
		assertTrue(optionalAllocatedSlot.isPresent());
		roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
		assertEquals(4, roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.size());
	}

	@Test
	public void testRoundRobinAvailableSlotsWithLocationPrefer() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s6 = createAllocatedSlot(tm3, 0);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);

		List<AllocatedSlot> allocatedSlots = new ArrayList<>();
		allocatedSlots.add(roundRobinAvailableSlots.getByAllocationId(ResourceProfile.ANY, s4.getAllocationId()).get());
		roundRobinAvailableSlots.tryRemove(allocatedSlots.get(0).getAllocationId());
		allocatedSlots.add(roundRobinAvailableSlots.getByHost(ResourceProfile.ANY, tm1.getFQDNHostname(), alwaysTure).get());
		roundRobinAvailableSlots.tryRemove(allocatedSlots.get(1).getAllocationId());
		allocatedSlots.add(roundRobinAvailableSlots.getByTaskManagerLocation(ResourceProfile.ANY, tm2, alwaysTure).get());
		roundRobinAvailableSlots.tryRemove(allocatedSlots.get(2).getAllocationId());
		while (true) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s4, s1, s3, s6, s2, s5));
		assertTrue(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.isEmpty());
	}

	@Test
	public void testRemoveSlot() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm1, 2);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s6 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s7 = createAllocatedSlot(tm2, 3);
		AllocatedSlot s8 = createAllocatedSlot(tm3, 0);
		AllocatedSlot s9 = createAllocatedSlot(tm3, 1);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);
		roundRobinAvailableSlots.add(s7, ts);
		roundRobinAvailableSlots.add(s8, ts);
		roundRobinAvailableSlots.add(s9, ts);

		List<AllocatedSlot> allocatedSlots = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s1, s4, s8));

		roundRobinAvailableSlots.tryRemove(s5.getAllocationId());
		for (int i = 0; i < 2; i++) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s1, s4, s8, s2, s9));
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.values(), containsInAnyOrder(s3, s6, s7));
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).taskManagersByHost.keySet(), containsInAnyOrder(tm1.getFQDNHostname(), tm2.getFQDNHostname()));
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slotsByTaskManager.keySet(), containsInAnyOrder(tm1, tm2));

		roundRobinAvailableSlots.add(s5, System.currentTimeMillis());

		while (true) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}

		assertThat(allocatedSlots, contains(s1, s4, s8, s2, s9, s3, s6, s7, s5));
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.values(), empty());
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).taskManagersByHost.keySet(), empty());
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slotsByTaskManager.keySet(), empty());
	}

	@Test
	public void testRemoveTaskManager() throws Exception {
		RoundRobinSlotPoolImpl.RoundRobinAvailableSlots roundRobinAvailableSlots = new RoundRobinSlotPoolImpl.RoundRobinAvailableSlots();
		InetAddress host1 = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
		InetAddress host2 = InetAddress.getByAddress(new byte[]{127, 0, 0, 2});
		TaskManagerLocation tm1 = new TaskManagerLocation(new ResourceID("1"), host1, 1);
		TaskManagerLocation tm2 = new TaskManagerLocation(new ResourceID("2"), host2, 1);
		TaskManagerLocation tm3 = new TaskManagerLocation(new ResourceID("3"), host1, 2);
		AllocatedSlot s1 = createAllocatedSlot(tm1, 0);
		AllocatedSlot s2 = createAllocatedSlot(tm1, 1);
		AllocatedSlot s3 = createAllocatedSlot(tm1, 2);
		AllocatedSlot s4 = createAllocatedSlot(tm2, 0);
		AllocatedSlot s5 = createAllocatedSlot(tm2, 1);
		AllocatedSlot s6 = createAllocatedSlot(tm2, 2);
		AllocatedSlot s7 = createAllocatedSlot(tm2, 3);
		AllocatedSlot s8 = createAllocatedSlot(tm3, 0);
		AllocatedSlot s9 = createAllocatedSlot(tm3, 1);
		long ts = System.currentTimeMillis();
		roundRobinAvailableSlots.add(s1, ts);
		roundRobinAvailableSlots.add(s2, ts);
		roundRobinAvailableSlots.add(s3, ts);
		roundRobinAvailableSlots.add(s4, ts);
		roundRobinAvailableSlots.add(s5, ts);
		roundRobinAvailableSlots.add(s6, ts);
		roundRobinAvailableSlots.add(s7, ts);
		roundRobinAvailableSlots.add(s8, ts);
		roundRobinAvailableSlots.add(s9, ts);

		List<AllocatedSlot> allocatedSlots = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s1, s4, s8));

		roundRobinAvailableSlots.removeAllForTaskManager(tm3.getResourceID());
		while (true) {
			Optional<AllocatedSlot> optionalAllocatedSlot = roundRobinAvailableSlots.getNextAvailableSlot(ResourceProfile.ANY, alwaysTure);
			if (!optionalAllocatedSlot.isPresent()) {
				break;
			} else {
				allocatedSlots.add(optionalAllocatedSlot.get());
				roundRobinAvailableSlots.tryRemove(optionalAllocatedSlot.get().getAllocationId());
			}
		}
		assertThat(allocatedSlots, contains(s1, s4, s8, s2, s5, s3, s6, s7));
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slots.values(), empty());
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).taskManagersByHost.keySet(), empty());
		assertThat(roundRobinAvailableSlots.allSlots.get(ResourceProfile.ANY).slotsByTaskManager.keySet(), empty());
	}

	private static AllocatedSlot createAllocatedSlot(TaskManagerLocation taskManagerLocation, int slotNumber) {
		return new AllocatedSlot(
			new AllocationID(),
			taskManagerLocation,
			slotNumber,
			ResourceProfile.ANY,
			new SimpleAckingTaskManagerGateway());
	}
}
