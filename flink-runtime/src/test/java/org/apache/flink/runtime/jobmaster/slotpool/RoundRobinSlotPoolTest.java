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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link RoundRobinSlotPoolImpl}.
 */
public class RoundRobinSlotPoolTest extends TestLogger {

	Predicate<AllocatedSlot> alwaysTure = slot -> true;

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
