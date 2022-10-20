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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PhysicalSlotProviderImpl}.
 */
@RunWith(Parameterized.class)
public class PhysicalSlotProviderImplTest {
	private static ScheduledExecutorService singleThreadScheduledExecutorService;

	private static ComponentMainThreadExecutor mainThreadExecutor;

	private TestingSlotPoolImpl slotPool;

	private PhysicalSlotProvider physicalSlotProvider;

	private Time timeout = Time.seconds(10);

	@Parameterized.Parameter
	public boolean batchRequestEnable;

	@Parameterized.Parameters(name = "batchRequestEnable = {0}")
	public static Collection<Boolean> parameters() {
		return Arrays.asList(true, false);
	}

	@BeforeClass
	public static void setupClass() {
		singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadScheduledExecutorService);
	}

	@AfterClass
	public static void teardownClass() {
		if (singleThreadScheduledExecutorService != null) {
			singleThreadScheduledExecutorService.shutdownNow();
		}
	}

	@Before
	public void setup() throws Exception {
		slotPool = new SlotPoolBuilder(mainThreadExecutor, batchRequestEnable).build();
		physicalSlotProvider = new PhysicalSlotProviderImpl(LocationPreferenceSlotSelectionStrategy.createRoundRobin(), slotPool, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
	}

	@After
	public void teardown() {
		CompletableFuture.runAsync(() -> slotPool.close(), mainThreadExecutor).join();
	}

	@Test
	public void testBulkSlotAllocationFulfilledWithAvailableSlots() throws InterruptedException, ExecutionException {
		PhysicalSlotRequest request = createPhysicalSlotRequest();
		addSlotToSlotPool();
		CompletableFuture<PhysicalSlotRequest.Result> slotFuture = allocateSlot(request);
		PhysicalSlotRequest.Result result = slotFuture.get();
		assertThat(result.getSlotRequestId(), is(request.getSlotRequestId()));
	}

	@Test
	public void testBulkSlotAllocationFulfilledWithNewSlots() throws ExecutionException, InterruptedException {
		final CompletableFuture<PhysicalSlotRequest.Result> slotFuture = allocateSlot(createPhysicalSlotRequest());
		assertThat(slotFuture.isDone(), is(false));
		addSlotToSlotPool();
		slotFuture.get();
	}

	@Test
	public void testBulkSlotAllocationCanceled() {
		PhysicalSlotRequest request = createPhysicalSlotRequest();
		CompletableFuture<Acknowledge> future = new CompletableFuture<>();
		slotPool.setRequiredResourceSatisfiedFuture(future);
		CompletableFuture<PhysicalSlotRequest.Result> slotFuture = allocateSlot(request);
		CompletableFuture.runAsync(() -> physicalSlotProvider.cancelSlotRequest(request.getSlotRequestId(), new Exception("excepted")), mainThreadExecutor).join();
		future.complete(Acknowledge.get());
		assertTrue(slotFuture.isCompletedExceptionally());
	}

	@Test
	public void testSetRequiredResources() throws Exception {
		SlotPoolBuilder slotPoolBuilder = new SlotPoolBuilder(mainThreadExecutor);
		TestingRoundRobinSlotPoolImpl slotPool = slotPoolBuilder.buildRoundRobinWithoutSetup();

		PhysicalSlotProvider physicalSlotProvider = new PhysicalSlotProviderImpl(LocationPreferenceSlotSelectionStrategy.createRoundRobin(), slotPool, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		physicalSlotProvider.setRequiredResources(requiredResource);

		slotPoolBuilder.setupSlotPool(slotPool);

		assertEquals(10, slotPool.getPendingRequests().size());
	}

	@Test
	public void testCancelSlotRequest() throws Exception {
		SlotPoolBuilder slotPoolBuilder = new SlotPoolBuilder(mainThreadExecutor);
		TestingRoundRobinSlotPoolImpl slotPool = slotPoolBuilder.buildRoundRobinWithoutSetup();

		PhysicalSlotProvider physicalSlotProvider = new PhysicalSlotProviderImpl(LocationPreferenceSlotSelectionStrategy.createRoundRobin(), slotPool, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
		Map<ResourceProfile, Integer> requiredResource = new HashMap<>();
		requiredResource.put(ResourceProfile.UNKNOWN, 10);
		physicalSlotProvider.setRequiredResources(requiredResource);

		slotPoolBuilder.setupSlotPool(slotPool);

		assertEquals(10, slotPool.getPendingRequests().size());

		// stop keep min resource.
		slotPool.markWillBeClosed();
		SlotRequestId pendingRequestId = slotPool.getPendingRequests().keySetA().stream().findFirst().get();
		CompletableFuture.runAsync(() -> physicalSlotProvider.cancelSlotRequest(pendingRequestId, new Exception("excepted")), mainThreadExecutor).join();
		assertEquals(9, slotPool.getPendingRequests().size());
	}

	private CompletableFuture<PhysicalSlotRequest.Result> allocateSlot(PhysicalSlotRequest request) {
		return CompletableFuture
			.supplyAsync(
				() -> physicalSlotProvider.allocatePhysicalSlot(request, timeout),
				mainThreadExecutor)
			.thenCompose(Function.identity());
	}

	private void addSlotToSlotPool() {
		SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(ResourceProfile.ANY));
	}

	private static PhysicalSlotRequest createPhysicalSlotRequest() {
		return new PhysicalSlotRequest(
			new SlotRequestId(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			true);
	}
}
