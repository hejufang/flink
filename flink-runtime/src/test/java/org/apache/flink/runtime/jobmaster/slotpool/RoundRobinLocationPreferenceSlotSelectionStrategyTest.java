/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.PreviousAllocationSlotSelectionStrategyTest;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Tests for {@link RoundRobinLocationPreferenceSlotSelectionStrategy}.
 * This will run all parent tests.
 */
@RunWith(Parameterized.class)
public class RoundRobinLocationPreferenceSlotSelectionStrategyTest extends PreviousAllocationSlotSelectionStrategyTest {
	private SlotPool slotPool;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	private final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

	@Parameterized.Parameter
	public boolean batchRequestSlotsEnable;

	@Parameterized.Parameters(name = "batchRequestSlotsEnable = {0}")
	public static Collection<Boolean> batchRequestSlotsEnable() {
		return Arrays.asList(true, false);
	}

	public RoundRobinLocationPreferenceSlotSelectionStrategyTest() {
		super(LocationPreferenceSlotSelectionStrategy.createRoundRobin());
	}

	@Before
	public void setUp() throws Exception {
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
		setupSlotPool();
	}

	private void setupSlotPool() throws Exception {
		final String jobManagerAddress = "foobar";
		slotPool = new TestingRoundRobinSlotPoolImpl(new JobID(), batchRequestSlotsEnable);
		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);

		for (SlotSelectionStrategy.SlotInfoAndResources slotInfoAndResources : candidates) {
			SlotInfo slotInfo = slotInfoAndResources.getSlotInfo();
			slotPool.registerTaskManager(slotInfo.getTaskManagerLocation().getResourceID());
			slotPool.offerSlots(
				slotInfo.getTaskManagerLocation(),
				taskManagerGateway,
				Collections.singletonList(new SlotOffer(slotInfo.getAllocationId(), slotInfo.getPhysicalSlotNumber(), slotInfo.getResourceProfile())));
		}
	}

	@Override
	protected Optional<SlotSelectionStrategy.SlotInfoAndLocality> runMatching(SlotProfile slotProfile) {
		return selectionStrategy.selectBestSlotForProfile(slotProfile, slotPool);
	}
}
