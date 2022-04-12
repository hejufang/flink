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

package org.apache.flink.runtime.jobmaster.slotpool.strategy;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.ResolvedTaskManagerTopology;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test case for task manager allocation strategy.
 */
public class AllocateTaskManagerStrategyTest {
	/**
	 * Test random allocate taskmanagers.
	 */
	@Test
	public void testRandomAllocateTaskManagers() {
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		for (int i = 0; i < 100; i++) {
			taskManagers.put(ResourceID.generate(), new ResolvedTaskManagerTopology(null, null));
		}

		Set<ResourceID> resourceIds1 = RandomTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 10);
		Set<ResourceID> resourceIds2 = RandomTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 10);
		assertEquals(10, resourceIds1.size());
		assertNotEquals(resourceIds1, resourceIds2);
	}

	/**
	 * Test random allocate when there are not enough task managers.
	 */
	@Test
	public void testRandomAllocateNotEnoughTaskManagers() {
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			taskManagers.put(ResourceID.generate(), new ResolvedTaskManagerTopology(null, null));
		}

		Set<ResourceID> resourceIds1 = RandomTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 20);
		assertEquals(resourceIds1, taskManagers.keySet());

		Set<ResourceID> resourceIds2 = RandomTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 0);
		assertEquals(resourceIds2, taskManagers.keySet());
	}

	/**
	 * Test job spread allocate task managers.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testJobSpreadAllocateTaskManagers() throws Exception {
		List<ResourceID> resourceIdList = new ArrayList<>();
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		InetAddress inetAddress = InetAddress.getLocalHost();
		for (int i = 0; i < 100; i++) {
			ResourceID resourceId = ResourceID.generate();
			resourceIdList.add(resourceId);
			ResolvedTaskManagerTopology taskManagerTopology = new ResolvedTaskManagerTopology(
				null,
				new TaskManagerLocation(resourceId, inetAddress, -1));
			for (int j = 0; j < i; j++) {
				taskManagerTopology.incrementRunningJob();
			}
			taskManagers.put(resourceId, taskManagerTopology);
		}

		Set<ResourceID> allocatedResourceIds = JobSpreadTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 10);
		assertEquals(new HashSet<>(resourceIdList.subList(0, 10)), allocatedResourceIds);

		for (int i = 0; i < 10; i++) {
			ResourceID resourceId = resourceIdList.get(i);
			assertEquals(i + 1, taskManagers.get(resourceId).getRunningJobCount());
		}

		JobSpreadTaskManagerStrategy.getInstance().releaseTaskManagers(allocatedResourceIds, taskManagers);
		for (int i = 0; i < 10; i++) {
			ResourceID resourceId = resourceIdList.get(i);
			assertEquals(i, taskManagers.get(resourceId).getRunningJobCount());
		}
	}

	/**
	 * Test job spread allocate task managers when there are not enough task managers.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testJobSpreadAllocateNotEnoughTaskManagers() throws Exception {
		List<ResourceID> resourceIdList = new ArrayList<>();
		Map<ResourceID, ResolvedTaskManagerTopology> taskManagers = new HashMap<>();
		InetAddress inetAddress = InetAddress.getLocalHost();
		for (int i = 0; i < 10; i++) {
			ResourceID resourceId = ResourceID.generate();
			resourceIdList.add(resourceId);
			ResolvedTaskManagerTopology taskManagerTopology = new ResolvedTaskManagerTopology(
				null,
				new TaskManagerLocation(resourceId, inetAddress, -1));
			for (int j = 0; j < i; j++) {
				taskManagerTopology.incrementRunningJob();
			}
			taskManagers.put(resourceId, taskManagerTopology);
		}

		Set<ResourceID> allocatedResourceIds = JobSpreadTaskManagerStrategy.getInstance().allocateTaskManagers(taskManagers, 20);
		assertEquals(taskManagers.keySet(), allocatedResourceIds);

		for (int i = 0; i < 10; i++) {
			ResourceID resourceId = resourceIdList.get(i);
			assertEquals(i + 1, taskManagers.get(resourceId).getRunningJobCount());
		}

		JobSpreadTaskManagerStrategy.getInstance().releaseTaskManagers(allocatedResourceIds, taskManagers);
		for (int i = 0; i < 10; i++) {
			ResourceID resourceId = resourceIdList.get(i);
			assertEquals(i, taskManagers.get(resourceId).getRunningJobCount());
		}
	}
}
