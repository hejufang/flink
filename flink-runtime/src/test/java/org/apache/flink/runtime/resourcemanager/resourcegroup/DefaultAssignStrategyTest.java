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

package org.apache.flink.runtime.resourcemanager.resourcegroup;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link DefaultAssignStrategy}.
 */
public class DefaultAssignStrategyTest {

	private final DefaultAssignStrategy defaultAssignStrategy = new DefaultAssignStrategy();

	@Test
	public void testOnResourceInfoChangedAndTMChanged() {
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers = new HashMap<>();
		List<ResourceInfo> previousResourceInfos = new ArrayList<>();
		List<ResourceInfo> currentResourceInfos = new ArrayList<>();
		TaskManagerSpec taskManagerSpec = new TaskManagerSpec(12, MemorySize.ofMebiBytes(60 * 1024));

		for (int i = 0; i < 30; i++) {
			taskManagers.put(ResourceID.generate(), new UnresolvedTaskManagerTopology(null, null, null, null));
		}

		ResourceInfo resourceInfo1 = ResourceInfo
			.builder()
			.setId("1")
			.setResourceName("1")
			.setClusterName("default")
			.setResourceType(ResourceInfo.ResourceType.ISOLATED)
			.setApCPUCores(80.0)
			.build();

		ResourceInfo resourceInfo2 = ResourceInfo
			.builder()
			.setId("2")
			.setResourceName("2")
			.setClusterName("default")
			.setResourceType(ResourceInfo.ResourceType.ISOLATED)
			.setApCPUCores(90.0)
			.build();

		ResourceInfo resourceInfo3 = ResourceInfo
			.builder()
			.setId("3")
			.setResourceName("3")
			.setClusterName("default")
			.setResourceType(ResourceInfo.ResourceType.ISOLATED)
			.setApCPUCores(100.0)
			.build();

		ResourceInfo resourceInfo4 = ResourceClientUtils.getDefaultSharedResourceInfo();

		ResourceInfo resourceInfo5 = ResourceInfo
			.builder()
			.setId("5")
			.setResourceName("5")
			.setClusterName("default")
			.setResourceType(ResourceInfo.ResourceType.ISOLATED)
			.setApCPUCores(300.0)
			.build();

		currentResourceInfos.add(resourceInfo1);
		currentResourceInfos.add(resourceInfo2);
		currentResourceInfos.add(resourceInfo3);
		currentResourceInfos.add(resourceInfo5);

		Map<ResourceID, UnresolvedTaskManagerTopology> result = defaultAssignStrategy.onResourceInfoChanged(taskManagers, previousResourceInfos, currentResourceInfos, taskManagerSpec);
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned = new HashMap<>();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo1).size(), 4);
		assertEquals(assigned.get(resourceInfo2).size(), 5);
		assertEquals(assigned.get(resourceInfo3).size(), 6);
		assertEquals(assigned.get(resourceInfo5).size(), 15);

		previousResourceInfos = new ArrayList<>(currentResourceInfos);
		currentResourceInfos.remove(3);
		resourceInfo5 = ResourceInfo
			.builder()
			.setId("5")
			.setResourceName("5")
			.setClusterName("default")
			.setResourceType(ResourceInfo.ResourceType.ISOLATED)
			.setApCPUCores(70.0)
			.build();
		currentResourceInfos.add(resourceInfo5);
		result = defaultAssignStrategy.onResourceInfoChanged(taskManagers, previousResourceInfos, currentResourceInfos, taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo1).size(), 7);
		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo5).size(), 6);

		previousResourceInfos = new ArrayList<>(currentResourceInfos);
		currentResourceInfos.remove(3);
		result = defaultAssignStrategy.onResourceInfoChanged(taskManagers, previousResourceInfos, currentResourceInfos, taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo1).size(), 7);
		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo4).size(), 6);

		previousResourceInfos = new ArrayList<>(currentResourceInfos);
		currentResourceInfos.remove(resourceInfo1);
		result = defaultAssignStrategy.onResourceInfoChanged(taskManagers, previousResourceInfos, currentResourceInfos, taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo4).size(), 13);

		ResourceID added = ResourceID.generate();
		result = defaultAssignStrategy.onAddTaskManager(taskManagers, currentResourceInfos, added, new UnresolvedTaskManagerTopology(null, null, null, null), taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo4).size(), 14);

		result = defaultAssignStrategy.onRemoveTaskManager(taskManagers, currentResourceInfos, added, taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo4).size(), 13);

		previousResourceInfos = new ArrayList<>(currentResourceInfos);
		currentResourceInfos.add(resourceInfo1);
		result = defaultAssignStrategy.onResourceInfoChanged(taskManagers, previousResourceInfos, currentResourceInfos, taskManagerSpec);
		assigned.clear();

		result.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		assertEquals(assigned.get(resourceInfo1).size(), 7);
		assertEquals(assigned.get(resourceInfo2).size(), 8);
		assertEquals(assigned.get(resourceInfo3).size(), 9);
		assertEquals(assigned.get(resourceInfo4).size(), 6);
	}
}
