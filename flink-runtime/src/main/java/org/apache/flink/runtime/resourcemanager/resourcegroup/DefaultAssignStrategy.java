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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;
import org.apache.flink.runtime.resourcemanager.resourcegroup.client.ResourceClientUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Default AssignStrategy responsible for minimize TaskManager movement between different ResourceGroups.
 */
public class DefaultAssignStrategy implements AssignStrategy {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onResourceInfoChanged(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> previousResourceInfos,
		List<ResourceInfo> currentResourceInfos,
		TaskManagerSpec taskManagerSpec) {
		log.info("onResourceInfoChanged: previousResourceInfos {}, currentResourceInfos {}.", previousResourceInfos, currentResourceInfos);

		// 1, differentiate currently assigned TaskManagers(key: ResourceInfo, val: TaskManagers) and unassigned TaskManagers
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned = extractAssigned(taskManagers);
		List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> unassigned = extractUnassigned(taskManagers);

		if (log.isInfoEnabled()) {
			log.info("Current assigned status:");
			assigned.forEach((k, v) -> {
				log.info("ResourceInfo: {}, TM size {}.", k.toString(), v.size());
			});
		}

		// 2, differentiate removed ResourceInfos
		Set<ResourceInfo> removedResourceInfos = previousResourceInfos
			.stream()
			.filter(((Predicate<ResourceInfo>) currentResourceInfos::contains).negate())
			.collect(Collectors.toSet());
		log.info("ResourceInfos to remove: {}", removedResourceInfos);

		// 3, move removedResourceInfos' TaskManagers from assigned to unassigned TaskManagers
		assigned.forEach((k, v) -> {
			if (removedResourceInfos.contains(k)) {
				unassigned.addAll(v);
			}
		});
		assigned.keySet().removeAll(removedResourceInfos);

		return redistributeUnassigned(unassigned, assigned, currentResourceInfos, taskManagerSpec);

	}

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onAddTaskManager(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> resourceInfos,
		ResourceID addedResourceID,
		UnresolvedTaskManagerTopology addedTaskManager,
		TaskManagerSpec taskManagerSpec) {

		if (resourceInfos.isEmpty()) {
			taskManagers.put(addedResourceID, addedTaskManager);
			return taskManagers;
		}

		// 1, differentiate currently assigned TaskManagers(key: ResourceInfo, val: TaskManagers) and unassigned TaskManagers
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned = extractAssigned(taskManagers);
		List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> unassigned = extractUnassigned(taskManagers);

		// 2, add new taskManager to the first of unassigned TaskManagers
		unassigned.add(0, new Tuple2<>(addedResourceID, addedTaskManager));

		return redistributeUnassigned(unassigned, assigned, resourceInfos, taskManagerSpec);
	}

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onRemoveTaskManager(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> resourceInfos,
		ResourceID removeResourceID,
		TaskManagerSpec taskManagerSpec) {

		taskManagers.remove(removeResourceID);

		if (resourceInfos.isEmpty()) {
			return taskManagers;
		}

		// 1, differentiate currently assigned TaskManagers(key: ResourceInfo, val: TaskManagers) and unassigned TaskManagers
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned = extractAssigned(taskManagers);
		List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> unassigned = extractUnassigned(taskManagers);

		return redistributeUnassigned(unassigned, assigned, resourceInfos, taskManagerSpec);
	}

	private Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> extractAssigned(Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers) {
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned = new HashMap<>();

		taskManagers.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			if (resourceInfo != null && !ResourceInfo.ResourceType.SHARED.equals(resourceInfo.getResourceType())) {
				assigned
					.computeIfAbsent(resourceInfo, x -> new ArrayList<>())
					.add(new Tuple2<>(k, v));
			}
		});

		return assigned;
	}

	private List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> extractUnassigned(Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers) {
		List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> unassigned = new ArrayList<>();

		taskManagers.forEach((k, v) -> {
			ResourceInfo resourceInfo = v.getResourceInfo();
			// SharedResourceInfo's taskManagers should also be labeled as unassigned taskManagers
			if (resourceInfo == null || ResourceInfo.ResourceType.SHARED.equals(resourceInfo.getResourceType())) {
				unassigned.add(new Tuple2<>(k, v));
			}
		});

		return unassigned;
	}

	private Map<ResourceID, UnresolvedTaskManagerTopology> redistributeUnassigned(
		List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> unassigned,
		Map<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> assigned,
		List<ResourceInfo> currentResourceInfos,
		TaskManagerSpec taskManagerSpec) {

		TreeSet<Tuple2<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>>> deficitResourceInfos = new TreeSet<>((o1, o2) -> {
			int required1 =  o1.f0.calRequiredTaskManager(taskManagerSpec);
			int required2 =  o2.f0.calRequiredTaskManager(taskManagerSpec);
			double deficitRatio1 = (required1 - o1.f1.size()) / (double) required1;
			double deficitRatio2 = (required2 - o2.f1.size()) / (double) required2;

			// larger deficit comes first
			if (deficitRatio1 > deficitRatio2) {
				return -1;
			} else if (deficitRatio1 < deficitRatio2) {
				return 1;
			}

			return o1.f0.compareTo(o2.f0);
		});

		for (ResourceInfo info : currentResourceInfos) {
			int required = info.calRequiredTaskManager(taskManagerSpec);
			List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> assignedList = assigned.computeIfAbsent(info, key -> new ArrayList<>());
			log.info("ResourceInfo {} currently has TM num {} and requires TM num {}.", info, assigned.size(), required);

			// move extra TaskManagers from assigned to unassigned
			if (required > 0 && required < assignedList.size()) {
				int extra = assignedList.size() - required;
				List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>> shrunk = assignedList.subList(0, extra);
				unassigned.addAll(new ArrayList<>(shrunk));
				shrunk.clear();
			} else if (required > assignedList.size()) {
				deficitResourceInfos.add(Tuple2.of(info, assignedList));
			}
		}

		while (!unassigned.isEmpty() && !deficitResourceInfos.isEmpty()) {
			Tuple2<ResourceInfo, List<Tuple2<ResourceID, UnresolvedTaskManagerTopology>>> largestDeficit = deficitResourceInfos.pollFirst();
			if (largestDeficit == null) {
				break;
			}

			Tuple2<ResourceID, UnresolvedTaskManagerTopology> addTm = unassigned.remove(0);
			// set resourceInfo for new added taskManagers
			addTm.f1.setResourceInfo(largestDeficit.f0);
			largestDeficit.f1.add(addTm);
			int required = largestDeficit.f0.calRequiredTaskManager(taskManagerSpec);

			// add back resourceInfo if it is still deficit
			if (required > largestDeficit.f1.size()) {
				deficitResourceInfos.add(largestDeficit);
			}
		}

		// add left taskManagers to sharedResourceInfo
		if (!unassigned.isEmpty()) {
			ResourceInfo sharedResourceInfo = currentResourceInfos
				.stream()
				.filter(r -> ResourceInfo.ResourceType.SHARED.equals(r.getResourceType()))
				.findAny()
				.orElse(ResourceClientUtils.getDefaultSharedResourceInfo());

			// set resourceInfo for new added taskManagers
			unassigned.forEach(t -> t.f1.setResourceInfo(sharedResourceInfo));

			assigned
				.computeIfAbsent(sharedResourceInfo, x -> new ArrayList<>())
				.addAll(unassigned);
			unassigned.clear();
		}

		if (log.isInfoEnabled()) {
			log.info("After assigned status:");
			assigned.forEach((k, v) -> {
				log.info("ResourceInfo: {}, TM size {}.", k.toString(), v.size());
			});
		}

		return assigned
			.values()
			.stream()
			.flatMap(List::stream)
			.collect(
				Collectors.toMap(t -> t.f0, t -> t.f1)
			);
	}
}
