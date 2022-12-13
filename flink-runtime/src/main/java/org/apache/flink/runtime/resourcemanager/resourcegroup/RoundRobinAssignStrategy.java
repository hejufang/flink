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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.UnresolvedTaskManagerTopology;

import java.util.List;
import java.util.Map;

/**
 * Round-Robin AssignStrategy responsible for minimize TaskManager movement between different ResourceGroups.
 */
public class RoundRobinAssignStrategy implements AssignStrategy {

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onResourceInfoChanged(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> previousResourceInfos,
		List<ResourceInfo> currentResourceInfos,
		TaskManagerSpec taskManagerSpec) {
		return null;
	}

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onAddTaskManager(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> resourceInfos,
		ResourceID addedResourceID,
		UnresolvedTaskManagerTopology addedTaskManager,
		TaskManagerSpec taskManagerSpec) {
		return null;
	}

	@Override
	public Map<ResourceID, UnresolvedTaskManagerTopology> onRemoveTaskManager(
		Map<ResourceID, UnresolvedTaskManagerTopology> taskManagers,
		List<ResourceInfo> resourceInfos,
		ResourceID removeResourceID,
		TaskManagerSpec taskManagerSpec) {
		return null;
	}
}