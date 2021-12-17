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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.GatewayTaskDeployment;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;

import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Manager of gateway deployment.
 */
public class GatewayDeploymentManager {
	private final Map<ResourceID, TaskManagerGateway> resourceGatewayMap;
	private final Map<ResourceID, List<GatewayTaskDeployment>> gatewayDeploymentList;

	public GatewayDeploymentManager() {
		this.resourceGatewayMap = new HashMap<>();
		this.gatewayDeploymentList = new HashedMap();
	}

	public void addGatewayDeployment(ResourceID resourceId, TaskManagerGateway gateway, GatewayTaskDeployment deploymentDescriptor) {
		resourceGatewayMap.putIfAbsent(resourceId, gateway);
		List<GatewayTaskDeployment> gatewayTaskDeployments = gatewayDeploymentList.computeIfAbsent(resourceId, key -> new ArrayList<>());
		gatewayTaskDeployments.add(deploymentDescriptor);
	}

	public Map<ResourceID, List<GatewayTaskDeployment>> getGatewayDeploymentList() {
		return gatewayDeploymentList;
	}

	public TaskManagerGateway getTaskManagerGateway(ResourceID resourceId) {
		return checkNotNull(resourceGatewayMap.get(resourceId));
	}
}
