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

import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.GatewayJobDeployment;
import org.apache.flink.runtime.deployment.GatewayTaskDeployment;
import org.apache.flink.runtime.deployment.JobDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Manager of gateway deployment.
 */
public class GatewayDeploymentManager {
	private final boolean optimizedJobDeploymentStructureEnable;
	private final Map<ResourceID, TaskManagerGateway> resourceGatewayMap;
	private final Map<ResourceID, List<GatewayTaskDeployment>> gatewayDeploymentList;
	private final Map<ResourceID, GatewayJobDeployment> gatewayJobDeployment;

	public GatewayDeploymentManager(boolean optimizedJobDeploymentStructureEnable) {
		this.optimizedJobDeploymentStructureEnable = optimizedJobDeploymentStructureEnable;
		this.resourceGatewayMap = new HashMap<>();
		this.gatewayDeploymentList = new HashMap<>();
		this.gatewayJobDeployment = new HashMap<>();
	}

	public void addGatewayDeployment(ResourceID resourceId, TaskManagerGateway gateway, GatewayTaskDeployment deploymentDescriptor) {
		resourceGatewayMap.putIfAbsent(resourceId, gateway);
		List<GatewayTaskDeployment> gatewayTaskDeployments = gatewayDeploymentList.computeIfAbsent(resourceId, key -> new ArrayList<>());
		gatewayTaskDeployments.add(deploymentDescriptor);
	}

	public void addGatewayJobDeployment(
			ResourceID resourceId,
			TaskManagerGateway gateway,
			LogicalSlot slot,
			JobManagerTaskRestore taskRestore,
			boolean updateConsumer,
			Execution execution) throws IOException {
		Preconditions.checkArgument(optimizedJobDeploymentStructureEnable);

		resourceGatewayMap.putIfAbsent(resourceId, gateway);
		GatewayJobDeployment jobDeployment = gatewayJobDeployment.computeIfAbsent(
			resourceId,
			key -> new GatewayJobDeployment(JobDeploymentDescriptor.from(execution.getVertex().getExecutionGraph())));
		jobDeployment.addJobVertexDeploymentDescriptor(slot, taskRestore, updateConsumer, execution);
	}

	public Map<ResourceID, List<GatewayTaskDeployment>> getGatewayDeploymentList() {
		return gatewayDeploymentList;
	}

	public Map<ResourceID, GatewayJobDeployment> getGatewayJobDeployment() {
		return gatewayJobDeployment;
	}

	public TaskManagerGateway getTaskManagerGateway(ResourceID resourceId) {
		return checkNotNull(resourceGatewayMap.get(resourceId));
	}

	public boolean isOptimizedJobDeploymentStructureEnable() {
		return optimizedJobDeploymentStructureEnable;
	}
}
