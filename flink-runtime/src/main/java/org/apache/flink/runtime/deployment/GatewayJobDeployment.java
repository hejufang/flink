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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Job deployment for each gateway.
 */
public class GatewayJobDeployment {
	private final JobDeploymentDescriptor jobDeploymentDescriptor;
	private final List<ExecutionConsumerEntity> executionConsumerEntityList;
	private final Map<JobVertexID, JobVertexDeploymentDescriptor> vertexDeploymentDescriptorMap;

	public GatewayJobDeployment(JobDeploymentDescriptor jobDeploymentDescriptor) {
		this.jobDeploymentDescriptor = jobDeploymentDescriptor;
		this.executionConsumerEntityList = new ArrayList<>();
		this.vertexDeploymentDescriptorMap = new HashMap<>();
	}

	public void addJobVertexDeploymentDescriptor(
			LogicalSlot slot,
			JobManagerTaskRestore taskRestore,
			boolean updateConsumers,
			Execution execution) throws IOException {
		executionConsumerEntityList.add(new ExecutionConsumerEntity(execution, updateConsumers));
		JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor = vertexDeploymentDescriptorMap.get(execution.getVertex().getJobvertexId());
		if (jobVertexDeploymentDescriptor == null) {
			JobDeploymentDescriptorHelper.JobVertexResultPartitionEntity vertexResultPartitionEntity =
				JobDeploymentDescriptorHelper.createJobVertexResultPartitionEntity(execution);
			JobDeploymentDescriptorHelper.JobVertexInputGatesEntity vertexInputGatesEntity =
				JobDeploymentDescriptorHelper.createJobVertexInputGatesEntity(execution.getVertex());

			jobVertexDeploymentDescriptor = JobVertexDeploymentDescriptor.from(
				execution,
				vertexResultPartitionEntity.getVertexResultPartitionDeploymentList(),
				vertexInputGatesEntity.getAllToAllInputGates());
			vertexDeploymentDescriptorMap.put(execution.getVertex().getJobvertexId(), jobVertexDeploymentDescriptor);

			jobVertexDeploymentDescriptor.addJobTaskDeploymentDescriptor(
				JobTaskDeploymentDescriptor.from(
					taskRestore,
					slot,
					execution,
					vertexResultPartitionEntity.getTaskPartitionDescriptorList(),
					vertexInputGatesEntity.getPointWiseInputGates()));
			jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(jobVertexDeploymentDescriptor);
		} else {
			jobVertexDeploymentDescriptor.addJobTaskDeploymentDescriptor(
				JobTaskDeploymentDescriptor.from(
					taskRestore,
					slot,
					execution));
		}
	}

	public JobDeploymentDescriptor getJobDeploymentDescriptor() {
		return jobDeploymentDescriptor;
	}

	public List<ExecutionConsumerEntity> getExecutionConsumerEntityList() {
		return executionConsumerEntityList;
	}
}
