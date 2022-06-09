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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.DummyJobInformation;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Builder for {@link JobDeploymentDescriptor}.
 */
public class JobDeploymentDescriptorBuilder {
	private JobID jobId;
	private JobVertexID jobVertexId;
	private TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation;
	private TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> serializedTaskInformation;
	private ExecutionAttemptID executionId;
	private AllocationID allocationId;
	private int subtaskIndex;
	private int attemptNumber;
	private List<JobTaskPartitionDescriptor> taskPartitionDescriptors;
	private List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitionDescriptors;
	private List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates;
	private List<JobTaskInputGateDeploymentDescriptor> pointwiseInputGates;
	private int targetSlotNumber;

	@Nullable
	private JobManagerTaskRestore taskRestore;

	private JobDeploymentDescriptorBuilder(
			JobID jobId,
			JobVertexID jobVertexId,
			String invokableClassName) throws IOException {
		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			"test task",
			1,
			1,
			invokableClassName,
			new Configuration());

		this.jobId = jobId;
		this.jobVertexId = jobVertexId;
		this.serializedJobInformation =
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(new DummyJobInformation(jobId, "DummyJob")));
		this.serializedTaskInformation = new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation));
		this.executionId = new ExecutionAttemptID();
		this.allocationId = new AllocationID();
		this.subtaskIndex = 0;
		this.attemptNumber = 0;
		this.vertexResultPartitionDescriptors = Collections.emptyList();
		this.taskPartitionDescriptors = Collections.emptyList();
		this.allToAllInputGates = Collections.emptyList();
		this.pointwiseInputGates = Collections.emptyList();
		this.targetSlotNumber = 0;
		this.taskRestore = null;
	}

	public JobDeploymentDescriptorBuilder setSerializedJobInformation(
		TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation) {
		this.serializedJobInformation = serializedJobInformation;
		return this;
	}

	public JobDeploymentDescriptorBuilder setSerializedTaskInformation(
		TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> serializedTaskInformation) {
		this.serializedTaskInformation = serializedTaskInformation;
		return this;
	}

	public JobDeploymentDescriptorBuilder setJobId(JobID jobId) {
		this.jobId = jobId;
		return this;
	}

	public JobDeploymentDescriptorBuilder setJobVertexId(JobVertexID jobVertexId) {
		this.jobVertexId = jobVertexId;
		return this;
	}

	public JobDeploymentDescriptorBuilder setExecutionId(ExecutionAttemptID executionId) {
		this.executionId = executionId;
		return this;
	}

	public JobDeploymentDescriptorBuilder setAllocationId(AllocationID allocationId) {
		this.allocationId = allocationId;
		return this;
	}

	public JobDeploymentDescriptorBuilder setSubtaskIndex(int subtaskIndex) {
		this.subtaskIndex = subtaskIndex;
		return this;
	}

	public JobDeploymentDescriptorBuilder setAttemptNumber(int attemptNumber) {
		this.attemptNumber = attemptNumber;
		return this;
	}

	public JobDeploymentDescriptorBuilder setTaskPartitionDescriptors(
			List<JobTaskPartitionDescriptor> taskPartitionDescriptors) {
		this.taskPartitionDescriptors = taskPartitionDescriptors;
		return this;
	}

	public JobDeploymentDescriptorBuilder setVertexResultPartitionDescriptors(
			List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitionDescriptors) {
		this.vertexResultPartitionDescriptors = vertexResultPartitionDescriptors;
		return this;
	}

	public JobDeploymentDescriptorBuilder setAllToAllInputGates(List<JobVertexInputGateDeploymentDescriptor> inputGates) {
		this.allToAllInputGates = inputGates;
		return this;
	}

	public JobDeploymentDescriptorBuilder setPointwiseInputGates(List<JobTaskInputGateDeploymentDescriptor> inputGates) {
		this.pointwiseInputGates = inputGates;
		return this;
	}

	public JobDeploymentDescriptorBuilder setTargetSlotNumber(int targetSlotNumber) {
		this.targetSlotNumber = targetSlotNumber;
		return this;
	}

	public JobDeploymentDescriptorBuilder setTaskRestore(@Nullable JobManagerTaskRestore taskRestore) {
		this.taskRestore = taskRestore;
		return this;
	}

	public JobDeploymentDescriptor build() {
		JobDeploymentDescriptor jdd = new JobDeploymentDescriptor(jobId, serializedJobInformation);
		JobVertexDeploymentDescriptor jvdd = new JobVertexDeploymentDescriptor(
			jobVertexId,
			serializedTaskInformation,
			vertexResultPartitionDescriptors,
			allToAllInputGates);
		JobTaskDeploymentDescriptor jtdd = new JobTaskDeploymentDescriptor(
			subtaskIndex,
			executionId,
			allocationId,
			attemptNumber,
			targetSlotNumber,
			taskRestore,
			taskPartitionDescriptors,
			pointwiseInputGates);
		jvdd.addJobTaskDeploymentDescriptor(jtdd);
		jdd.addJobVertexDeploymentDescriptor(jvdd);
		return jdd;
	}

	public static JobDeploymentDescriptorBuilder newBuilder(
			JobID jobId,
			JobVertexID jobVertexId,
			Class<?> invokableClass)  throws IOException {
		return new JobDeploymentDescriptorBuilder(jobId, jobVertexId, invokableClass.getName());
	}
}
