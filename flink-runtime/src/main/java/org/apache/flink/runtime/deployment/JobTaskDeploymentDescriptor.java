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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Deployment descriptor for execution subtask.
 */
public class JobTaskDeploymentDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private int subtaskIndex;

	private ExecutionAttemptID executionAttemptId;

	private AllocationID allocationId;

	private int attemptNumber;

	private List<JobTaskPartitionDescriptor> taskPartitionDescriptorList;

	/** The list of consumed intermediate result partitions. */
	private List<JobTaskInputGateDeploymentDescriptor> inputGates;

	/** Slot number to run the sub task in on the target machine. */
	private int targetSlotNumber;

	/** Information to restore the task. This can be null if there is no state to restore. */
	@Nullable
	private JobManagerTaskRestore taskRestore;

	private transient Map<ResourceID, String> connectionInfoMap;

	public JobTaskDeploymentDescriptor(Map<ResourceID, String> connectionInfoMap) {
		this.connectionInfoMap = connectionInfoMap;
	}

	public JobTaskDeploymentDescriptor(
			int subtaskIndex,
			ExecutionAttemptID executionAttemptId,
			AllocationID allocationId,
			int attemptNumber,
			int targetSlotNumber,
			@Nullable JobManagerTaskRestore taskRestore,
			List<JobTaskPartitionDescriptor> taskPartitionDescriptorList,
			List<JobTaskInputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
		this.subtaskIndex = subtaskIndex;
		this.executionAttemptId = executionAttemptId;
		this.allocationId = allocationId;

		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		this.attemptNumber = attemptNumber;

		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");
		this.targetSlotNumber = targetSlotNumber;
		this.taskRestore = taskRestore;

		this.taskPartitionDescriptorList = taskPartitionDescriptorList;
		this.inputGates = inputGateDeploymentDescriptors;
	}

	public int getSubtaskIndex() {
		return subtaskIndex;
	}

	public ExecutionAttemptID getExecutionAttemptId() {
		return executionAttemptId;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public int getAttemptNumber() {
		return attemptNumber;
	}

	public List<JobTaskPartitionDescriptor> getTaskPartitionDescriptorList() {
		return taskPartitionDescriptorList;
	}

	public List<JobTaskInputGateDeploymentDescriptor> getInputGates() {
		return inputGates;
	}

	public int getTargetSlotNumber() {
		return targetSlotNumber;
	}

	@Nullable
	public JobManagerTaskRestore getTaskRestore() {
		return taskRestore;
	}

	public static JobTaskDeploymentDescriptor from(
			JobManagerTaskRestore taskRestore,
			LogicalSlot slot,
			Execution execution) {
		return from(
			taskRestore,
			slot,
			execution,
			JobDeploymentDescriptorHelper.createTaskPartitionList(execution),
			JobDeploymentDescriptorHelper.createTaskInputGate(execution.getVertex()));
	}

	public static JobTaskDeploymentDescriptor from(
			JobManagerTaskRestore taskRestore,
			LogicalSlot slot,
			Execution execution,
			List<JobTaskPartitionDescriptor> taskPartitionDescriptorList,
			List<JobTaskInputGateDeploymentDescriptor> inputGates) {
		return new JobTaskDeploymentDescriptor(
			execution.getParallelSubtaskIndex(),
			execution.getAttemptId(),
			slot.getAllocationId(),
			execution.getAttemptNumber(),
			slot.getPhysicalSlotNumber(),
			taskRestore,
			taskPartitionDescriptorList,
			inputGates);
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeInt(subtaskIndex);
		out.writeLong(executionAttemptId.getLowerPart());
		out.writeLong(executionAttemptId.getUpperPart());
		out.writeLong(allocationId.getLowerPart());
		out.writeLong(allocationId.getUpperPart());
		out.writeInt(attemptNumber);
		out.writeInt(taskPartitionDescriptorList.size());
		for (JobTaskPartitionDescriptor partitionDescriptor: taskPartitionDescriptorList) {
			partitionDescriptor.write(out);
		}
		out.writeInt(inputGates == null ? 0 : inputGates.size());
		if (inputGates != null) {
			for (JobTaskInputGateDeploymentDescriptor inputGateDeploymentDescriptor : inputGates) {
				inputGateDeploymentDescriptor.write(out);
			}
		}
		out.writeInt(targetSlotNumber);
		// Use java serialize for taskRestore, since store object would be null in OLAP part
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(taskRestore);
		oos.flush();
		byte[] storeObj = baos.toByteArray();
		out.writeInt(storeObj.length);
		out.write(storeObj);
	}

	@Override
	public void read(DataInputView in) throws Exception {
		subtaskIndex = in.readInt();
		executionAttemptId = new ExecutionAttemptID(in.readLong(), in.readLong());
		allocationId = new AllocationID(in.readLong(), in.readLong());
		attemptNumber = in.readInt();
		int partitionListSize = in.readInt();
		taskPartitionDescriptorList = new ArrayList<>();
		for (int i = 0; i < partitionListSize; i++) {
			JobTaskPartitionDescriptor partitionDescriptor = new JobTaskPartitionDescriptor();
			partitionDescriptor.read(in);
			taskPartitionDescriptorList.add(partitionDescriptor);
		}
		int inputGateListSize = in.readInt();
		if (inputGateListSize > 0) {
			inputGates = new ArrayList<>();
			for (int i = 0; i < inputGateListSize; i++) {
				JobTaskInputGateDeploymentDescriptor inputGateDeploymentDescriptor =
					new JobTaskInputGateDeploymentDescriptor(connectionInfoMap);
				inputGateDeploymentDescriptor.read(in);
				inputGates.add(inputGateDeploymentDescriptor);
			}
		}
		targetSlotNumber = in.readInt();
		// Use java deserialize for taskRestore, since store object would be null in OLAP part
		int storeObjLen = in.readInt();
		byte[] storeObj = new byte[storeObjLen];
		in.readFully(storeObj);
		ByteArrayInputStream bais = new ByteArrayInputStream(storeObj);
		ObjectInputStream ois = new ObjectInputStream(bais);
		taskRestore = (JobManagerTaskRestore) ois.readObject();
	}
}
