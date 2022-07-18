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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A job vertex deployment descriptor contains deployment information for the same task.
 */
public class JobVertexDeploymentDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private JobVertexID jobVertexId;

	private TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> serializedTaskInformation;

	private List<JobTaskDeploymentDescriptor> taskDeploymentDescriptorList;

	private List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitions;

	private List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates;

	private transient Map<ResourceID, String> connectionInfoMap;

	public JobVertexDeploymentDescriptor(Map<ResourceID, String> connectionInfoMap) {
		this.connectionInfoMap = connectionInfoMap;
	}

	public JobVertexDeploymentDescriptor(
			JobVertexID jobVertexId,
			TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> serializedTaskInformation,
			List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitions,
			List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates) {
		this.jobVertexId = jobVertexId;
		this.serializedTaskInformation = serializedTaskInformation;
		this.taskDeploymentDescriptorList = new ArrayList<>();
		this.vertexResultPartitions = vertexResultPartitions;
		this.allToAllInputGates = allToAllInputGates;
	}

	public void addJobTaskDeploymentDescriptor(JobTaskDeploymentDescriptor jobTaskDeploymentDescriptor) {
		taskDeploymentDescriptorList.add(jobTaskDeploymentDescriptor);
	}

	public JobVertexID getJobVertexId() {
		return jobVertexId;
	}

	public List<JobTaskDeploymentDescriptor> getTaskDeploymentDescriptorList() {
		return taskDeploymentDescriptorList;
	}

	public SerializedValue<TaskInformation> getSerializedTaskInformation() {
		if (serializedTaskInformation instanceof TaskDeploymentDescriptor.NonOffloaded) {
			TaskDeploymentDescriptor.NonOffloaded<TaskInformation> taskInformation =
				(TaskDeploymentDescriptor.NonOffloaded<TaskInformation>) serializedTaskInformation;
			return taskInformation.serializedValue;
		} else {
			throw new IllegalStateException(
				"Trying to work with offloaded serialized job information.");
		}
	}

	public List<JobVertexResultPartitionDeploymentDescriptor> getVertexResultPartitions() {
		return vertexResultPartitions;
	}

	public List<JobVertexInputGateDeploymentDescriptor> getAllToAllInputGates() {
		return allToAllInputGates;
	}

	public void loadBigData(JobID jobId, @Nullable PermanentBlobService blobService)
		throws IOException, ClassNotFoundException {
		// re-integrate offloaded task info from blob
		if (serializedTaskInformation instanceof TaskDeploymentDescriptor.Offloaded) {
			PermanentBlobKey taskInfoKey = ((TaskDeploymentDescriptor.Offloaded<TaskInformation>) serializedTaskInformation).serializedValueKey;

			Preconditions.checkNotNull(blobService);

			final File dataFile = blobService.getFile(jobId, taskInfoKey);
			// NOTE: Do not delete the task info BLOB since it may be needed again during recovery.
			//       (it is deleted automatically on the BLOB server and cache when the job
			//       enters a terminal state)
			SerializedValue<TaskInformation> serializedValue =
				SerializedValue.fromBytes(FileUtils.readAllBytes(dataFile.toPath()));
			serializedTaskInformation = new TaskDeploymentDescriptor.NonOffloaded<>(serializedValue);
		}

		// make sure that the serialized task information fields are filled
		Preconditions.checkNotNull(serializedTaskInformation);
	}

	public static JobVertexDeploymentDescriptor from(
			Execution execution,
			List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitions,
			List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates) throws IOException {
		return new JobVertexDeploymentDescriptor(
			execution.getVertex().getJobvertexId(),
			TaskDeploymentDescriptorFactory.getSerializedTaskInformation(
				execution.getVertex().getJobVertex().getTaskInformationOrBlobKey()),
			vertexResultPartitions,
			allToAllInputGates);
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(jobVertexId.getLowerPart());
		out.writeLong(jobVertexId.getUpperPart());
		if (serializedTaskInformation instanceof TaskDeploymentDescriptor.NonOffloaded) {
			out.writeBoolean(true);
			TaskDeploymentDescriptor.NonOffloaded<TaskInformation> nonOffloaded = (TaskDeploymentDescriptor.NonOffloaded<TaskInformation>) serializedTaskInformation;
			byte[] serializedValue = nonOffloaded.serializedValue.getByteArray();
			out.writeInt(serializedValue.length);
			out.write(serializedValue);
		} else {
			out.writeBoolean(false);
			TaskDeploymentDescriptor.Offloaded<TaskInformation> offloaded =
				(TaskDeploymentDescriptor.Offloaded<TaskInformation>) serializedTaskInformation;
			PermanentBlobKey taskInfoKey = offloaded.serializedValueKey;
			out.writeInt(taskInfoKey.getHash().length);
			out.write(taskInfoKey.getHash());
			out.writeInt(taskInfoKey.getType().ordinal());
			out.writeLong(taskInfoKey.getRandom().getLowerPart());
			out.writeLong(taskInfoKey.getRandom().getUpperPart());
		}
		out.writeInt(taskDeploymentDescriptorList.size());
		for (JobTaskDeploymentDescriptor taskDeploymentDescriptor: taskDeploymentDescriptorList) {
			taskDeploymentDescriptor.write(out);
		}
		out.writeInt(vertexResultPartitions.size());
		for (JobVertexResultPartitionDeploymentDescriptor partitionDeploymentDescriptor: vertexResultPartitions) {
			partitionDeploymentDescriptor.write(out);
		}
		out.writeInt(allToAllInputGates == null ? 0 : allToAllInputGates.size());
		if (allToAllInputGates != null) {
			for (JobVertexInputGateDeploymentDescriptor jobVertexInputGateDeploymentDescriptor : allToAllInputGates) {
				jobVertexInputGateDeploymentDescriptor.write(out);
			}
		}
	}

	@Override
	public void read(DataInputView in) throws Exception {
		jobVertexId = new JobVertexID(in.readLong(), in.readLong());
		boolean nonOffloadedFlag = in.readBoolean();
		if (nonOffloadedFlag) {
			int len = in.readInt();
			byte[] serializedValue = new byte[len];
			in.readFully(serializedValue);
			serializedTaskInformation = new TaskDeploymentDescriptor.NonOffloaded<>(SerializedValue.fromBytes(serializedValue));
		} else {
			int keyLength = in.readInt();
			byte[] key = new byte[keyLength];
			in.readFully(key);
			BlobKey.BlobType type = BlobKey.BlobType.values()[in.readInt()];
			AbstractID random = new AbstractID(in.readLong(), in.readLong());
			PermanentBlobKey jobInfoKey = new PermanentBlobKey(type, key, random);
			serializedTaskInformation = new TaskDeploymentDescriptor.Offloaded<>(jobInfoKey);
		}
		int taskDeploymentListSize = in.readInt();
		taskDeploymentDescriptorList = new ArrayList<>();
		for (int i = 0; i < taskDeploymentListSize; i++) {
			JobTaskDeploymentDescriptor taskDeploymentDescriptor = new JobTaskDeploymentDescriptor(connectionInfoMap);
			taskDeploymentDescriptor.read(in);
			taskDeploymentDescriptorList.add(taskDeploymentDescriptor);
		}
		int partitionListSize = in.readInt();
		vertexResultPartitions = new ArrayList<>();
		for (int i = 0; i < partitionListSize; i++) {
			JobVertexResultPartitionDeploymentDescriptor vertexResultPartitionDeploymentDescriptor =
				new JobVertexResultPartitionDeploymentDescriptor();
			vertexResultPartitionDeploymentDescriptor.read(in);
			vertexResultPartitions.add(vertexResultPartitionDeploymentDescriptor);
		}
		int inputGateListSize = in.readInt();
		if (inputGateListSize > 0) {
			allToAllInputGates = new ArrayList<>();
			for (int i = 0; i < inputGateListSize; i++) {
				JobVertexInputGateDeploymentDescriptor vertexInputGateDeploymentDescriptor =
					new JobVertexInputGateDeploymentDescriptor(connectionInfoMap);
				vertexInputGateDeploymentDescriptor.read(in);
				allToAllInputGates.add(vertexInputGateDeploymentDescriptor);
			}
		}
	}
}
