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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A job deployment descriptor contains deployment information of task list for one job.
 */
public class JobDeploymentDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private JobID jobId;

	private TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation;

	private List<JobVertexDeploymentDescriptor> vertexDeploymentDescriptorList;

	private transient Map<ResourceID, String> connectionInfoMap;

	private long createTimestamp;

	private long serializeTimestamp;

	private long deserializeTimestamp;

	public JobDeploymentDescriptor() {
	}

	public JobDeploymentDescriptor(
			JobID jobId,
			TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation) {
		this.jobId = jobId;
		this.serializedJobInformation = serializedJobInformation;
		this.vertexDeploymentDescriptorList = new ArrayList<>();
		this.connectionInfoMap = new HashMap<>();
		this.createTimestamp = System.currentTimeMillis();
	}

	public void addJobVertexDeploymentDescriptor(JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor) {
		vertexDeploymentDescriptorList.add(jobVertexDeploymentDescriptor);
	}

	public JobID getJobId() {
		return jobId;
	}

	public List<JobVertexDeploymentDescriptor> getVertexDeploymentDescriptorList() {
		return vertexDeploymentDescriptorList;
	}

	public int getTaskCount() {
		int count = 0;
		for (JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor : vertexDeploymentDescriptorList) {
			count += jobVertexDeploymentDescriptor.getTaskDeploymentDescriptorList().size();
		}
		return count;
	}

	public void appendConnection(Map<ResourceID, String> newConnectionInfo) {
		connectionInfoMap.putAll(newConnectionInfo);
	}

	public SerializedValue<JobInformation> getSerializedJobInformation() {
		if (serializedJobInformation instanceof TaskDeploymentDescriptor.NonOffloaded) {
			TaskDeploymentDescriptor.NonOffloaded<JobInformation> jobInformation =
				(TaskDeploymentDescriptor.NonOffloaded<JobInformation>) serializedJobInformation;
			return jobInformation.serializedValue;
		} else {
			throw new IllegalStateException(
				"Trying to work with offloaded serialized job information.");
		}
	}

	public void loadBigData(@Nullable PermanentBlobService blobService)
			throws IOException, ClassNotFoundException {
		// re-integrate offloaded job info from blob
		// here, if this fails, we need to throw the exception as there is no backup path anymore
		if (serializedJobInformation instanceof TaskDeploymentDescriptor.Offloaded) {
			PermanentBlobKey jobInfoKey = ((TaskDeploymentDescriptor.Offloaded<JobInformation>) serializedJobInformation).serializedValueKey;

			Preconditions.checkNotNull(blobService);

			final File dataFile = blobService.getFile(jobId, jobInfoKey);
			// NOTE: Do not delete the job info BLOB since it may be needed again during recovery.
			//       (it is deleted automatically on the BLOB server and cache when the job
			//       enters a terminal state)
			SerializedValue<JobInformation> serializedValue =
				SerializedValue.fromBytes(FileUtils.readAllBytes(dataFile.toPath()));
			serializedJobInformation = new TaskDeploymentDescriptor.NonOffloaded<>(serializedValue);
		}

		// make sure that the serialized job information fields are filled
		Preconditions.checkNotNull(serializedJobInformation);
	}

	public long getCreateTimestamp() {
		return createTimestamp;
	}

	public long getSerializeTimestamp() {
		return serializeTimestamp;
	}

	public long getDeserializeTimestamp() {
		return deserializeTimestamp;
	}

	public static JobDeploymentDescriptor from(ExecutionGraph executionGraph) {
		return new JobDeploymentDescriptor(
			executionGraph.getJobID(),
			TaskDeploymentDescriptorFactory.getSerializedJobInformation(executionGraph));
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(jobId.getLowerPart());
		out.writeLong(jobId.getUpperPart());
		if (serializedJobInformation instanceof TaskDeploymentDescriptor.NonOffloaded) {
			out.writeBoolean(true);
			TaskDeploymentDescriptor.NonOffloaded<JobInformation> nonOffloaded = (TaskDeploymentDescriptor.NonOffloaded<JobInformation>) serializedJobInformation;
			byte[] serializedValue = nonOffloaded.serializedValue.getByteArray();
			out.writeInt(serializedValue.length);
			out.write(serializedValue);
		} else {
			out.writeBoolean(false);
			TaskDeploymentDescriptor.Offloaded<JobInformation> offloaded =
				(TaskDeploymentDescriptor.Offloaded<JobInformation>) serializedJobInformation;
			PermanentBlobKey jobInfoKey = offloaded.serializedValueKey;
			out.writeInt(jobInfoKey.getHash().length);
			out.write(jobInfoKey.getHash());
			out.writeInt(jobInfoKey.getType().ordinal());
			out.writeLong(jobInfoKey.getRandom().getLowerPart());
			out.writeLong(jobInfoKey.getRandom().getUpperPart());
		}
		out.writeInt(connectionInfoMap.size());
		for (Map.Entry<ResourceID, String> connectionInfo: connectionInfoMap.entrySet()) {
			StringSerializer.INSTANCE.serialize(connectionInfo.getKey().getResourceIdString(), out);
			StringSerializer.INSTANCE.serialize(connectionInfo.getValue(), out);
		}
		out.writeInt(vertexDeploymentDescriptorList.size());
		for (JobVertexDeploymentDescriptor vertexDeploymentDescriptor: vertexDeploymentDescriptorList) {
			vertexDeploymentDescriptor.write(out);
		}
		out.writeLong(createTimestamp);
		out.writeLong(System.currentTimeMillis());
	}

	@Override
	public void read(DataInputView in) throws Exception {
		jobId = new JobID(in.readLong(), in.readLong());
		boolean nonOffloadedFlag = in.readBoolean();
		if (nonOffloadedFlag) {
			int len = in.readInt();
			byte[] serializedValue = new byte[len];
			in.readFully(serializedValue);
			serializedJobInformation = new TaskDeploymentDescriptor.NonOffloaded<>(SerializedValue.fromBytes(serializedValue));
		} else {
			int keyLength = in.readInt();
			byte[] key = new byte[keyLength];
			in.readFully(key);
			BlobKey.BlobType type = BlobKey.BlobType.values()[in.readInt()];
			AbstractID random = new AbstractID(in.readLong(), in.readLong());
			PermanentBlobKey jobInfoKey = new PermanentBlobKey(type, key, random);
			serializedJobInformation = new TaskDeploymentDescriptor.Offloaded<>(jobInfoKey);
		}

		int connectionInfoMapSize = in.readInt();
		connectionInfoMap = new HashMap<>();
		for (int i = 0; i < connectionInfoMapSize; i++) {
			connectionInfoMap.put(
				new ResourceID(StringSerializer.INSTANCE.deserialize(in)),
				StringSerializer.INSTANCE.deserialize(in));
		}

		int vertexDeploymentListSize = in.readInt();
		vertexDeploymentDescriptorList = new ArrayList<>();
		for (int i = 0; i < vertexDeploymentListSize; i++) {
			JobVertexDeploymentDescriptor jvdd = new JobVertexDeploymentDescriptor(connectionInfoMap);
			jvdd.read(in);
			vertexDeploymentDescriptorList.add(jvdd);
		}

		createTimestamp = in.readLong();
		serializeTimestamp = in.readLong();
		deserializeTimestamp = System.currentTimeMillis();
	}
}
