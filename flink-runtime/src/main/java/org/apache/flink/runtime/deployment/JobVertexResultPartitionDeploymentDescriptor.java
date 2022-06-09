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

import java.io.Serializable;

/**
 * A job vertex result partition descriptor contains result partition information shared by tasks.
 */
public class JobVertexResultPartitionDeploymentDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private JobVertexPartitionDescriptor jobVertexPartitionDescriptor;

	private int maxParallelism;

	private boolean sendScheduleOrUpdateConsumersMessage;

	public JobVertexResultPartitionDeploymentDescriptor() {
	}

	public JobVertexResultPartitionDeploymentDescriptor(
			JobVertexPartitionDescriptor jobVertexPartitionDescriptor,
			int maxParallelism,
			boolean sendScheduleOrUpdateConsumersMessage) {
		this.jobVertexPartitionDescriptor = jobVertexPartitionDescriptor;
		this.maxParallelism = maxParallelism;
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
	}

	public JobVertexPartitionDescriptor getJobVertexPartitionDescriptor() {
		return jobVertexPartitionDescriptor;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public boolean isSendScheduleOrUpdateConsumersMessage() {
		return sendScheduleOrUpdateConsumersMessage;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		jobVertexPartitionDescriptor.write(out);
		out.writeInt(maxParallelism);
		out.writeBoolean(sendScheduleOrUpdateConsumersMessage);
	}

	@Override
	public void read(DataInputView in) throws Exception {
		jobVertexPartitionDescriptor = new JobVertexPartitionDescriptor();
		jobVertexPartitionDescriptor.read(in);
		maxParallelism = in.readInt();
		sendScheduleOrUpdateConsumersMessage = in.readBoolean();
	}
}
