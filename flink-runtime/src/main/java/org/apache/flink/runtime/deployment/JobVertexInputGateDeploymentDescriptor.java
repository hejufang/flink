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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;

import java.io.Serializable;
import java.util.Map;

/**
 * Container for all-to-all input gate deployment information.
 */
public class JobVertexInputGateDeploymentDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1;

	private IntermediateDataSetID consumedResultId;

	private ResultPartitionType consumedPartitionType;

	private ShuffleDescriptor[] inputChannels;

	private transient Map<ResourceID, String> connectionInfoMap;

	public JobVertexInputGateDeploymentDescriptor(Map<ResourceID, String> connectionInfoMap) {
		this.connectionInfoMap = connectionInfoMap;
	}

	public JobVertexInputGateDeploymentDescriptor(IntermediateDataSetID consumedResultId, ResultPartitionType consumedPartitionType, ShuffleDescriptor[] inputChannels) {
		this.consumedResultId = consumedResultId;
		this.consumedPartitionType = consumedPartitionType;
		this.inputChannels = inputChannels;
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	public ShuffleDescriptor[] getInputChannels() {
		return inputChannels;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(consumedResultId.getLowerPart());
		out.writeLong(consumedResultId.getUpperPart());
		out.writeInt(consumedPartitionType.ordinal());
		out.writeInt(inputChannels.length);
		for (ShuffleDescriptor shuffleDescriptor: inputChannels) {
			if (shuffleDescriptor.isUnknown()) {
				out.writeBoolean(true);
				((UnknownShuffleDescriptor) shuffleDescriptor).write(out);
			} else {
				out.writeBoolean(false);
				((NettyShuffleDescriptor) shuffleDescriptor).write(out);
			}
		}
	}

	@Override
	public void read(DataInputView in) throws Exception {
		consumedResultId = new IntermediateDataSetID(in.readLong(), in.readLong());
		consumedPartitionType = ResultPartitionType.values()[in.readInt()];
		int len = in.readInt();
		inputChannels = new ShuffleDescriptor[len];
		for (int i = 0; i < len; i++) {
			boolean isUnknown = in.readBoolean();
			if (isUnknown) {
				inputChannels[i] = new UnknownShuffleDescriptor();
			} else {
				inputChannels[i] = new NettyShuffleDescriptor(connectionInfoMap);
			}
			inputChannels[i].read(in);
		}
	}
}
