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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;

/**
 * A job vertex partition descriptor contains partition information shared by tasks.
 */
public class JobVertexPartitionDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private IntermediateDataSetID resultId;

	private int totalNumberOfPartitions;

	private ResultPartitionType partitionType;

	private int connectionIndex;

	public JobVertexPartitionDescriptor() {
	}

	public JobVertexPartitionDescriptor(
			IntermediateDataSetID resultId,
			int totalNumberOfPartitions,
			ResultPartitionType partitionType,
			int connectionIndex) {
		this.resultId = resultId;
		this.totalNumberOfPartitions = totalNumberOfPartitions;
		this.partitionType = partitionType;
		this.connectionIndex = connectionIndex;
	}

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public int getTotalNumberOfPartitions() {
		return totalNumberOfPartitions;
	}

	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	public int getConnectionIndex() {
		return connectionIndex;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(resultId.getLowerPart());
		out.writeLong(resultId.getUpperPart());
		out.writeInt(totalNumberOfPartitions);
		out.writeInt(partitionType.ordinal());
		out.writeInt(connectionIndex);
	}

	@Override
	public void read(DataInputView in) throws Exception {
		resultId = new IntermediateDataSetID(in.readLong(), in.readLong());
		totalNumberOfPartitions = in.readInt();
		partitionType = ResultPartitionType.values()[in.readInt()];
		connectionIndex = in.readInt();
	}
}
