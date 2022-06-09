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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;

/**
 * Partition descriptor for task.
 */
public class JobTaskPartitionDescriptor implements DeploymentReadableWritable, Serializable {
	private static final long serialVersionUID = 1L;

	private IntermediateDataSetID resultId;

	private int numberOfSubpartitions;

	public JobTaskPartitionDescriptor() {
	}

	public JobTaskPartitionDescriptor(IntermediateDataSetID resultId, int numberOfSubpartitions) {
		this.resultId = resultId;
		this.numberOfSubpartitions = numberOfSubpartitions;
	}

	public IntermediateDataSetID getResultId() {
		return resultId;
	}

	public int getNumberOfSubpartitions() {
		return numberOfSubpartitions;
	}

	@Override
	public void write(DataOutputView out) throws Exception {
		out.writeLong(resultId.getLowerPart());
		out.writeLong(resultId.getUpperPart());
		out.writeInt(numberOfSubpartitions);
	}

	@Override
	public void read(DataInputView in) throws Exception {
		resultId = new IntermediateDataSetID(in.readLong(), in.readLong());
		numberOfSubpartitions = in.readInt();
	}
}
