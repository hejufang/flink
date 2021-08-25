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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Group of consumed {@link IntermediateResultPartitionID}s.
 */
public class ConsumedPartitionGroup {

	private final JobVertexID producerVertexId;
	private final List<IntermediateResultPartitionID> resultPartitions;
	private final DistributionPattern distributionPattern;

	public ConsumedPartitionGroup(List<IntermediateResultPartitionID> resultPartitions, DistributionPattern distributionPattern, JobVertexID jobVertexID) {
		this.resultPartitions = resultPartitions;
		this.distributionPattern = distributionPattern;
		this.producerVertexId = jobVertexID;
	}

	public ConsumedPartitionGroup(IntermediateResultPartitionID resultPartition, DistributionPattern distributionPattern, JobVertexID jobVertexID) {
		this(Collections.singletonList(resultPartition), distributionPattern, jobVertexID);
	}

	public List<IntermediateResultPartitionID> getResultPartitions() {
		return resultPartitions;
	}

	public DistributionPattern getDistributionPattern() {
		return distributionPattern;
	}

	public JobVertexID getProducerVertexId() {
		return producerVertexId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ConsumedPartitionGroup that = (ConsumedPartitionGroup) o;
		return Objects.equals(producerVertexId, that.producerVertexId)
				&& Objects.equals(resultPartitions, that.resultPartitions)
				&& distributionPattern == that.distributionPattern;
	}

	@Override
	public int hashCode() {
		return Objects.hash(producerVertexId, resultPartitions, distributionPattern);
	}
}
