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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.shuffle.ShuffleInfo;

import java.util.List;

public class IntermediateResultPartition {

	private final IntermediateResult totalResult;

	private final ExecutionVertex producer;

	private final IntermediateResultPartitionID partitionId;

	/** Unique ID for a group of upstream-downstream shuffle. */
	private ShuffleInfo shuffleInfo;

	/** Whether this partition has produced some data. */
	private boolean hasDataProduced = false;

	public IntermediateResultPartition(IntermediateResult totalResult, ExecutionVertex producer, int partitionNumber) {
		this.totalResult = totalResult;
		this.producer = producer;
		this.partitionId = new IntermediateResultPartitionID(totalResult.getId(), partitionNumber);
		this.shuffleInfo = null;

		producer.getExecutionGraph().registerResultPartition(partitionId, this);
	}

	public ExecutionVertex getProducer() {
		return producer;
	}

	public int getPartitionNumber() {
		return partitionId.getPartitionNumber();
	}

	public IntermediateResult getIntermediateResult() {
		return totalResult;
	}

	public IntermediateResultPartitionID getPartitionId() {
		return partitionId;
	}

	public ResultPartitionType getResultType() {
		return totalResult.getResultType();
	}

	public List<ConsumerVertexGroup> getConsumers() {
		return getEdgeManager().getPartitionConsumers(partitionId);
	}

	public void markDataProduced() {
		hasDataProduced = true;
		totalResult.notifyPartitionStateChanged();
	}

	public boolean isConsumable() {
		if (getResultType().isPipelined()) {
			return hasDataProduced;
		} else {
			return totalResult.areAllPartitionsFinished();
		}
	}

	void resetForNewExecution() {
		if (getResultType().isBlocking() && hasDataProduced) {
			// A BLOCKING result partition with data produced means it is finished
			// Need to add the running producer count of the result on resetting it
			totalResult.incrementNumberOfRunningProducersAndGetRemaining();
		}
		hasDataProduced = false;
		totalResult.notifyPartitionStateChanged();
	}

	public void setConsumers(ConsumerVertexGroup consumers) {
		producer.getExecutionGraph().getEdgeManager().addPartitionConsumers(partitionId, consumers);
	}

	public void setShuffleInfo(ShuffleInfo shuffleInfo) {
		this.shuffleInfo = shuffleInfo;
	}

	public ShuffleInfo getShuffleInfo() {
		return shuffleInfo;
	}

	EdgeManager getEdgeManager() {
		return producer.getExecutionGraph().getEdgeManager();
	}

	boolean markFinished() {
		// Sanity check that this is only called on blocking partitions.
		if (!getResultType().isBlocking()) {
			throw new IllegalStateException("Tried to mark a non-blocking result partition as finished");
		}

		hasDataProduced = true;
		totalResult.notifyPartitionStateChanged();

		final int refCnt = totalResult.decrementNumberOfRunningProducersAndGetRemaining();

		if (refCnt == 0) {
			return true;
		}
		else if (refCnt  < 0) {
			throw new IllegalStateException("Decremented number of unfinished producers below 0. "
					+ "This is most likely a bug in the execution state/intermediate result "
					+ "partition management.");
		}

		return false;
	}
}
