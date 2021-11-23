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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Utilities for building {@link EdgeManager}.
 */
public class EdgeManagerBuildUtil {

	private static final AtomicInteger shuffleIdGenerator = new AtomicInteger(0);

	public static void registerToExecutionEdgeManager(
		ExecutionVertex[] taskVertices,
		IntermediateResult ires,
		int inputNumber,
		DistributionPattern distributionPattern) {

		switch (distributionPattern) {
			case POINTWISE:
				connectPointwise(taskVertices, ires, inputNumber);
				break;
			case ALL_TO_ALL:
				connectAllToAll(taskVertices, ires, inputNumber);
				break;
			default:
				throw new RuntimeException("Unrecognized distribution pattern.");
		}
	}

	private static void connectAllToAll(
		ExecutionVertex[] taskVertices, IntermediateResult ires, int inputNumber) {

		// ALL-to-ALL group share the same shuffle id
		final int shuffleId = shuffleIdGenerator.addAndGet(1);
		final ShuffleInfo shuffleInfo = new ShuffleInfo(
			shuffleId,
			0,
			ires.getPartitions().length - 1,
			0,
			taskVertices.length - 1);

		ConsumedPartitionGroup consumedPartitions =
			new ConsumedPartitionGroup(
				Arrays.stream(ires.getPartitions())
					.map(IntermediateResultPartition::getPartitionId)
					.collect(Collectors.toList()),
				DistributionPattern.ALL_TO_ALL,
				ires.getProducer().getJobVertexId());
		for (ExecutionVertex ev : taskVertices) {
			ev.setConsumedPartitions(consumedPartitions, inputNumber);
		}

		ConsumerVertexGroup vertices =
			new ConsumerVertexGroup(
				Arrays.stream(taskVertices)
					.map(ExecutionVertex::getID)
					.collect(Collectors.toList()));
		for (IntermediateResultPartition partition : ires.getPartitions()) {
			partition.setConsumers(vertices);
			partition.setShuffleInfo(shuffleInfo);
		}
	}

	private static void connectPointwise(
		ExecutionVertex[] taskVertices, IntermediateResult ires, int inputNumber) {

		final int sourceCount = ires.getPartitions().length;
		final int targetCount = taskVertices.length;

		if (sourceCount == targetCount) {
			for (int i = 0; i < sourceCount; i++) {

				// every upstream-downstream pair uses same shuffle id
				final int shuffleId = shuffleIdGenerator.addAndGet(1);

				ExecutionVertex executionVertex = taskVertices[i];
				IntermediateResultPartition partition = ires.getPartitions()[i];

				ConsumerVertexGroup consumerVertexGroup =
					new ConsumerVertexGroup(executionVertex.getID());
				partition.setConsumers(consumerVertexGroup);
				partition.setShuffleInfo(new ShuffleInfo(shuffleId, i, i, i, i));

				ConsumedPartitionGroup consumedPartitionGroup =
					new ConsumedPartitionGroup(partition.getPartitionId(), DistributionPattern.POINTWISE, ires.getProducer().getJobVertexId());
				executionVertex.setConsumedPartitions(consumedPartitionGroup, inputNumber);
			}
		} else if (sourceCount > targetCount) {
			for (int index = 0; index < targetCount; index++) {

				// every upstream(p>1)-downstream(p=1) pair uses same shuffle id
				final int shuffleId = shuffleIdGenerator.addAndGet(1);

				ExecutionVertex executionVertex = taskVertices[index];
				ConsumerVertexGroup consumerVertexGroup =
					new ConsumerVertexGroup(executionVertex.getID());

				List<IntermediateResultPartitionID> consumedPartitions =
					new ArrayList<>(sourceCount / targetCount + 1);

				if (sourceCount % targetCount == 0) {
					int factor = sourceCount / targetCount;
					int start = index * factor;

					final ShuffleInfo shuffleInfo = new ShuffleInfo(shuffleId, start, start + factor - 1, index, index);
					for (int i = 0; i < factor; i++) {
						IntermediateResultPartition partition = ires.getPartitions()[start + i];
						partition.setConsumers(consumerVertexGroup);
						partition.setShuffleInfo(shuffleInfo);

						consumedPartitions.add(partition.getPartitionId());
					}
				} else {
					float factor = ((float) sourceCount) / targetCount;
					int start = (int) (index * factor);
					int end =
						(index == targetCount - 1) ? sourceCount : (int) ((index + 1) * factor);

					final ShuffleInfo shuffleInfo = new ShuffleInfo(shuffleId, start, end - 1, index, index);
					for (int i = 0; i < end - start; i++) {
						IntermediateResultPartition partition = ires.getPartitions()[start + i];
						partition.setConsumers(consumerVertexGroup);
						partition.setShuffleInfo(shuffleInfo);

						consumedPartitions.add(partition.getPartitionId());
					}
				}

				ConsumedPartitionGroup consumedPartitionGroup =
					new ConsumedPartitionGroup(consumedPartitions, DistributionPattern.POINTWISE, ires.getProducer().getJobVertexId());
				executionVertex.setConsumedPartitions(consumedPartitionGroup, inputNumber);
			}
		} else {
			for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

				// every upstream(p=1)-downstream(p>1) pair uses same shuffle id
				final int shuffleId = shuffleIdGenerator.addAndGet(1);

				IntermediateResultPartition partition = ires.getPartitions()[partitionNum];
				ConsumedPartitionGroup consumerPartitionGroup =
					new ConsumedPartitionGroup(partition.getPartitionId(), DistributionPattern.POINTWISE, ires.getProducer().getJobVertexId());

				List<ExecutionVertexID> consumers = new ArrayList<>(targetCount / sourceCount + 1);

				if (targetCount % sourceCount == 0) {
					int factor = targetCount / sourceCount;
					int start = partitionNum * factor;
					for (int i = 0; i < factor; i++) {
						ExecutionVertex executionVertex = taskVertices[start + i];
						executionVertex.setConsumedPartitions(consumerPartitionGroup, inputNumber);

						consumers.add(executionVertex.getID());
					}
					partition.setShuffleInfo(new ShuffleInfo(shuffleId, partitionNum, partitionNum, start, start + factor - 1));
				} else {
					float factor = ((float) targetCount) / sourceCount;
					int mirrorPartitionNumber = sourceCount - 1 - partitionNum;
					int start = (int) (mirrorPartitionNumber * factor);
					int end =
						(mirrorPartitionNumber == sourceCount - 1)
							? targetCount
							: (int) ((mirrorPartitionNumber + 1) * factor);

					for (int i = 0; i < end - start; i++) {
						int mirrorVertexSubTaskIndex = start + i;
						int vertexSubtaskIndex = targetCount - 1 - mirrorVertexSubTaskIndex;

						ExecutionVertex executionVertex = taskVertices[vertexSubtaskIndex];
						executionVertex.setConsumedPartitions(consumerPartitionGroup, inputNumber);

						consumers.add(executionVertex.getID());
					}
					partition.setShuffleInfo(new ShuffleInfo(shuffleId, partitionNum, partitionNum, start, end - 1));
				}

				ConsumerVertexGroup consumerVertexGroup = new ConsumerVertexGroup(consumers);
				partition.setConsumers(consumerVertexGroup);
			}
		}
	}

	@VisibleForTesting
	public static void resetShuffleIdGenerator() {
		shuffleIdGenerator.set(0);
	}
}
