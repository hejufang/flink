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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A helper class to separate input gates / result partition by job vertex/task level.
 */
public class JobDeploymentDescriptorHelper {

	/**
	 * A helper function to separate result partition entity of {@link Execution} into job vertex and task level.
	 *
	 * @param execution the execution
	 * @return Entity which contains job vertex and task level result partition entities of the execution
	 */
	public static JobVertexResultPartitionEntity createJobVertexResultPartitionEntity(Execution execution) {
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions = execution.getProducedPartitions();

		Map<IntermediateDataSetID, JobVertexResultPartitionDeploymentDescriptor> jobVertexResultPartitionDeploymentDescriptorMap = new HashMap<>();
		List<JobTaskPartitionDescriptor> taskPartitionDescriptorList = new ArrayList<>();

		for (Map.Entry<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> entry: producedPartitions.entrySet()) {
			IntermediateResultPartitionID intermediateResultPartitionID = entry.getKey();
			ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = entry.getValue();

			IntermediateDataSetID intermediateDataSetID = intermediateResultPartitionID.getIntermediateDataSetID();
			if (!jobVertexResultPartitionDeploymentDescriptorMap.containsKey(intermediateDataSetID)) {
				int connectionIndex = execution
					.getVertex()
					.getProducedPartitions()
					.get(intermediateResultPartitionID)
					.getIntermediateResult()
					.getConnectionIndex();
				JobVertexPartitionDescriptor jobVertexPartitionDescriptor = new JobVertexPartitionDescriptor(
					intermediateDataSetID,
					resultPartitionDeploymentDescriptor.getTotalNumberOfPartitions(),
					resultPartitionDeploymentDescriptor.getPartitionType(),
					connectionIndex);
				JobVertexResultPartitionDeploymentDescriptor jobVertexResultPartitionDeploymentDescriptor =
					new JobVertexResultPartitionDeploymentDescriptor(
						jobVertexPartitionDescriptor,
						resultPartitionDeploymentDescriptor.getMaxParallelism(),
						resultPartitionDeploymentDescriptor.sendScheduleOrUpdateConsumersMessage());
				jobVertexResultPartitionDeploymentDescriptorMap.put(intermediateDataSetID, jobVertexResultPartitionDeploymentDescriptor);

				JobTaskPartitionDescriptor taskPartitionDescriptor = new JobTaskPartitionDescriptor(
					intermediateDataSetID, resultPartitionDeploymentDescriptor.getNumberOfSubpartitions());
				taskPartitionDescriptorList.add(taskPartitionDescriptor);
			}
		}
		return new JobVertexResultPartitionEntity(
			new ArrayList<>(jobVertexResultPartitionDeploymentDescriptorMap.values()),
			taskPartitionDescriptorList);
	}

	/**
	 * A helper function to fetch task level partition entity. This is only called when the job vertex level information
	 * is created and cached in {@link GatewayJobDeployment}.
	 *
	 * @param execution the execution
	 * @return list of job task level result partition entity
	 */
	public static List<JobTaskPartitionDescriptor> createTaskPartitionList(Execution execution) {
		List<JobTaskPartitionDescriptor> taskPartitionDescriptorList = new ArrayList<>();

		for (Map.Entry<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> entry: execution.getProducedPartitions().entrySet()) {
			IntermediateResultPartitionID intermediateResultPartitionID = entry.getKey();
			ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = entry.getValue();
			IntermediateDataSetID intermediateDataSetID = intermediateResultPartitionID.getIntermediateDataSetID();

			JobTaskPartitionDescriptor taskPartitionDescriptor = new JobTaskPartitionDescriptor(
				intermediateDataSetID, resultPartitionDeploymentDescriptor.getNumberOfSubpartitions());
			taskPartitionDescriptorList.add(taskPartitionDescriptor);
		}
		return taskPartitionDescriptorList;
	}

	/**
	 *  A helper function to separate input gate of {@link ExecutionVertex} into job vertex and task level.
	 *
	 * @param executionVertex the executionVertex
	 * @return Entity which contains job vertex and task level input gate entities of the execution vertex
	 */
	public static JobVertexInputGatesEntity createJobVertexInputGatesEntity(ExecutionVertex executionVertex) {
		final ExecutionGraph executionGraph = executionVertex.getExecutionGraph();
		final List<ConsumedPartitionGroup> consumedPartitions = executionVertex.getAllConsumedPartitions();
		final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById = executionGraph.getIntermediateResultPartitionMapping();
		final boolean allowUnknownPartitions = executionGraph.getScheduleMode().allowLazyDeployment() || executionGraph.isRecoverable();

		List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates = new ArrayList<>();
		List<JobTaskInputGateDeploymentDescriptor> pointWiseInputGates = new ArrayList<>();

		for (ConsumedPartitionGroup partitions: consumedPartitions) {
			DistributionPattern distributionPattern = partitions.getDistributionPattern();

			if (distributionPattern == DistributionPattern.ALL_TO_ALL) {
				createJobVertexInputGateDeployment(
					allowUnknownPartitions,
					resultPartitionsById,
					partitions,
					allToAllInputGates);
			} else {
				createTaskInputGateDeployment(
					executionVertex,
					allowUnknownPartitions,
					resultPartitionsById,
					partitions,
					pointWiseInputGates);
			}
		}

		return new JobVertexInputGatesEntity(allToAllInputGates, pointWiseInputGates);
	}

	/**
	 * A helper function to fetch task level input gate entity. This is only called when the job vertex level information
	 * is created and cached in {@link GatewayJobDeployment}.
     *
	 * @param executionVertex the executionVertex
	 * @return list of job task level input gate entity
	 */
	public static List<JobTaskInputGateDeploymentDescriptor> createTaskInputGate(ExecutionVertex executionVertex) {
		ExecutionGraph executionGraph = executionVertex.getExecutionGraph();
		final boolean allowUnknownPartitions = executionGraph.getScheduleMode().allowLazyDeployment() || executionGraph.isRecoverable();
		final List<ConsumedPartitionGroup> consumedPartitionGroupList = executionVertex.getAllConsumedPartitions();
		final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById = executionGraph.getIntermediateResultPartitionMapping();

		List<JobTaskInputGateDeploymentDescriptor> taskInputGateDeploymentList = new ArrayList<>();
		for (ConsumedPartitionGroup partitions : consumedPartitionGroupList) {
			final DistributionPattern distributionPattern = partitions.getDistributionPattern();
			if (distributionPattern == DistributionPattern.POINTWISE) {
				createTaskInputGateDeployment(
					executionVertex,
					allowUnknownPartitions,
					resultPartitionsById,
					partitions,
					taskInputGateDeploymentList);
			}
		}
		return taskInputGateDeploymentList.isEmpty() ? null : taskInputGateDeploymentList;
	}

	public static Map<ResourceID, String> getConnectionInfo(ExecutionVertex executionVertex) {
		final ExecutionGraph executionGraph = executionVertex.getExecutionGraph();
		final List<ConsumedPartitionGroup> consumedPartitions = executionVertex.getAllConsumedPartitions();
		final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById = executionGraph.getIntermediateResultPartitionMapping();
		final boolean allowUnknownPartitions = executionGraph.getScheduleMode().allowLazyDeployment() || executionGraph.isRecoverable();

		Map<ResourceID, String> connectionInfoMap = new HashMap<>();
		for (ConsumedPartitionGroup partitions: consumedPartitions) {
			if (partitions.getResultPartitions().size() > 0) {
				IntermediateResult consumedIntermediateResult = resultPartitionsById
					.get(partitions.getResultPartitions().get(0))
					.getIntermediateResult();

				ShuffleDescriptor[] shuffleDescriptors = getConsumedPartitionShuffleDescriptors(
					consumedIntermediateResult,
					partitions,
					resultPartitionsById,
					allowUnknownPartitions);

				for (ShuffleDescriptor shuffleDescriptor: shuffleDescriptors) {
					if (!shuffleDescriptor.isUnknown()) {
						NettyShuffleDescriptor nettyShuffleDescriptor = (NettyShuffleDescriptor) shuffleDescriptor;
						connectionInfoMap.put(nettyShuffleDescriptor.getProducerLocation(),
							nettyShuffleDescriptor.getConnectionId().getAddress().getAddress().getCanonicalHostName());
					}
				}
			}
		}
		return connectionInfoMap;
	}

	private static void createJobVertexInputGateDeployment(
			boolean allowUnknownPartitions,
			Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById,
			ConsumedPartitionGroup partitions,
			List<JobVertexInputGateDeploymentDescriptor> vertexInputGateDeploymentList) {
		IntermediateResultPartition resultPartition = resultPartitionsById.get(partitions.getResultPartitions().get(0));
		IntermediateResult consumedIntermediateResult = resultPartition.getIntermediateResult();
		IntermediateDataSetID resultId = consumedIntermediateResult.getId();
		ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

		JobVertexInputGateDeploymentDescriptor inputGateDeploymentDescriptor =
			new JobVertexInputGateDeploymentDescriptor(
				resultId,
				partitionType,
				getConsumedPartitionShuffleDescriptors(
					consumedIntermediateResult,
					partitions,
					resultPartitionsById,
					allowUnknownPartitions));

		vertexInputGateDeploymentList.add(inputGateDeploymentDescriptor);
	}

	private static void createTaskInputGateDeployment(
			ExecutionVertex executionVertex,
			boolean allowUnknownPartitions,
			Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById,
			ConsumedPartitionGroup partitions,
			List<JobTaskInputGateDeploymentDescriptor> taskInputGateDeploymentList) {
		IntermediateResultPartition resultPartition = resultPartitionsById.get(partitions.getResultPartitions().get(0));

		int numConsumer = resultPartition.getConsumers().get(0).getVertices().size();

		int queueToRequest = executionVertex.getID().getSubtaskIndex() % numConsumer;
		IntermediateResult consumedIntermediateResult = resultPartition.getIntermediateResult();
		IntermediateDataSetID resultId = consumedIntermediateResult.getId();
		ResultPartitionType partitionType = consumedIntermediateResult.getResultType();
		JobTaskInputGateDeploymentDescriptor inputGateDeploymentDescriptor = new JobTaskInputGateDeploymentDescriptor(
			resultId,
			partitionType,
			queueToRequest,
			getConsumedPartitionShuffleDescriptors(
				consumedIntermediateResult,
				partitions,
				resultPartitionsById,
				allowUnknownPartitions));

		taskInputGateDeploymentList.add(inputGateDeploymentDescriptor);
	}

	private static ShuffleDescriptor[] getConsumedPartitionShuffleDescriptors(
			IntermediateResult intermediateResult,
			ConsumedPartitionGroup consumedPartitionGroup,
			Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById,
			boolean allowUnknownPartitions) {
		if (consumedPartitionGroup.getResultPartitions().size() == 0) {
			return new ShuffleDescriptor[0];
		}
		ShuffleDescriptor[] cachedShuffleDescriptors = intermediateResult.getCachedShuffleDescriptors(consumedPartitionGroup);
		if (cachedShuffleDescriptors != null) {
			return cachedShuffleDescriptors;
		} else {
			final ShuffleDescriptor[] shuffleDescriptors =
				buildConsumedPartitionShuffleDescriptors(consumedPartitionGroup.getResultPartitions(), resultPartitionsById, allowUnknownPartitions);
			intermediateResult.cacheShuffleDescriptors(consumedPartitionGroup, shuffleDescriptors);
			return shuffleDescriptors;
		}
	}

	private static ShuffleDescriptor[] buildConsumedPartitionShuffleDescriptors(
			List<IntermediateResultPartitionID> partitions,
			Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitionsById,
			boolean allowUnknownPartitions) {
		ShuffleDescriptor[] shuffleDescriptors = new ShuffleDescriptor[partitions.size()];
		for (int i = 0; i < partitions.size(); i++) {
			shuffleDescriptors[i] =
				TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(resultPartitionsById.get(partitions.get(i)), allowUnknownPartitions);
		}
		return shuffleDescriptors;
	}

	/**
	 * A helper function to recover original {@link ResultPartitionDeploymentDescriptor} from job vertex and task deployment
	 * structure for task.
	 *
	 * @param jobVertexDeploymentDescriptor stores job vertex level deployment information
	 * @param jobTaskDeploymentDescriptor stores task level deployment information
	 * @return list of result partition of {@link org.apache.flink.runtime.taskmanager.Task}
	 */
	public static List<ResultPartitionDeploymentDescriptor> buildResultPartitionDeploymentList(
			JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor,
			JobTaskDeploymentDescriptor jobTaskDeploymentDescriptor) {
		List<ResultPartitionDeploymentDescriptor> producedPartitions = new ArrayList<>();
		Map<IntermediateDataSetID, JobTaskPartitionDescriptor> taskPartitionLookupHelper = new HashMap<>();
		List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitions = jobVertexDeploymentDescriptor.getVertexResultPartitions();
		for (JobTaskPartitionDescriptor taskPartitionDescriptor: jobTaskDeploymentDescriptor.getTaskPartitionDescriptorList()) {
			taskPartitionLookupHelper.put(taskPartitionDescriptor.getResultId(), taskPartitionDescriptor);
		}

		if (vertexResultPartitions != null) {
			for (JobVertexResultPartitionDeploymentDescriptor vertexResultPartition: vertexResultPartitions) {
				IntermediateDataSetID intermediateDataSetID = vertexResultPartition.getJobVertexPartitionDescriptor().getResultId();
				JobTaskPartitionDescriptor taskPartitionDescriptor = checkNotNull(taskPartitionLookupHelper.get(intermediateDataSetID));
				IntermediateResultPartitionID intermediateResultPartitionID = new IntermediateResultPartitionID(intermediateDataSetID, jobTaskDeploymentDescriptor.getSubtaskIndex());
				PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
					intermediateDataSetID,
					vertexResultPartition.getJobVertexPartitionDescriptor().getTotalNumberOfPartitions(),
					intermediateResultPartitionID,
					vertexResultPartition.getJobVertexPartitionDescriptor().getPartitionType(),
					taskPartitionDescriptor.getNumberOfSubpartitions(),
					vertexResultPartition.getJobVertexPartitionDescriptor().getConnectionIndex());
				ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = new ResultPartitionDeploymentDescriptor(
					partitionDescriptor,
					new UnknownShuffleDescriptor(new ResultPartitionID(intermediateResultPartitionID, jobTaskDeploymentDescriptor.getExecutionAttemptId())),
					vertexResultPartition.getMaxParallelism(),
					vertexResultPartition.isSendScheduleOrUpdateConsumersMessage());
				producedPartitions.add(resultPartitionDeploymentDescriptor);
			}
		}

		return producedPartitions;
	}

	/**
	 * A helper function to recover original {@link InputGateDeploymentDescriptor} from job vertex and task deployment
	 * structure for task.
	 *
	 * @param jobVertexDeploymentDescriptor stores job vertex level deployment information
	 * @param jobTaskDeploymentDescriptor stores task level deployment information
	 * @return list of input gate of {@link org.apache.flink.runtime.taskmanager.Task}
	 */
	public static List<InputGateDeploymentDescriptor> buildInputGateDeploymentList(
			JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor,
			JobTaskDeploymentDescriptor jobTaskDeploymentDescriptor) {
		List<JobVertexInputGateDeploymentDescriptor> vertexInputGateList = jobVertexDeploymentDescriptor.getAllToAllInputGates();
		List<JobTaskInputGateDeploymentDescriptor> taskInputGateList = jobTaskDeploymentDescriptor.getInputGates();
		List<InputGateDeploymentDescriptor> inputGates = new ArrayList<>();
		if (vertexInputGateList != null) {
			for (JobVertexInputGateDeploymentDescriptor jobVertexInputGateDeploymentDescriptor: vertexInputGateList) {
				InputGateDeploymentDescriptor inputGateDeploymentDescriptor = new InputGateDeploymentDescriptor(
					jobVertexInputGateDeploymentDescriptor.getConsumedResultId(),
					jobVertexInputGateDeploymentDescriptor.getConsumedPartitionType(),
					jobTaskDeploymentDescriptor.getSubtaskIndex(),
					jobVertexInputGateDeploymentDescriptor.getInputChannels()
				);
				inputGates.add(inputGateDeploymentDescriptor);
			}
		}
		if (taskInputGateList != null) {
			for (JobTaskInputGateDeploymentDescriptor taskInputGateDeploymentDescriptor : taskInputGateList) {
				InputGateDeploymentDescriptor inputGateDeploymentDescriptor = new InputGateDeploymentDescriptor(
					taskInputGateDeploymentDescriptor.getConsumedResultId(),
					taskInputGateDeploymentDescriptor.getConsumedPartitionType(),
					taskInputGateDeploymentDescriptor.getConsumedSubpartitionIndex(),
					taskInputGateDeploymentDescriptor.getInputChannels()
				);
				inputGates.add(inputGateDeploymentDescriptor);
			}
		}
		return inputGates;
	}

	static class JobVertexResultPartitionEntity {
		private final List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitionDeploymentList;
		private final List<JobTaskPartitionDescriptor> taskPartitionDescriptorList;

		public JobVertexResultPartitionEntity(
				List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitionDeploymentList,
				List<JobTaskPartitionDescriptor> taskPartitionDescriptorList) {
			this.vertexResultPartitionDeploymentList = vertexResultPartitionDeploymentList;
			this.taskPartitionDescriptorList = taskPartitionDescriptorList;
		}

		public List<JobVertexResultPartitionDeploymentDescriptor> getVertexResultPartitionDeploymentList() {
			return vertexResultPartitionDeploymentList;
		}

		public List<JobTaskPartitionDescriptor> getTaskPartitionDescriptorList() {
			return taskPartitionDescriptorList;
		}
	}

	static class JobVertexInputGatesEntity {
		private final List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates;
		private final List<JobTaskInputGateDeploymentDescriptor> pointWiseInputGates;

		public JobVertexInputGatesEntity(
				List<JobVertexInputGateDeploymentDescriptor> allToAllInputGates,
				List<JobTaskInputGateDeploymentDescriptor> pointWiseInputGates) {
			this.allToAllInputGates = allToAllInputGates;
			this.pointWiseInputGates = pointWiseInputGates;
		}

		public List<JobVertexInputGateDeploymentDescriptor> getAllToAllInputGates() {
			return allToAllInputGates;
		}

		public List<JobTaskInputGateDeploymentDescriptor> getPointWiseInputGates() {
			return pointWiseInputGates;
		}
	}
}
