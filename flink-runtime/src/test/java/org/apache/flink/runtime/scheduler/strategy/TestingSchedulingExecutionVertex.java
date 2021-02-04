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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple scheduling execution vertex for testing purposes.
 */
public class TestingSchedulingExecutionVertex implements SchedulingExecutionVertex {

	private final ExecutionVertexID executionVertexId;

	private final List<ConsumedPartitionGroup> consumerPartitions;

	private final Collection<TestingSchedulingResultPartition> producedPartitions;

	private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition> resultPartitionsById;

	private InputDependencyConstraint inputDependencyConstraint;

	public TestingSchedulingExecutionVertex(JobVertexID jobVertexId, int subtaskIndex) {
		this(jobVertexId, subtaskIndex, InputDependencyConstraint.ANY);
	}

	public TestingSchedulingExecutionVertex(JobVertexID jobVertexId, int subtaskIndex,
		InputDependencyConstraint constraint) {

		this(jobVertexId, subtaskIndex, constraint, new ArrayList<>(), new HashMap<>());
	}

	public TestingSchedulingExecutionVertex(
		JobVertexID jobVertexId,
		int subtaskIndex,
		InputDependencyConstraint constraint,
		List<ConsumedPartitionGroup> consumerPartitions,
		Map<IntermediateResultPartitionID, TestingSchedulingResultPartition> resultPartitionsById) {

		this.executionVertexId = new ExecutionVertexID(jobVertexId, subtaskIndex);
		this.inputDependencyConstraint = constraint;
		this.consumerPartitions = checkNotNull(consumerPartitions);
		this.resultPartitionsById = checkNotNull(resultPartitionsById);
		this.producedPartitions = new ArrayList<>();
	}

	@Override
	public ExecutionVertexID getId() {
		return executionVertexId;
	}

	@Override
	public ExecutionState getState() {
		return ExecutionState.CREATED;
	}

	@Override
	public Iterable<TestingSchedulingResultPartition> getConsumedResults() {
		return consumerPartitions.stream()
			.map(ConsumedPartitionGroup::getResultPartitions)
			.flatMap(Collection::stream)
			.map(resultPartitionsById::get)
			.collect(Collectors.toList());
	}

	@Override
	public Iterable<TestingSchedulingResultPartition> getProducedResults() {
		return producedPartitions;
	}

	@Override
	public List<ConsumedPartitionGroup> getGroupedConsumedResults() {
		return consumerPartitions;
	}

	@Override
	public SchedulingResultPartition getResultPartition(IntermediateResultPartitionID id) {
		return resultPartitionsById.get(id);
	}

	@Override
	public InputDependencyConstraint getInputDependencyConstraint() {
		return inputDependencyConstraint;
	}

	void addConsumedPartition(
		ConsumedPartitionGroup consumedResultIdGroup,
		List<TestingSchedulingResultPartition> consumedResults) {
		this.consumerPartitions.add(consumedResultIdGroup);
		for (TestingSchedulingResultPartition partition : consumedResults) {
			this.resultPartitionsById.putIfAbsent(partition.getId(), partition);
		}
	}

	void addProducedPartition(TestingSchedulingResultPartition partition) {
		producedPartitions.add(partition);
	}
}
