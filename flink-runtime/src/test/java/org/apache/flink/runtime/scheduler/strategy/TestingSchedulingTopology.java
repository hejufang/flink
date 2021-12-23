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
import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A simple scheduling topology for testing purposes.
 */
public class TestingSchedulingTopology implements SchedulingTopology {

	// Use linked map here to so we can get the values in inserted order
	private final Map<ExecutionVertexID, TestingSchedulingExecutionVertex> schedulingExecutionVertices = new LinkedHashMap<>();

	private final Map<IntermediateResultPartitionID, TestingSchedulingResultPartition> schedulingResultPartitions = new HashMap<>();

	private Map<ExecutionVertexID, TestingSchedulingPipelinedRegion> vertexRegions;

	private boolean containsCoLocationConstraints;

	@Override
	public Iterable<TestingSchedulingExecutionVertex> getVertices() {
		return Collections.unmodifiableCollection(schedulingExecutionVertices.values());
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}

	public void setContainsCoLocationConstraints(final boolean containsCoLocationConstraints) {
		this.containsCoLocationConstraints = containsCoLocationConstraints;
	}

	@Override
	public TestingSchedulingExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
		final TestingSchedulingExecutionVertex executionVertex = schedulingExecutionVertices.get(executionVertexId);
		if (executionVertex == null) {
			throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
		}
		return executionVertex;
	}

	@Override
	public TestingSchedulingResultPartition getResultPartition(final IntermediateResultPartitionID intermediateResultPartitionId) {
		final TestingSchedulingResultPartition resultPartition = schedulingResultPartitions.get(intermediateResultPartitionId);
		if (resultPartition == null) {
			throw new IllegalArgumentException("can not find partition: " + intermediateResultPartitionId);
		}
		return resultPartition;
	}

	@Override
	public Iterable<SchedulingPipelinedRegion> getAllPipelinedRegions() {
		return new HashSet<>(getVertexRegions().values());
	}

	@Override
	public SchedulingPipelinedRegion getPipelinedRegionOfVertex(ExecutionVertexID vertexId) {
		return getVertexRegions().get(vertexId);
	}

	private Map<ExecutionVertexID, TestingSchedulingPipelinedRegion> getVertexRegions() {
		if (vertexRegions == null) {
			generatePipelinedRegions();
		}
		return vertexRegions;
	}

	private void generatePipelinedRegions() {
		vertexRegions = new HashMap<>();

		final Set<Set<SchedulingExecutionVertex>> rawRegions =
			PipelinedRegionComputeUtil.computePipelinedRegions(this);

		for (Set<SchedulingExecutionVertex> rawRegion : rawRegions) {
			final Set<TestingSchedulingExecutionVertex> vertices = rawRegion.stream()
				.map(vertex -> schedulingExecutionVertices.get(vertex.getId()))
				.collect(Collectors.toSet());
			final TestingSchedulingPipelinedRegion region = new TestingSchedulingPipelinedRegion(vertices);
			for (TestingSchedulingExecutionVertex vertex : vertices) {
				vertexRegions.put(vertex.getId(), region);
			}
		}
	}

	private void resetPipelinedRegions() {
		vertexRegions = null;
	}

	void addSchedulingExecutionVertex(TestingSchedulingExecutionVertex schedulingExecutionVertex) {
		checkState(!schedulingExecutionVertices.containsKey(schedulingExecutionVertex.getId()));

		schedulingExecutionVertices.put(schedulingExecutionVertex.getId(), schedulingExecutionVertex);
		updateVertexResultPartitions(schedulingExecutionVertex);
		resetPipelinedRegions();
	}

	private void updateVertexResultPartitions(final TestingSchedulingExecutionVertex schedulingExecutionVertex) {
		addSchedulingResultPartitions(schedulingExecutionVertex.getConsumedResults());
		addSchedulingResultPartitions(schedulingExecutionVertex.getProducedResults());
	}

	private void addSchedulingResultPartitions(final Iterable<TestingSchedulingResultPartition> resultPartitions) {
		for (TestingSchedulingResultPartition schedulingResultPartition : resultPartitions) {
			schedulingResultPartitions.put(schedulingResultPartition.getId(), schedulingResultPartition);
		}
	}

	void addSchedulingExecutionVertices(List<TestingSchedulingExecutionVertex> vertices) {
		for (TestingSchedulingExecutionVertex vertex : vertices) {
			addSchedulingExecutionVertex(vertex);
		}
	}

	public SchedulingExecutionVerticesBuilder addExecutionVertices() {
		return new SchedulingExecutionVerticesBuilder();
	}

	public TestingSchedulingExecutionVertex newExecutionVertex() {
		return newExecutionVertex(new JobVertexID(), 0);
	}

	public TestingSchedulingExecutionVertex newExecutionVertex(final JobVertexID jobVertexId, final int subtaskIndex) {
		final TestingSchedulingExecutionVertex newVertex = new TestingSchedulingExecutionVertex(jobVertexId, subtaskIndex);
		addSchedulingExecutionVertex(newVertex);
		return newVertex;
	}

	public TestingSchedulingTopology connect(
			final TestingSchedulingExecutionVertex producer,
			final TestingSchedulingExecutionVertex consumer) {

		return connect(producer, consumer, ResultPartitionType.PIPELINED);
	}

	public TestingSchedulingTopology connect(
			TestingSchedulingExecutionVertex producer,
			TestingSchedulingExecutionVertex consumer,
			ResultPartitionType resultPartitionType) {
		return connect(producer, consumer, resultPartitionType, DistributionPattern.POINTWISE);
	}

	public TestingSchedulingTopology connect(
		TestingSchedulingExecutionVertex producer,
		TestingSchedulingExecutionVertex consumer,
		ResultPartitionType resultPartitionType,
		DistributionPattern distributionPattern) {

		final TestingSchedulingResultPartition resultPartition = new TestingSchedulingResultPartition.Builder()
			.withResultPartitionType(resultPartitionType)
			.build();

		resultPartition.addConsumer(
			new ConsumerVertexGroup(consumer.getId()), Collections.singletonList(consumer));
		resultPartition.setProducer(producer);

		producer.addProducedPartition(resultPartition);
		consumer.addConsumedPartition(
			new ConsumedPartitionGroup(resultPartition.getId(), distributionPattern, producer.getId().getJobVertexId()), Collections.singletonList(resultPartition));

		updateVertexResultPartitions(producer);
		updateVertexResultPartitions(consumer);

		resetPipelinedRegions();

		return this;
	}

	public TestingSchedulingTopology connect(
			List<TestingSchedulingExecutionVertex> producers,
			TestingSchedulingExecutionVertex consumer,
			ResultPartitionType resultPartitionType,
			DistributionPattern distributionPattern) {
		return connect(
				producers,
				Collections.singletonList(consumer),
				resultPartitionType,
				distributionPattern);
	}

	public TestingSchedulingTopology connect(
			TestingSchedulingExecutionVertex producer,
			List<TestingSchedulingExecutionVertex> consumers,
			ResultPartitionType resultPartitionType,
			DistributionPattern distributionPattern) {
		return connect(
				Collections.singletonList(producer),
				consumers,
				resultPartitionType,
				distributionPattern);
	}

	public TestingSchedulingTopology connect(
			List<TestingSchedulingExecutionVertex> producers,
			List<TestingSchedulingExecutionVertex> consumers,
			ResultPartitionType resultPartitionType,
			DistributionPattern distributionPattern) {

		if (distributionPattern.equals(DistributionPattern.POINTWISE) && producers.size() > 1 && consumers.size() > 1) {
			throw new IllegalStateException("producers and consumers can not both large than 1 in pointwise mode.");
		}
		ConsumerVertexGroup consumerVertexGroup = new ConsumerVertexGroup(consumers.stream().map(TestingSchedulingExecutionVertex::getId).collect(Collectors.toList()));
		List<TestingSchedulingResultPartition> partitions = new ArrayList<>();

		JobVertexID producerJobVertexId = null;
		for (TestingSchedulingExecutionVertex producer : producers) {
			if (producerJobVertexId == null) {
				producerJobVertexId = producer.getId().getJobVertexId();
			} else {
				if (!producerJobVertexId.equals(producer.getId().getJobVertexId())) {
					throw new IllegalArgumentException("producers should belong to same JobVertex.");
				}
			}
			final TestingSchedulingResultPartition resultPartition = new TestingSchedulingResultPartition.Builder()
					.withResultPartitionType(resultPartitionType)
					.build();

			resultPartition.addConsumer(consumerVertexGroup, consumers);
			resultPartition.setProducer(producer);

			producer.addProducedPartition(resultPartition);
			partitions.add(resultPartition);

			updateVertexResultPartitions(producer);
		}

		ConsumedPartitionGroup consumedPartitionGroup = new ConsumedPartitionGroup(
				partitions.stream().map(TestingSchedulingResultPartition::getId).collect(Collectors.toList()),
				distributionPattern,
				producerJobVertexId);

		for (TestingSchedulingExecutionVertex consumer : consumers) {
			consumer.addConsumedPartition(consumedPartitionGroup, partitions);
			updateVertexResultPartitions(consumer);
		}

		resetPipelinedRegions();
		return this;
	}

	public ProducerConsumerConnectionBuilder connectPointwise(
		final List<TestingSchedulingExecutionVertex> producers,
		final List<TestingSchedulingExecutionVertex> consumers) {

		return new ProducerConsumerPointwiseConnectionBuilder(producers, consumers);
	}

	public ProducerConsumerConnectionBuilder connectAllToAll(
		final List<TestingSchedulingExecutionVertex> producers,
		final List<TestingSchedulingExecutionVertex> consumers) {

		return new ProducerConsumerAllToAllConnectionBuilder(producers, consumers);
	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition}.
	 */
	public abstract class ProducerConsumerConnectionBuilder {

		protected final List<TestingSchedulingExecutionVertex> producers;

		protected final List<TestingSchedulingExecutionVertex> consumers;

		protected ResultPartitionType resultPartitionType = ResultPartitionType.BLOCKING;

		protected ResultPartitionState resultPartitionState = ResultPartitionState.CONSUMABLE;

		protected IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();

		protected ProducerConsumerConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			this.producers = producers;
			this.consumers = consumers;
		}

		public ProducerConsumerConnectionBuilder withResultPartitionType(final ResultPartitionType resultPartitionType) {
			this.resultPartitionType = resultPartitionType;
			return this;
		}

		public ProducerConsumerConnectionBuilder withResultPartitionState(final ResultPartitionState state) {
			this.resultPartitionState = state;
			return this;
		}

		public ProducerConsumerConnectionBuilder withIntermediateDataSetID(final IntermediateDataSetID intermediateDataSetID) {
			this.intermediateDataSetID = intermediateDataSetID;
			return this;
		}

		public List<TestingSchedulingResultPartition> finish() {
			final List<TestingSchedulingResultPartition> resultPartitions = connect(intermediateDataSetID);

			producers.stream().forEach(TestingSchedulingTopology.this::updateVertexResultPartitions);
			consumers.stream().forEach(TestingSchedulingTopology.this::updateVertexResultPartitions);

			return resultPartitions;
		}

		TestingSchedulingResultPartition.Builder initTestingSchedulingResultPartitionBuilder() {
			return new TestingSchedulingResultPartition.Builder()
				.withResultPartitionType(resultPartitionType);
		}

		protected abstract List<TestingSchedulingResultPartition> connect(IntermediateDataSetID intermediateDataSetID);

	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition} of {@link DistributionPattern#POINTWISE}.
	 */
	private class ProducerConsumerPointwiseConnectionBuilder extends ProducerConsumerConnectionBuilder {

		private ProducerConsumerPointwiseConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			super(producers, consumers);
			// currently we only support one to one
			checkState(producers.size() == consumers.size());
		}

		@Override
		protected List<TestingSchedulingResultPartition> connect(IntermediateDataSetID intermediateDataSetID) {
			final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();

			for (int idx = 0; idx < producers.size(); idx++) {
				final TestingSchedulingExecutionVertex producer = producers.get(idx);
				final TestingSchedulingExecutionVertex consumer = consumers.get(idx);

				final TestingSchedulingResultPartition resultPartition = initTestingSchedulingResultPartitionBuilder()
					.withIntermediateDataSetID(intermediateDataSetID)
					.withResultPartitionState(resultPartitionState)
					.withPartitionNum(idx)
					.build();
				resultPartition.setProducer(producer);
				producer.addProducedPartition(resultPartition);
				consumer.addConsumedPartition(
					new ConsumedPartitionGroup(resultPartition.getId(), DistributionPattern.POINTWISE, producer.getId().getJobVertexId()), Collections.singletonList(resultPartition));
				resultPartition.addConsumer(
					new ConsumerVertexGroup(consumer.getId()), Collections.singletonList(consumer));
				resultPartitions.add(resultPartition);
			}

			return resultPartitions;
		}
	}

	/**
	 * Builder for {@link TestingSchedulingResultPartition} of {@link DistributionPattern#ALL_TO_ALL}.
	 */
	private class ProducerConsumerAllToAllConnectionBuilder extends ProducerConsumerConnectionBuilder {

		private ProducerConsumerAllToAllConnectionBuilder(
			final List<TestingSchedulingExecutionVertex> producers,
			final List<TestingSchedulingExecutionVertex> consumers) {
			super(producers, consumers);
		}

		@Override
		protected List<TestingSchedulingResultPartition> connect(IntermediateDataSetID intermediateDataSetID) {
			final List<TestingSchedulingResultPartition> resultPartitions = new ArrayList<>();

			int idx = 0;
			JobVertexID producerVertexId = producers.get(0).getId().getJobVertexId();

			for (TestingSchedulingExecutionVertex producer : producers) {
				if (!producer.getId().getJobVertexId().equals(producerVertexId)) {
					throw new IllegalArgumentException("JobVertexId of producers not same.");
				}
				final TestingSchedulingResultPartition resultPartition = initTestingSchedulingResultPartitionBuilder()
					.withIntermediateDataSetID(intermediateDataSetID)
					.withResultPartitionState(resultPartitionState)
					.withPartitionNum(idx)
					.build();
				resultPartition.setProducer(producer);
				producer.addProducedPartition(resultPartition);
				resultPartitions.add(resultPartition);
				idx++;
			}

			ConsumedPartitionGroup consumedPartitionGroup = new ConsumedPartitionGroup(
					resultPartitions.stream().map(TestingSchedulingResultPartition::getId).collect(Collectors.toList()),
					DistributionPattern.ALL_TO_ALL, producerVertexId);

			for (TestingSchedulingExecutionVertex consumer : consumers) {
				consumer.addConsumedPartition(consumedPartitionGroup, resultPartitions);
			}

			ConsumerVertexGroup consumerVertexGroup = new ConsumerVertexGroup(consumers.stream().map(TestingSchedulingExecutionVertex::getId).collect(Collectors.toList()));

			for (TestingSchedulingResultPartition partition : resultPartitions) {
				partition.addConsumer(consumerVertexGroup, consumers);
			}

			return resultPartitions;
		}
	}

	/**
	 * Builder for {@link TestingSchedulingExecutionVertex}.
	 */
	public class SchedulingExecutionVerticesBuilder {

		private JobVertexID jobVertexId = new JobVertexID();

		private int parallelism = 1;

		private InputDependencyConstraint inputDependencyConstraint = InputDependencyConstraint.ANY;

		public SchedulingExecutionVerticesBuilder withJobVertexID(final JobVertexID jobVertexId) {
			this.jobVertexId = jobVertexId;
			return this;
		}

		public SchedulingExecutionVerticesBuilder withParallelism(final int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public SchedulingExecutionVerticesBuilder withInputDependencyConstraint(final InputDependencyConstraint inputDependencyConstraint) {
			this.inputDependencyConstraint = inputDependencyConstraint;
			return this;
		}

		public List<TestingSchedulingExecutionVertex> finish() {
			final List<TestingSchedulingExecutionVertex> vertices = new ArrayList<>();
			for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
				vertices.add(new TestingSchedulingExecutionVertex(jobVertexId, subtaskIndex, inputDependencyConstraint));
			}

			TestingSchedulingTopology.this.addSchedulingExecutionVertices(vertices);

			return vertices;
		}
	}
}
