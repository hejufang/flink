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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}.
 */
public class DefaultExecutionTopology implements SchedulingTopology {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionTopology.class);

	private final boolean containsCoLocationConstraints;

	private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;

	private final List<DefaultExecutionVertex> executionVerticesList;

	private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

	private final Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex;

	private final List<DefaultSchedulingPipelinedRegion> pipelinedRegions;

	private final EdgeManager edgeManager;

	public DefaultExecutionTopology(ExecutionGraph graph) {
		checkNotNull(graph, "execution graph can not be null");

		this.containsCoLocationConstraints = graph.getAllVertices().values().stream()
			.map(ExecutionJobVertex::getCoLocationGroup)
			.anyMatch(Objects::nonNull);

		this.edgeManager = graph.getEdgeManager();

		this.executionVerticesById = new HashMap<>();
		this.executionVerticesList = new ArrayList<>(graph.getTotalNumberOfVertices());
		Map<IntermediateResultPartitionID, DefaultResultPartition> tmpResultPartitionsById = new HashMap<>();

		for (ExecutionVertex vertex : graph.getAllExecutionVertices()) {
			List<DefaultResultPartition> producedPartitions = generateProducedSchedulingResultPartition(vertex.getProducedPartitions(), getEdgeManager().getAllPartitionConsumers(), executionVerticesById);

			producedPartitions.forEach(partition -> tmpResultPartitionsById.put(partition.getId(), partition));

			DefaultExecutionVertex schedulingVertex = generateSchedulingExecutionVertex(vertex, producedPartitions, getEdgeManager().getVertexConsumedPartitions(vertex.getID()), tmpResultPartitionsById);
			this.executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
			this.executionVerticesList.add(schedulingVertex);
		}
		this.resultPartitionsById = tmpResultPartitionsById;

		this.pipelinedRegionsByVertex = new HashMap<>();
		this.pipelinedRegions = new ArrayList<>();
		initializePipelinedRegions(graph);
	}

	private void initializePipelinedRegions(ExecutionGraph executionGraph) {
		final long buildRegionsStartTime = System.nanoTime();

		final Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions = DefaultSchedulingPipelinedRegionComputeUtil.computePipelinedRegions(executionVerticesList, executionGraph);
		for (Set<? extends SchedulingExecutionVertex> rawPipelinedRegion : rawPipelinedRegions) {
			//noinspection unchecked
			final DefaultSchedulingPipelinedRegion pipelinedRegion = new DefaultSchedulingPipelinedRegion((Set<DefaultExecutionVertex>) rawPipelinedRegion, resultPartitionsById);
			pipelinedRegions.add(pipelinedRegion);

			for (SchedulingExecutionVertex executionVertex : rawPipelinedRegion) {
				pipelinedRegionsByVertex.put(executionVertex.getId(), pipelinedRegion);
			}
		}

		final long buildRegionsDuration = (System.nanoTime() - buildRegionsStartTime) / 1_000_000;
		LOG.info("Built {} pipelined regions in {} ms", pipelinedRegions.size(), buildRegionsDuration);
	}

	public EdgeManager getEdgeManager() {
		return edgeManager;
	}

	@Override
	public Iterable<DefaultExecutionVertex> getVertices() {
		return executionVerticesList;
	}

	@Override
	public boolean containsCoLocationConstraints() {
		return containsCoLocationConstraints;
	}

	@Override
	public DefaultExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
		final DefaultExecutionVertex executionVertex = executionVerticesById.get(executionVertexId);
		if (executionVertex == null) {
			throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
		}
		return executionVertex;
	}

	@Override
	public DefaultResultPartition getResultPartition(final IntermediateResultPartitionID intermediateResultPartitionId) {
		final DefaultResultPartition resultPartition = resultPartitionsById.get(intermediateResultPartitionId);
		if (resultPartition == null) {
			throw new IllegalArgumentException("can not find partition: " + intermediateResultPartitionId);
		}
		return resultPartition;
	}

	@Override
	public Iterable<DefaultSchedulingPipelinedRegion> getAllPipelinedRegions() {
		return Collections.unmodifiableCollection(pipelinedRegions);
	}

	@Override
	public DefaultSchedulingPipelinedRegion getPipelinedRegionOfVertex(final ExecutionVertexID vertexId) {
		final DefaultSchedulingPipelinedRegion pipelinedRegion = pipelinedRegionsByVertex.get(vertexId);
		if (pipelinedRegion == null) {
			throw new IllegalArgumentException("Unknown execution vertex " + vertexId);
		}
		return pipelinedRegion;
	}

	private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedIntermediatePartitions,
		Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers,
		Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById) {

		List<DefaultResultPartition> producedSchedulingPartitions = new ArrayList<>(producedIntermediatePartitions.size());

		producedIntermediatePartitions.values().forEach(
			irp -> producedSchedulingPartitions.add(
				new DefaultResultPartition(
					irp.getPartitionId(),
					irp.getIntermediateResult().getId(),
					irp.getResultType(),
					() -> irp.isConsumable() ? ResultPartitionState.CONSUMABLE : ResultPartitionState.CREATED,
					partitionConsumers.getOrDefault(irp.getPartitionId(), Collections.emptyList()),
					executionVerticesById)));

		return producedSchedulingPartitions;
	}

	private static DefaultExecutionVertex generateSchedulingExecutionVertex(
		ExecutionVertex vertex,
		List<DefaultResultPartition> producedPartitions,
		List<ConsumedPartitionGroup> consumedPartitionIds,
		Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById) {

		DefaultExecutionVertex schedulingVertex = new DefaultExecutionVertex(
			vertex.getID(),
			producedPartitions,
			() -> vertex.getExecutionState(),
			vertex.getInputDependencyConstraint(),
			consumedPartitionIds,
			resultPartitionsById);

		producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));

		return schedulingVertex;
	}
}
