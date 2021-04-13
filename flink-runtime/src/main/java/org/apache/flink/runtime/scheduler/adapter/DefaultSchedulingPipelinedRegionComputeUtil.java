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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.executiongraph.failover.flip1.StronglyConnectedComponentsComputeUtils;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertex;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.mergeRegions;
import static org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil.uniqueRegions;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility for computing {@link SchedulingPipelinedRegion}.
 */
public final class DefaultSchedulingPipelinedRegionComputeUtil {

	public static Set<Set<SchedulingExecutionVertex>> computePipelinedRegions(
			final Iterable<DefaultExecutionVertex> topologicallySortedVertexes,
			ExecutionGraph executionGraph) {
		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion =
				buildRawRegions(topologicallySortedVertexes, executionGraph);
		return mergeRegionsOnCycles(vertexToRegion);
	}

	/**
	 * Build logicalRegions which only container ALL_TO_ALL DistributionPattern.
	 */
	private static Map<JobVertexID, Set<DefaultLogicalVertex>> buildAllToAllPipelinedLogicalRegions(ExecutionGraph executionGraph) {
		Set<Set<DefaultLogicalVertex>> logicalRegions = PipelinedRegionComputeUtil.computePipelinedRegions(executionGraph.getLogicalTopology());
		logicalRegions = logicalRegions.stream().filter(logicalVertices -> {
			for (DefaultLogicalVertex logicalVertex : logicalVertices) {
				ExecutionJobVertex  executionJobVertex = executionGraph.getJobVertex(logicalVertex.getId());
				if (executionJobVertex == null) {
					throw new IllegalStateException("Can not found ExecutionJobVertex for "
							+ logicalVertex.getId() + ", This should be a failover region building bug.");
				}
				for (JobEdge jobEdge : executionJobVertex.getJobVertex().getInputs()) {
					if (jobEdge.getDistributionPattern().equals(DistributionPattern.ALL_TO_ALL)
							&& jobEdge.getSource().getResultType().isPipelined()) {
						return true;
					}
				}
			}
			return false;
		}).collect(Collectors.toSet());

		final Map<JobVertexID, Set<DefaultLogicalVertex>> logicalRegionsMap = new HashMap<>();
		for (Set<DefaultLogicalVertex> logicalVertices : logicalRegions) {
			for (DefaultLogicalVertex logicalVertex : logicalVertices) {
				logicalRegionsMap.put(logicalVertex.getId(), Collections.unmodifiableSet(logicalVertices));
			}
		}
		return logicalRegionsMap;
	}

	private static Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> buildRawRegions(
			final Iterable<? extends DefaultExecutionVertex> topologicallySortedVertexes,
			ExecutionGraph executionGraph) {

		final Map<JobVertexID, Set<DefaultLogicalVertex>> allToAllPipelinedLogicalRegions = buildAllToAllPipelinedLogicalRegions(executionGraph);
		final Map<Set<DefaultLogicalVertex>, Set<SchedulingExecutionVertex>> logicalRegionToExecutionRegion = new HashMap<>();
		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (DefaultExecutionVertex vertex : topologicallySortedVertexes) {
			Set<SchedulingExecutionVertex> currentRegion = new HashSet<>();
			// check task whether in a all-to-all region. if true put this task to one region directly.
			if (allToAllPipelinedLogicalRegions.containsKey(vertex.getId().getJobVertexId())) {
				Set<DefaultLogicalVertex> ejvRegion = allToAllPipelinedLogicalRegions.get(vertex.getId().getJobVertexId());
				if (logicalRegionToExecutionRegion.containsKey(ejvRegion)) {
					currentRegion = logicalRegionToExecutionRegion.get(ejvRegion);
					currentRegion.add(vertex);
				} else {
					logicalRegionToExecutionRegion.put(ejvRegion, currentRegion);
					currentRegion.add(vertex);
					vertexToRegion.put(vertex, currentRegion);
				}
				continue;
			}

			currentRegion.add(vertex);
			vertexToRegion.put(vertex, currentRegion);

			for (ConsumedPartitionGroup consumedResultIds : vertex.getGroupedConsumedResults()) {
				for (IntermediateResultPartitionID consumerId : consumedResultIds.getResultPartitions()) {
					SchedulingResultPartition consumedResult = vertex.getResultPartition(consumerId);
					if (consumedResult.getResultType().isPipelined()) {
						final SchedulingExecutionVertex producerVertex = consumedResult.getProducer();
						final Set<SchedulingExecutionVertex> producerRegion = vertexToRegion.get(producerVertex);

						if (producerRegion == null) {
							throw new IllegalStateException(
								"Producer task "
									+ producerVertex.getId()
									+ " failover region is null while calculating failover region for the consumer task "
									+ vertex.getId()
									+ ". This should be a failover region building bug.");
						}

						// check if it is the same as the producer region, if so skip the merge
						// this check can significantly reduce compute complexity in All-to-All
						// PIPELINED edge case
						if (currentRegion != producerRegion) {
							currentRegion =
								mergeRegions(currentRegion, producerRegion, vertexToRegion);
						}
					}
				}
			}
		}
		return vertexToRegion;
	}

	public static Set<Set<SchedulingExecutionVertex>> mergeRegionsOnCycles(
		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion) {

		final List<Set<SchedulingExecutionVertex>> regionList =
			new ArrayList<>(uniqueRegions(vertexToRegion));
		final List<List<Integer>> outEdges = buildOutEdgesDesc(vertexToRegion, regionList);
		final Set<Set<Integer>> sccs =
			StronglyConnectedComponentsComputeUtils.computeStronglyConnectedComponents(
				outEdges.size(), outEdges);

		final Set<Set<SchedulingExecutionVertex>> mergedRegions =
			Collections.newSetFromMap(new IdentityHashMap<>());
		for (Set<Integer> scc : sccs) {
			checkState(scc.size() > 0);

			Set<SchedulingExecutionVertex> mergedRegion = new HashSet<>();
			for (int regionIndex : scc) {
				mergedRegion =
					mergeRegions(mergedRegion, regionList.get(regionIndex), vertexToRegion);
			}
			mergedRegions.add(mergedRegion);
		}

		return mergedRegions;
	}

	private static List<List<Integer>> buildOutEdgesDesc(
		final Map<SchedulingExecutionVertex, Set<SchedulingExecutionVertex>> vertexToRegion,
		final List<Set<SchedulingExecutionVertex>> regionList) {

		final Map<Set<SchedulingExecutionVertex>, Integer> regionIndices = new IdentityHashMap<>();
		for (int i = 0; i < regionList.size(); i++) {
			regionIndices.put(regionList.get(i), i);
		}

		final List<List<Integer>> outEdges = new ArrayList<>(regionList.size());
		for (Set<SchedulingExecutionVertex> schedulingExecutionVertices : regionList) {
			final List<Integer> currentRegionOutEdges = new ArrayList<>();
			for (SchedulingExecutionVertex vertex : schedulingExecutionVertices) {
				for (SchedulingResultPartition producedResult : vertex.getProducedResults()) {
					if (producedResult.getResultType().isPipelined()) {
						continue;
					}
					for (ConsumerVertexGroup consumerGroup : producedResult.getGroupedConsumers()) {
						for (ExecutionVertexID consumerVertexId : consumerGroup.getVertices()) {
							SchedulingExecutionVertex consumerVertex =
								producedResult.getVertex(consumerVertexId);
							if (!schedulingExecutionVertices.contains(consumerVertex)) {
								currentRegionOutEdges.add(
									regionIndices.get(vertexToRegion.get(consumerVertex)));
							}
						}
					}
				}
			}
			outEdges.add(currentRegionOutEdges);
		}

		return outEdges;
	}
}
