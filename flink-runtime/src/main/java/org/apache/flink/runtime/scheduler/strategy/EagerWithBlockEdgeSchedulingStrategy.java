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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResult;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertex;
import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.scheduler.SchedulerOperations;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link SchedulingStrategy} instance for schedule all tasks with pipeline immediately, and block tasks when upstream finished.
 * this strategy do not support task failover.
 */
public class EagerWithBlockEdgeSchedulingStrategy implements SchedulingStrategy {
	protected final SchedulerOperations schedulerOperations;

	protected final SchedulingTopology schedulingTopology;

	protected DefaultLogicalTopology defaultLogicalTopology;

	private final DeploymentOption deploymentOption = new DeploymentOption(false);

	Set<JobVertexID> verticesCanDeploy = new HashSet<>();
	Map<IntermediateDataSetID, Set<IntermediateResultPartitionID>> producingDataSet = new HashMap<>();

	Set<SchedulingExecutionVertex> executionVerticesWaitingDeploy = new HashSet<>();

	public EagerWithBlockEdgeSchedulingStrategy(
			SchedulerOperations schedulerOperations,
			SchedulingTopology schedulingTopology,
			DefaultLogicalTopology defaultLogicalTopology) {
		this.schedulerOperations = checkNotNull(schedulerOperations);
		this.schedulingTopology = checkNotNull(schedulingTopology);
		this.defaultLogicalTopology = defaultLogicalTopology;
	}

	@Override
	public void startScheduling() {
		for (SchedulingExecutionVertex schedulingVertex : schedulingTopology.getVertices()) {
			for (SchedulingResultPartition srp : schedulingVertex.getProducedResults()) {
				if (srp.getResultType().isBlocking()) {
					producingDataSet.computeIfAbsent(srp.getResultId(), intermediateDataSetID -> new HashSet<>()).add(srp.getId());
				}
			}
			executionVerticesWaitingDeploy.add(schedulingVertex);
		}
		checkVerticesCanDeploy();

		allocateSlotsAndDeploy(executionVerticesWaitingDeploy);
	}

	private void checkVerticesCanDeploy() {
		Set<DefaultLogicalVertex> visited = new HashSet<>();
		Set<DefaultLogicalVertex> sources = new HashSet<>();

		// all sources can deploy.
		for (DefaultLogicalVertex vertex : defaultLogicalTopology.getVertices()) {
			if (Iterables.isEmpty(vertex.getConsumedResults())) {
				verticesCanDeploy.add(vertex.getId());
				visited.add(vertex);
				sources.add(vertex);
			}
		}

		for (DefaultLogicalVertex vertex : sources) {
			checkVertexCanDeploy(visited, vertex);
		}
	}

	private void checkVertexCanDeploy(final Set<DefaultLogicalVertex> visited, DefaultLogicalVertex vertex) {
		boolean canDeploy = true;
		boolean isVisited = true;
		if (!verticesCanDeploy.contains(vertex.getId())) {
			for (DefaultLogicalResult logicalResult : vertex.getConsumedResults()) {
				// one of producers is not finished, this vertex can not deploy.
				if (producingDataSet.containsKey(logicalResult.getId())) {
					canDeploy = false;
					break;
				}
				// one of producer is not visited, ignore, this vertex will be visited after.
				if (!visited.contains(logicalResult.getProducer())) {
					isVisited = false;
					canDeploy = false;
					break;
				}
				// one of producer can not deploy, this vertex can not deploy.
				if (!verticesCanDeploy.contains(logicalResult.getProducer().getId())) {
					canDeploy = false;
					break;
				}
			}
		}
		// this vertex is visited, it`s consumers can visit.
		if (isVisited) {
			visited.add(vertex);
		}
		if (canDeploy) {
			verticesCanDeploy.add(vertex.getId());
			for (DefaultLogicalResult logicalResult : vertex.getProducedResults()) {
				for (DefaultLogicalVertex consumer : logicalResult.getConsumers()) {
					checkVertexCanDeploy(visited, consumer);
				}
			}
		}
	}

	@Override
	public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
		throw new UnsupportedOperationException("EagerWithBlockEdgeSchedulingStrategy not support failover.");
	}

	@Override
	public void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState) {
		if (!FINISHED.equals(executionState)) {
			return;
		}
		if (producingDataSet.isEmpty()) {
			// producing data set is empty, means all vertex is already deployed, no need to check.
			return;
		}

		for (SchedulingResultPartition srp : schedulingTopology.getVertex(executionVertexId).getProducedResults()) {
			if (srp.getResultType().isBlocking()) {
				producingDataSet.get(srp.getResultId()).remove(srp.getId());
				if (producingDataSet.get(srp.getResultId()).isEmpty()) {
					producingDataSet.remove(srp.getResultId());
				}
			}
		}
		checkVerticesCanDeploy();

		allocateSlotsAndDeploy(executionVerticesWaitingDeploy);
	}

	@Override
	public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {
		// Will not react to these notifications.
	}

	private void allocateSlotsAndDeploy(final Iterable<? extends SchedulingExecutionVertex> vertices) {
		final Set<ExecutionVertexID> verticesToDeploy = new HashSet<>();
		Iterator<? extends SchedulingExecutionVertex> vertexIterator = vertices.iterator();
		while (vertexIterator.hasNext()) {
			SchedulingExecutionVertex vertex = vertexIterator.next();
			if (verticesCanDeploy.contains(vertex.getId().getJobVertexId())) {
				vertexIterator.remove();
				verticesToDeploy.add(vertex.getId());
			}
		}

		final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions =
			SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(
				schedulingTopology,
				verticesToDeploy,
				id -> deploymentOption);
		schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
	}

	/**
	 * The factory for creating {@link EagerWithBlockEdgeSchedulingStrategy}.
	 */
	public static class Factory implements SchedulingStrategyFactory {

		@Override
		public SchedulingStrategy createInstance(SchedulerOperations schedulerOperations, SchedulingTopology schedulingTopology, DefaultLogicalTopology defaultLogicalTopology) {
			return new EagerWithBlockEdgeSchedulingStrategy(schedulerOperations, schedulingTopology, defaultLogicalTopology);
		}
	}
}
