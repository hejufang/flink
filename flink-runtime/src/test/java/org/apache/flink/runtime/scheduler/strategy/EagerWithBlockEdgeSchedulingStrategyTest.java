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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link EagerWithBlockEdgeSchedulingStrategy}.
 */
public class EagerWithBlockEdgeSchedulingStrategyTest extends TestLogger {

	private final TestingSchedulerOperations testingSchedulerOperation = new TestingSchedulerOperations();

	/**
	 * Test job like A->(Block,ALL)B->(Pipeline,ALL)C.
	 */
	@Test
	public void testScheduleWithBlock() {
		final JobVertex[] jobVertices = new JobVertex[3];
		final int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[2] = createNoOpVertex("v3", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, BLOCKING);
		jobVertices[2].connectNewDataSetAsInput(jobVertices[1], ALL_TO_ALL, PIPELINED);
		JobGraph jobGraph = new JobGraph(jobVertices);
		final DefaultLogicalTopology logicalTopology = new DefaultLogicalTopology(jobGraph);
		final TestingSchedulingTopology testingSchedulingTopology = createSchedulingTopologyFromJobGraph(jobGraph);

		final EagerWithBlockEdgeSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology, logicalTopology);
		final List<TestingSchedulingExecutionVertex> vertices = new ArrayList<>();
		testingSchedulingTopology.getVertices().forEach(v -> {
			if (v.getId().getJobVertexId().equals(jobVertices[0].getID())) {
				vertices.add(v);
			}
		});
		// only jobVertex1 deployed.
		assertLatestScheduledVerticesAreEqualTo(vertices);

		// jobVertex1 finished.
		for (TestingSchedulingExecutionVertex vertex : vertices) {
			schedulingStrategy.onExecutionStateChange(vertex.getId(), ExecutionState.FINISHED);
		}

		// jobVertex2,jobVertex3 deployed
		vertices.clear();
		testingSchedulingTopology.getVertices().forEach(v -> {
			if (v.getId().getJobVertexId().equals(jobVertices[1].getID()) || v.getId().getJobVertexId().equals(jobVertices[2].getID())) {
				vertices.add(v);
			}
		});

		assertLatestScheduledVerticesAreEqualTo(vertices);
	}

	/**
	 * Test job like A->(Pipeline,ALL)B->(Pipeline,ALL)C.
	 */
	@Test
	public void testScheduleWithAllPipelined() {
		final JobVertex[] jobVertices = new JobVertex[3];
		final int parallelism = 3;
		jobVertices[0] = createNoOpVertex("v1", parallelism);
		jobVertices[1] = createNoOpVertex("v2", parallelism);
		jobVertices[2] = createNoOpVertex("v3", parallelism);
		jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);
		jobVertices[2].connectNewDataSetAsInput(jobVertices[1], ALL_TO_ALL, PIPELINED);
		JobGraph jobGraph = new JobGraph(jobVertices);
		final DefaultLogicalTopology logicalTopology = new DefaultLogicalTopology(jobGraph);
		final TestingSchedulingTopology testingSchedulingTopology = createSchedulingTopologyFromJobGraph(jobGraph);

		final EagerWithBlockEdgeSchedulingStrategy schedulingStrategy = startScheduling(testingSchedulingTopology, logicalTopology);
		final List<TestingSchedulingExecutionVertex> vertices = new ArrayList<>();
		testingSchedulingTopology.getVertices().forEach(vertices::add);
		// all jobVertices deployed.
		assertLatestScheduledVerticesAreEqualTo(vertices);
	}

	private EagerWithBlockEdgeSchedulingStrategy startScheduling(TestingSchedulingTopology testingSchedulingTopology, DefaultLogicalTopology logicalTopology) {
		EagerWithBlockEdgeSchedulingStrategy schedulingStrategy = new EagerWithBlockEdgeSchedulingStrategy(
			testingSchedulerOperation,
			testingSchedulingTopology,
			logicalTopology);
		schedulingStrategy.startScheduling();
		return schedulingStrategy;
	}

	private void assertLatestScheduledVerticesAreEqualTo(final List<TestingSchedulingExecutionVertex> expected) {
		final List<List<ExecutionVertexDeploymentOption>> deploymentOptions = testingSchedulerOperation.getScheduledVertices();
		assertFalse(deploymentOptions.isEmpty());
		final List<ExecutionVertexDeploymentOption> latestDeploymentOptions = deploymentOptions.get(deploymentOptions.size() - 1);
		assertThat(expected.size(), lessThanOrEqualTo(latestDeploymentOptions.size()));
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(
				expected.get(expected.size() - i - 1).getId(),
				latestDeploymentOptions.get(latestDeploymentOptions.size() - i - 1).getExecutionVertexId());
		}
	}

	private static JobVertex createNoOpVertex(String name, int parallelism) {
		JobVertex vertex = new JobVertex(name);
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(parallelism);
		return vertex;
	}

	private static TestingSchedulingTopology createSchedulingTopologyFromJobGraph(JobGraph jobGraph) {
		final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();
		Map<JobVertexID, List<TestingSchedulingExecutionVertex>> vertices = new HashMap<>();

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			vertices.put(jobVertex.getID(),
					testingSchedulingTopology.addExecutionVertices()
							.withJobVertexID(jobVertex.getID())
							.withParallelism(jobVertex.getParallelism())
							.finish());
		}

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			for (JobEdge jobEdge: jobVertex.getInputs()) {
				DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
				ResultPartitionType resultPartitionType = jobEdge.getSource().getResultType();
				JobVertex producer = jobEdge.getSource().getProducer();
				if (distributionPattern.equals(ALL_TO_ALL)) {
					testingSchedulingTopology
							.connectAllToAll(vertices.get(producer.getID()), vertices.get(jobVertex.getID()))
							.withResultPartitionType(resultPartitionType)
							.withIntermediateDataSetID(jobEdge.getSourceId())
							.finish();
				} else if (distributionPattern.equals(POINTWISE)) {
					testingSchedulingTopology
							.connectPointwise(vertices.get(producer.getID()), vertices.get(jobVertex.getID()))
							.withResultPartitionType(resultPartitionType)
							.withIntermediateDataSetID(jobEdge.getSourceId())
							.finish();
				}
			}
		}
		return testingSchedulingTopology;
	}
}
