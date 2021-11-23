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
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.shuffle.ShuffleInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link EdgeManagerBuildUtil}.
 */
public class EdgeManagerTest {

	@Before
	public void setUp() {
		EdgeManagerBuildUtil.resetShuffleIdGenerator();
	}

	@Test
	public void testAlltoAllShuffleInfo() throws Exception {
		// m*n shuffle, m=2,n=3
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			2,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.ALL_TO_ALL);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 1, 0, 2);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 1, 0, 1, 0, 2);
	}

	@Test
	public void testForwardShuffleInfo() throws Exception {
		// m*n shuffle, m=2,n=2
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			2,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.POINTWISE);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 0, 0, 0);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 2, 1, 1, 1, 1);
	}

	@Test
	public void testRescaleShuffleInfo1() throws Exception {
		// m*n shuffle, m=2,n=4
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			2,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.POINTWISE);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 0, 0, 1);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 2, 1, 1, 2, 3);
	}

	@Test
	public void testRescaleShuffleInfo2() throws Exception {
		// m*n shuffle, m=4,n=2
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			4,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		ires.setPartition(2, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 2));
		ires.setPartition(3, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 3));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.POINTWISE);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 1, 0, 0);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 1, 0, 1, 0, 0);
		checkShuffleInfo(ires.getPartitions()[2].getShuffleInfo(), 2, 2, 3, 1, 1);
		checkShuffleInfo(ires.getPartitions()[3].getShuffleInfo(), 2, 2, 3, 1, 1);
	}

	@Test
	public void testRescaleShuffleInfo3() throws Exception {
		// m*n shuffle, m=5,n=2
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			5,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		ires.setPartition(2, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 2));
		ires.setPartition(3, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 3));
		ires.setPartition(4, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 4));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.POINTWISE);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 1, 0, 0);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 1, 0, 1, 0, 0);
		checkShuffleInfo(ires.getPartitions()[2].getShuffleInfo(), 2, 2, 4, 1, 1);
		checkShuffleInfo(ires.getPartitions()[3].getShuffleInfo(), 2, 2, 4, 1, 1);
		checkShuffleInfo(ires.getPartitions()[4].getShuffleInfo(), 2, 2, 4, 1, 1);
	}

	@Test
	public void testRescaleShuffleInfo5() throws Exception {
		// m*n shuffle, m=2,n=5
		IntermediateResult ires = new IntermediateResult(
			new IntermediateDataSetID(),
			ExecutionGraphTestUtils.getExecutionJobVertex(new JobVertexID()),
			2,
			ResultPartitionType.BLOCKING);
		ires.setPartition(0, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 0));
		ires.setPartition(1, new IntermediateResultPartition(ires, ExecutionGraphTestUtils.getExecution().getVertex(), 1));
		EdgeManagerBuildUtil.registerToExecutionEdgeManager(
			new ExecutionVertex[]{
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex(),
				ExecutionGraphTestUtils.getExecution().getVertex()},
			ires,
			0,
			DistributionPattern.POINTWISE);
		checkShuffleInfo(ires.getPartitions()[0].getShuffleInfo(), 1, 0, 0, 2, 4);
		checkShuffleInfo(ires.getPartitions()[1].getShuffleInfo(), 2, 1, 1, 0, 1);
	}

	private void checkShuffleInfo(ShuffleInfo candidate, int shuffleId, int mapperBeginIndex, int mapperEndIndex, int reducerBeginIndex, int reducerEndIndex) {
		Assert.assertEquals(shuffleId, candidate.getShuffleId());
		Assert.assertEquals(mapperBeginIndex, candidate.getMapperBeginIndex());
		Assert.assertEquals(mapperEndIndex, candidate.getMapperEndIndex());
		Assert.assertEquals(reducerBeginIndex, candidate.getReducerBeginIndex());
		Assert.assertEquals(reducerEndIndex, candidate.getReducerEndIndex());
	}
}
