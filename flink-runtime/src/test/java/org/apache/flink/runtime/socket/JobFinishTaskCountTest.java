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

package org.apache.flink.runtime.socket;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.socket.handler.SocketJobSubmitHandler;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Test case for finish task count in {@link JobGraph}.
 */
public class JobFinishTaskCountTest {
	@Test
	public void testFinishTaskCount() {
		JobGraph jobGraph = new JobGraph();

		List<JobVertex> vertexList = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			JobVertex v = new JobVertex(UUID.randomUUID().toString(), new JobVertexID());
			v.setParallelism(i + 1);
			vertexList.add(v);
			jobGraph.addVertex(v);
		}

		JobVertex v1 = vertexList.get(5);
		JobVertex v2 = vertexList.get(4);
		JobVertex v3 = vertexList.get(3);

		JobVertex v4 = vertexList.get(2);
		v4.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		JobVertex v5 = vertexList.get(1);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		JobVertex v6 = vertexList.get(0);
		v6.connectNewDataSetAsInput(v5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		assertEquals(1, SocketJobSubmitHandler.computeFinishTaskCount(jobGraph));
	}
}
