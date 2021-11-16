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

package org.apache.flink.client.css;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CloudShuffleCoordinator}.
 */
public class CloudShuffleCoordinatorTest {

	@Test
	public void testReconfigureConfig() {
		// test exception
		JobVertex jobVertex1 = new JobVertex("source");
		JobVertex jobVertex2 = new JobVertex("sink");
		jobVertex2.connectNewDataSetAsInput(jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		JobGraph jobGraph1 = new JobGraph("graph-1", jobVertex1, jobVertex2);
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1024)
			.setTaskManagerMemoryMB(1024)
			.setSlotsPerTaskManager(2)
			.createClusterSpecification();
		Configuration configuration = new Configuration();
		configuration.setBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT, true);
		Assert.assertFalse(CloudShuffleCoordinator.reconfigureConfig(jobGraph1, clusterSpecification, configuration));
	}

	@Test
	public void testIsValidJobGraph() {
		// test pipelined
		JobVertex jobVertex1 = new JobVertex("source");
		JobVertex jobVertex2 = new JobVertex("sink");
		jobVertex2.connectNewDataSetAsInput(jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		JobGraph jobGraph1 = new JobGraph("graph-1", jobVertex1, jobVertex2);
		Assert.assertFalse(CloudShuffleCoordinator.isValidJobGraph(jobGraph1));

		// test pointwise
		JobVertex jobVertex3 = new JobVertex("source");
		JobVertex jobVertex4 = new JobVertex("sink");
		jobVertex4.connectNewDataSetAsInput(jobVertex3, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		JobGraph jobGraph2 = new JobGraph("graph-2", jobVertex3, jobVertex4);
		Assert.assertFalse(CloudShuffleCoordinator.isValidJobGraph(jobGraph2));

		// test all-to-all and BLOCKING
		JobVertex jobVertex5 = new JobVertex("source");
		JobVertex jobVertex6 = new JobVertex("sink");
		jobVertex6.connectNewDataSetAsInput(jobVertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		JobGraph jobGraph3 = new JobGraph("graph-3", jobVertex5, jobVertex6);
		Assert.assertTrue(CloudShuffleCoordinator.isValidJobGraph(jobGraph3));
	}
}
