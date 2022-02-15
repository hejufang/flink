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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.shuffle.CloudShuffleOptions;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * The tests for CloudShuffleConfiguration.
 */
public class CloudShuffleConfigurationTest {

	@Test
	public void testReconfigureConfig() {
		JobVertex jobVertex1 = new JobVertex("source");
		JobVertex jobVertex2 = new JobVertex("sink");
		jobVertex2.connectNewDataSetAsInput(jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		JobGraph jobGraph1 = new JobGraph("graph-1", jobVertex1, jobVertex2);
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1024)
			.setTaskManagerMemoryMB(1024)
			.setSlotsPerTaskManager(2)
			.createClusterSpecification();

		// case 1: streaming job, start css, and get css conf from coordinator
		Configuration configuration1 = new Configuration();
		configuration1.setBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT, true);
		Assert.assertFalse(CloudShuffleConfiguration.reconfigureConfig(jobGraph1, clusterSpecification, configuration1));

		// case 2: batch job, start css, and get css conf from coordinator
		Configuration configuration2 = new Configuration();
		configuration2.setBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT, true);
		configuration2.setString(ConfigConstants.FLINK_APPLICATION_TYPE, ConfigConstants.FLINK_BATCH_APPLICATION_TYPE);
		configuration2.setString(ConfigConstants.DC_KEY, "cn");
		configuration2.setString(ConfigConstants.CLUSTER_NAME_KEY, "leser");
		configuration2.setString(ConfigConstants.YARN_APPLICATION_QUEUE, "root.leser_flink.online");
		configuration2.setString(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_COORDINATOR_URL, "http://css-coordinator.byted.org");
		if (CloudShuffleConfiguration.reconfigureConfig(jobGraph1, clusterSpecification, configuration2)) {
			// some environments could not connect to the css coordinator
			Assert.assertEquals(configuration2.getString(ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS), "org.apache.flink.runtime.shuffle.CloudShuffleServiceFactory");
			Assert.assertTrue(configuration2.containsKey(CloudShuffleCoordinator.PROVIDED_USER_SHARED_LIB_DIRS.key()));
		}

		// case 3: batch job, start css, and get css conf from coordinator, but not set css coordinator url
		Configuration configuration3 = new Configuration();
		configuration3.setBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT, true);
		configuration3.setString(ConfigConstants.FLINK_APPLICATION_TYPE, ConfigConstants.FLINK_BATCH_APPLICATION_TYPE);
		configuration3.setString(ConfigConstants.DC_KEY, "cn");
		configuration3.setString(ConfigConstants.CLUSTER_NAME_KEY, "leser");
		configuration3.setString(ConfigConstants.YARN_APPLICATION_QUEUE, "root.leser_flink.online");
		Assert.assertFalse(CloudShuffleConfiguration.reconfigureConfig(jobGraph1, clusterSpecification, configuration3));

		// case 4: batch job, start css, but not get css conf from coordinators
		Configuration configuration4 = new Configuration();
		configuration4.setBoolean(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_SUPPORT, true);
		configuration4.setString(ConfigConstants.FLINK_APPLICATION_TYPE, ConfigConstants.FLINK_BATCH_APPLICATION_TYPE);
		configuration4.setString(ConfigConstants.DC_KEY, "cn");
		configuration4.setString(ConfigConstants.CLUSTER_NAME_KEY, "leser");
		configuration4.setString(ConfigConstants.YARN_APPLICATION_QUEUE, "root.leser_flink.online");
		configuration4.setString(CloudShuffleOptions.CLOUD_SHUFFLE_CLUSTER, "xx");
		configuration4.setString(CloudShuffleOptions.CLOUD_SHUFFLE_ZK_ADDRESS, "127.0.0.1:2181");
		Assert.assertTrue(CloudShuffleConfiguration.reconfigureConfig(jobGraph1, clusterSpecification, configuration4));
		Assert.assertEquals(configuration4.getString(ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS), "org.apache.flink.runtime.shuffle.CloudShuffleServiceFactory");
		Assert.assertTrue(!configuration4.containsKey(CloudShuffleCoordinator.PROVIDED_USER_SHARED_LIB_DIRS.key()));
	}

	@Test
	public void testIsValidJobGraph() {
		// test pipelined
		JobVertex jobVertex1 = new JobVertex("source1");
		JobVertex jobVertex2 = new JobVertex("sink1");
		jobVertex2.connectNewDataSetAsInput(jobVertex1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		JobGraph jobGraph1 = new JobGraph("graph-1", jobVertex1, jobVertex2);
		Assert.assertFalse(CloudShuffleConfiguration.isValidJobGraph(jobGraph1));

		// test pointwise
		JobVertex jobVertex3 = new JobVertex("source3");
		JobVertex jobVertex4 = new JobVertex("sink3");
		jobVertex4.connectNewDataSetAsInput(jobVertex3, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
		JobGraph jobGraph2 = new JobGraph("graph-2", jobVertex3, jobVertex4);
		Assert.assertTrue(CloudShuffleConfiguration.isValidJobGraph(jobGraph2));

		// test all-to-all and BLOCKING
		JobVertex jobVertex5 = new JobVertex("source5");
		JobVertex jobVertex6 = new JobVertex("sink5");
		jobVertex6.connectNewDataSetAsInput(jobVertex5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		JobGraph jobGraph3 = new JobGraph("graph-3", jobVertex5, jobVertex6);
		Assert.assertTrue(CloudShuffleConfiguration.isValidJobGraph(jobGraph3));
	}
}
