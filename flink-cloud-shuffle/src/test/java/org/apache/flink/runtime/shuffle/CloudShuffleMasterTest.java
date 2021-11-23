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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import com.bytedance.css.common.protocol.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Tests for {@link CloudShuffleMaster}.
 */
public class CloudShuffleMasterTest {

	@Test
	public void testShuffleIdGenerator() throws UnknownHostException, ExecutionException, InterruptedException {
		final boolean[] shuffleRegistered = {false};
		final TestShuffleClient testShuffleClient = new TestShuffleClient() {
			@Override
			public List<PartitionInfo> registerShuffle(String s, int i, int i1, int i2) throws IOException {
				shuffleRegistered[0] = true;
				return null;
			}
		};

		final int applicationAttemptNumber = 2;
		final CloudShuffleMaster cloudShuffleMaster = new CloudShuffleMaster("TEST_APP_ID", testShuffleClient, applicationAttemptNumber);

		final PartitionDescriptor partitionDescriptor1 = buildRandomPartitionDescriptor(new ShuffleInfo(1, 0, 1, 0, 1));
		final ProducerDescriptor producerDescriptor1 = buildRandomProducerDescriptor();
		final CloudShuffleDescriptor shuffleDescriptor1 = cloudShuffleMaster.registerPartitionWithProducer(partitionDescriptor1, producerDescriptor1).get();
		Assert.assertEquals((applicationAttemptNumber << 16) + partitionDescriptor1.getShuffleInfo().getShuffleId(), shuffleDescriptor1.getShuffleId());

		// task failover and keep the same shuffleId
		final ProducerDescriptor producerDescriptor2 = buildRandomProducerDescriptor();
		final CloudShuffleDescriptor shuffleDescriptor2 = cloudShuffleMaster.registerPartitionWithProducer(partitionDescriptor1, producerDescriptor2).get();
		Assert.assertEquals((applicationAttemptNumber << 16) + partitionDescriptor1.getShuffleInfo().getShuffleId(), shuffleDescriptor2.getShuffleId());

		// register a new shuffle
		final PartitionDescriptor partitionDescriptor2 = buildRandomPartitionDescriptor(new ShuffleInfo(2, 0, 1, 0, 1));
		final ProducerDescriptor producerDescriptor3 = buildRandomProducerDescriptor();
		final CloudShuffleDescriptor shuffleDescriptor3 = cloudShuffleMaster.registerPartitionWithProducer(partitionDescriptor2, producerDescriptor3).get();
		Assert.assertEquals((applicationAttemptNumber << 16) + partitionDescriptor2.getShuffleInfo().getShuffleId(), shuffleDescriptor3.getShuffleId());
	}

	@Test
	public void testGetApplicationAttemptNumber() {
		final String containerId = "container_e522_1627528269117_282114_02_000001";
		final TestShuffleClient testShuffleClient = new TestShuffleClient();
		final CloudShuffleMaster cloudShuffleMaster = new CloudShuffleMaster("TEST_APP_ID", testShuffleClient, 2);
		final int attempt = cloudShuffleMaster.getApplicationAttemptNumber(containerId);
		Assert.assertEquals(2, attempt);
	}

	private PartitionDescriptor buildRandomPartitionDescriptor(ShuffleInfo shuffleInfo) {
		return new PartitionDescriptor(
				new IntermediateDataSetID(),
				2,
				new IntermediateResultPartitionID(new IntermediateDataSetID(), 0),
				ResultPartitionType.BLOCKING,
				10,
				0,
				shuffleInfo);
	}

	private ProducerDescriptor buildRandomProducerDescriptor() throws UnknownHostException {
		return new ProducerDescriptor(
				new ResourceID("resource"),
				new ExecutionAttemptID(),
				InetAddress.getByName("192.168.1.1"),
				-1,
				0);
	}
}
