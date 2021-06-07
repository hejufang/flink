/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.io.network.partition.PartitionerResultSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BacklogBasedRebalancePartitioner}.
 */
public class BacklogBasedRebalancePartitionerTest extends StreamPartitionerTest {

	@Override
	public StreamPartitioner<Tuple> createPartitioner() {
		StreamPartitioner<Tuple> partitioner = new BacklogBasedRebalancePartitioner<>();
		assertFalse(partitioner.isBroadcast());
		return partitioner;
	}

	@Test
	public void testSelectChannelsInterval1() {
		final int numberOfChannels = 2;
		if (streamPartitioner instanceof ConfigurableBacklogPartitioner) {
			((ConfigurableBacklogPartitioner) streamPartitioner).configure(2);
		}
		streamPartitioner.setup(numberOfChannels, new ResultSubpartition[]{
				new PartitionerResultSubpartition(1),
				new PartitionerResultSubpartition(0)
		});

		int initialChannel = streamPartitioner.selectChannel(serializationDelegate);
		assertTrue(0 <= initialChannel);
		assertTrue(numberOfChannels > initialChannel);

		for (int i = 1; i <= 3; i++) {
			assertSelectedChannel((initialChannel + i) % numberOfChannels);
		}
	}

	@Test
	public void testSelectChannelsInterval2() {
		final int numberOfChannels = 2;
		if (streamPartitioner instanceof ConfigurableBacklogPartitioner) {
			((ConfigurableBacklogPartitioner) streamPartitioner).configure(2);
		}
		streamPartitioner.setup(numberOfChannels, new ResultSubpartition[]{
				new PartitionerResultSubpartition(3),
				new PartitionerResultSubpartition(0)
		});

		int initialChannel = streamPartitioner.selectChannel(serializationDelegate);
		assertTrue(0 <= initialChannel);
		assertTrue(numberOfChannels > initialChannel);

		for (int i = 1; i <= 3; i++) {
			assertSelectedChannel(1);
		}
	}

	@Test
	public void testSelectChannelsInterval3() {
		final int numberOfChannels = 2;
		if (streamPartitioner instanceof ConfigurableBacklogPartitioner) {
			((ConfigurableBacklogPartitioner) streamPartitioner).configure(2);
		}
		streamPartitioner.setup(numberOfChannels, new ResultSubpartition[]{
				new PartitionerResultSubpartition(3),
				new PartitionerResultSubpartition(3)
		});

		int initialChannel = streamPartitioner.selectChannel(serializationDelegate);
		assertTrue(0 <= initialChannel);
		assertTrue(numberOfChannels > initialChannel);

		for (int i = 1; i <= 3; i++) {
			assertSelectedChannel((initialChannel + i) % numberOfChannels);
		}
	}
}
