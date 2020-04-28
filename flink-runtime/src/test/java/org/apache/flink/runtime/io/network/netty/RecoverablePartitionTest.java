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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.netty.PartitionRequestQueue;
import org.apache.flink.runtime.io.network.partition.PartitionTestUtils;
import org.apache.flink.runtime.io.network.partition.RecoverablePipelinedSubpartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class RecoverablePartitionTest {

	@Test
	public void testChannelClosed() throws Exception {
		final ResultPartitionManager partitionManager = new ResultPartitionManager();
		final InputChannelID receiverId1 = new InputChannelID();
		final PartitionRequestQueue queue = new PartitionRequestQueue();
		final CreditBasedSequenceNumberingViewReader reader1 = new CreditBasedSequenceNumberingViewReader(receiverId1, 1, queue);

		// initialize netty's context
		EmbeddedChannel channel = new EmbeddedChannel(queue);

		final ResultPartition partition = createRecoverableResultPartition(partitionManager);
		reader1.requestSubpartitionView(partitionManager, partition.getPartitionId(), 0);

		queue.notifyReaderCreated(reader1);

		// cancel channel
		queue.cancel(receiverId1);

		RecoverablePipelinedSubpartition subpartition = (RecoverablePipelinedSubpartition) partition.getAllPartitions()[0];
		assertFalse(subpartition.isSubpartitionAvailable());
		assertEquals(0, subpartition.getBuffersSize());
		assertNull(subpartition.getView());

		channel.close();

		// recover because the downstream task is restarted
		final InputChannelID receiverId2 = new InputChannelID();
		final CreditBasedSequenceNumberingViewReader reader2 = new CreditBasedSequenceNumberingViewReader(receiverId2, 1, queue);
		reader2.requestSubpartitionView(partitionManager, partition.getPartitionId(), 0);
		assertTrue(subpartition.isSubpartitionAvailable());
		queue.notifyReaderCreated(reader2);
		queue.addCredit(receiverId2, 2);
		assertEquals(3, reader2.getNumCreditsAvailable());
	}

	private ResultPartition createRecoverableResultPartition(ResultPartitionManager partitionManager) throws IOException {
		final ResultPartition partition = new ResultPartitionBuilder()
				.setNumberOfSubpartitions(1)
				.setResultPartitionType(ResultPartitionType.PIPELINED)
				.isRecoverable(true)
				.build();

		assertTrue(partition.getAllPartitions()[0] instanceof RecoverablePipelinedSubpartition);

		partitionManager.registerResultPartition(partition);

		partition.addBufferConsumer(createFilledBufferConsumer(1024, 1024), 0);
		partition.addBufferConsumer(createFilledBufferConsumer(1024, 1024), 0);
		partition.addBufferConsumer(createFilledBufferConsumer(1024, 1024), 0);

		return partition;
	}
}
