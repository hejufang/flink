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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link CloudShuffleWriter}.
 */
public class CloudShuffleWriterTest {

	@Test
	public void testFlushBuffer() throws IOException {
		final boolean[] dataPushed = new boolean[]{false};
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int pushData(String s, int i, int i1, int i2, int i3, byte[] bytes, int i4, int i5, int i6, int i7) throws IOException {
				dataPushed[0] = true;
				return 1;
			}
		};
		final CloudShuffleWriter shuffleWriter = new CloudShuffleWriter("app-id", 1, 1, 1, 1, 2, shuffleClient);

		final MemorySegment memorySegment1 = MemorySegmentFactory.allocateUnpooledSegment(32 * 1024);
		final BufferConsumer bufferConsumer1 = new BufferConsumer(memorySegment1, seg -> {}, Buffer.DataType.DATA_BUFFER);

		final int reducerId = 1;
		Assert.assertNull(shuffleWriter.getCurrentBuffer(reducerId));

		shuffleWriter.add(bufferConsumer1, reducerId);
		Assert.assertNotNull(shuffleWriter.getCurrentBuffer(reducerId));
		Assert.assertFalse(dataPushed[0]);

		final MemorySegment memorySegment2 = MemorySegmentFactory.allocateUnpooledSegment(32 * 1024);
		final BufferConsumer bufferConsumer2 = new BufferConsumer(memorySegment2, seg -> {}, Buffer.DataType.DATA_BUFFER);
		shuffleWriter.add(bufferConsumer2, reducerId);
		Assert.assertTrue(dataPushed[0]);
	}
}
