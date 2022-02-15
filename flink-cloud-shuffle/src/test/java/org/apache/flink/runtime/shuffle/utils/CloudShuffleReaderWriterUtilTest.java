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

package org.apache.flink.runtime.shuffle.utils;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Tests for {@link CloudShuffleReadWriterUtil}.
 */
public class CloudShuffleReaderWriterUtilTest {

	@Test
	public void testCloudShuffleWrite() throws IOException {
		final int mapperId = 1;
		final Buffer buffer = createTestBuffer();
		final byte[] cloudShuffleData = CloudShuffleReadWriterUtil.writeToCloudShuffleService(
			mapperId, buffer, CloudShuffleReadWriterUtil.allocatedWriteBufferArray());

		final MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledOffHeapMemory(32 * 1024, null);
		final InputStream inputStream = new ByteArrayInputStream(cloudShuffleData);
		CloudShuffleReadWriterUtil.readFromCloudShuffleService(inputStream, readBuffer, MemorySegment::free);
	}

	private static Buffer createTestBuffer() {
		return buildBufferWithAscendingInts(1024, 200, 0);
	}

	private static Buffer buildBufferWithAscendingInts(int bufferSize, int numInts, int nextValue) {
		final MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
		for (int i = 0; i < numInts; i++) {
			seg.putIntLittleEndian(4 * i, nextValue++);
		}

		return new NetworkBuffer(seg, MemorySegment::free, Buffer.DataType.DATA_BUFFER, 4 * numInts);
	}
}
