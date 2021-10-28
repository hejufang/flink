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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.shuffle.buffer.CloudShuffleBuffer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.client.stream.CssInputStreamImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;

/**
 * CloudShuffleReader.
 */
public class CloudShuffleReader implements BufferRecycler {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleReader.class);

	private static final int NUM_BUFFERS = 2;

	private final ArrayDeque<MemorySegment> buffers;

	private CssInputStreamImpl inputStream;

	public CloudShuffleReader(
			int memorySegmentSize,
			String applicationId,
			int shuffleId,
			int reducerId,
			int numberOfMappers,
			ShuffleClient shuffleClient) {
		this.buffers = new ArrayDeque<>(NUM_BUFFERS);

		for (int i = 0; i < NUM_BUFFERS; i++) {
			buffers.addLast(MemorySegmentFactory.allocateUnpooledOffHeapMemory(memorySegmentSize, null));
		}

		try {
			inputStream = (CssInputStreamImpl) shuffleClient.readPartitions(
					applicationId,
					shuffleId,
					new int[] {reducerId},
					0,
					numberOfMappers);
		} catch (IOException e) {
			LOG.error("Fail to connect to remote shuffle service.", e);
			throw new RuntimeException(e);
		}
	}

	public CloudShuffleBuffer pollNext() throws IOException {
		final MemorySegment memory = buffers.pollFirst();
		if (memory == null) {
			return null;
		}

		final CloudShuffleBuffer next = CloudShuffleReadWriterUtil.readFromCloudShuffleSerivice(inputStream, memory, this);
		if (next == null) {
			recycle(memory);
		}

		return next;
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		// this will be called after bytes are copied to StreamTaskNetworkInput
		buffers.addLast(memorySegment);
	}

	public void close() {
		while (!buffers.isEmpty()) {
			MemorySegment segment = buffers.pollFirst();
			segment.free();
		}
	}
}
