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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.CloudShuffleVerifierEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;

import com.bytedance.css.client.ShuffleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Writer in {@link CloudShuffleResultPartition}.
 */
public class CloudShuffleWriter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleWriter.class);

	private final String applicationId;

	private final int shuffleId;

	private final int mapperId;

	private final int mapperAttemptId;

	private final int numberOfMappers;

	private final int numberOfReducers;

	private final ShuffleClient shuffleClient;

	private final ByteBuffer[] headerAndBufferArray;

	private final BufferConsumer[] currentBuffers;

	private final long[] sendBytes;

	public CloudShuffleWriter(
			String applicationId,
			int shuffleId,
			int mapperId,
			int mapperAttemptId,
			int numberOfMappers,
			int numberOfReducers,
			ShuffleClient shuffleClient) {
		this.applicationId = applicationId;
		this.shuffleId = shuffleId;
		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;
		this.numberOfMappers = numberOfMappers;
		this.numberOfReducers = numberOfReducers;
		this.shuffleClient = shuffleClient;
		this.headerAndBufferArray = CloudShuffleReadWriterUtil.allocatedWriteBufferArray();
		this.currentBuffers = new BufferConsumer[numberOfReducers];
		this.sendBytes = new long[numberOfReducers];
	}

	public boolean add(BufferConsumer bufferConsumer, int reducerId) throws IOException {
		flushCurrentBuffer(reducerId);
		currentBuffers[reducerId] = bufferConsumer;
		return true;
	}

	private void flushCurrentBuffer(int reducerId) throws IOException {
		final BufferConsumer currentBuffer = currentBuffers[reducerId];
		if (currentBuffer != null) {
			writeAndCloseBufferConsumer(currentBuffer, reducerId);
			currentBuffers[reducerId] = null;
		}
	}

	private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer, int reducerId) throws IOException {
		try {
			final Buffer buffer = bufferConsumer.build();
			try {
				writeBuffer(buffer, reducerId);
				sendBytes[reducerId] += buffer.getSize();
			} finally {
				buffer.recycleBuffer();
			}
		} finally {
			bufferConsumer.close();
		}
	}

	private void writeBuffer(Buffer buffer, int reducerId) throws IOException {
		byte[] data = CloudShuffleReadWriterUtil.writeToCloudShuffleService(mapperId, buffer, headerAndBufferArray);
		this.shuffleClient.pushData(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				reducerId,
				data,
				0,
				data.length,
				numberOfMappers,
				numberOfReducers);
	}

	public void flushAll() throws IOException {
		for (int i = 0; i < numberOfReducers; i++) {
			flushCurrentBuffer(i);
		}
	}

	public void finishWrite() throws IOException {
		for (int i = 0; i < numberOfReducers; i++) {
			add(EventSerializer.toBufferConsumer(new CloudShuffleVerifierEvent(sendBytes[i])), i);
			add(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE), i);
		}

		flushAll();

		shuffleClient.mapperEnd(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				numberOfMappers);
		LOG.info("Writer finish the mapper(appId={},shuffleId={},mapperId={},mapperAttemptId={},numberOfMappers={}).",
				applicationId, shuffleId, mapperId, mapperAttemptId, numberOfMappers);
	}

	@Override
	public void close() throws IOException {
		shuffleClient.shutDown();
	}

	@VisibleForTesting
	public BufferConsumer getCurrentBuffer(int reducerId) {
		return currentBuffers[reducerId];
	}
}
