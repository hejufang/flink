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

package org.apache.flink.runtime.shuffle.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.shuffle.buffer.CloudShuffleBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utils for read/write on Cloud Shuffle Service.
 */
public class CloudShuffleReadWriterUtil {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleReadWriterUtil.class);

	static final int HEADER_LENGTH = 10;

	private static final short HEADER_VALUE_IS_BUFFER = 0;

	private static final short HEADER_VALUE_IS_EVENT = 1;

	public static byte[] writeToCloudShuffleService(int mapperId, Buffer buffer, ByteBuffer[] arrayWithHeaderBuffer) {
		final ByteBuffer headerBuffer = arrayWithHeaderBuffer[0];
		headerBuffer.clear();
		headerBuffer.putInt(mapperId);
		headerBuffer.putShort(buffer.isBuffer() ? HEADER_VALUE_IS_BUFFER : HEADER_VALUE_IS_EVENT);
		headerBuffer.putInt(buffer.getSize());
		headerBuffer.flip();

		/*
		LOG.info("HeadBuffer is written. mapperId={},sendCount={},isEvent={},isCompressed={},Data size={}.", mapperId, sendCount, !buffer.isBuffer(), buffer.isCompressed(), buffer.getSize());
		 */

		final ByteBuffer dataBuffer = buffer.getNioBufferReadable();
		arrayWithHeaderBuffer[1] = dataBuffer;

		final int bytesExpected = HEADER_LENGTH + dataBuffer.remaining();

		byte[] data = new byte[bytesExpected];
		headerBuffer.get(data, 0, HEADER_LENGTH);
		dataBuffer.get(data, HEADER_LENGTH, data.length - HEADER_LENGTH);
		return data;
	}

	@Nullable
	public static CloudShuffleBuffer readFromCloudShuffleSerivice(
			InputStream inputStream,
			MemorySegment memorySegment,
			BufferRecycler bufferRecycler) throws IOException {
		byte[] header = new byte[HEADER_LENGTH];
		ByteBuffer headerBuffer = configureByteBuffer(ByteBuffer.wrap(header));
		// read header
		int headerLength = inputStream.read(header, 0, HEADER_LENGTH);
		if (headerLength == -1) {
			return null;
		}

		final ByteBuffer targetBuf;
		final int mapperId;
		final boolean isEvent;
		final int size;
		try {
			mapperId = headerBuffer.getInt();
			isEvent = headerBuffer.getShort() == HEADER_VALUE_IS_EVENT;
			size = headerBuffer.getInt();

			/*
			LOG.info("HeadBuffer is read out. mapperId={},sendCount={},isEvent={},isCompressed={},Data size={}.", mapperId, sendCount, isEvent, isCompressed, size);
			 */

			targetBuf = memorySegment.wrap(0, size);
		} catch (BufferUnderflowException | IllegalArgumentException e) {
			// buffer underflow if header buffer is undersized
			// IllegalArgumentException if size is outside memory segment size
			throwCorruptDataException();
			return null; // silence compiler
		}

		// read fully data
		final byte[] data = new byte[size];
		int len = inputStream.read(data);
		// copy to direct ByteBuffer
		targetBuf.put(data);

		if (len == -1) {
			throw new IllegalStateException();
		}

		Buffer.DataType dataType = isEvent ? Buffer.DataType.EVENT_BUFFER : Buffer.DataType.DATA_BUFFER;
		NetworkBuffer networkBuffer = new NetworkBuffer(memorySegment, bufferRecycler, dataType, size);
		return new CloudShuffleBuffer(networkBuffer, mapperId);
	}

	private static void throwCorruptDataException() throws IOException {
		throw new IOException("The spill file is corrupt: buffer size and boundaries invalid");
	}

	static ByteBuffer allocatedHeaderBuffer() {
		ByteBuffer bb = ByteBuffer.allocateDirect(HEADER_LENGTH);
		configureByteBuffer(bb);
		return bb;
	}

	public static ByteBuffer[] allocatedWriteBufferArray() {
		return new ByteBuffer[] { allocatedHeaderBuffer(), null };
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	static ByteBuffer configureByteBuffer(ByteBuffer buffer) {
		buffer.order(ByteOrder.nativeOrder());
		return buffer;
	}
}
