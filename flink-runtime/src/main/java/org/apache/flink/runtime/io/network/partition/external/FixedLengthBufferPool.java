/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A fix sized buffer pool managing a number of memory segments.
 */
public class FixedLengthBufferPool implements BufferPool, BufferRecycler {
	private final int numberOfBuffers;
	private final int memorySegmentSize;
	private final boolean freeOnDestroy;

	private final Queue<MemorySegment> memorySegments;

	private boolean isDestroyed;

	public FixedLengthBufferPool(int numberOfBuffers, int memorySegmentSize) {
		checkArgument(numberOfBuffers > 0, "There should be at least one buffer.");

		this.numberOfBuffers = numberOfBuffers;
		this.memorySegmentSize = memorySegmentSize;
		this.freeOnDestroy = true;

		this.memorySegments = new ArrayDeque<>(numberOfBuffers);

		synchronized (memorySegments) {
			for (int i = 0; i < numberOfBuffers; i++) {
				ByteBuffer memory = ByteBuffer.allocateDirect(memorySegmentSize);
				memorySegments.add(MemorySegmentFactory.wrapOffHeapMemory(memory));
			}
		}
	}

	public FixedLengthBufferPool(List<MemorySegment> memorySegments, boolean freeOnDestroy) {
		checkArgument(memorySegments.size() > 0, "There should be at least one buffer.");

		this.numberOfBuffers = memorySegments.size();
		this.memorySegmentSize = memorySegments.get(0).size();
		this.freeOnDestroy = freeOnDestroy;

		this.memorySegments = new ArrayDeque<>(memorySegments);
	}

	@Override
	public void lazyDestroy() {
		synchronized (memorySegments) {
			if (isDestroyed) {
				return;
			}

			memorySegments.notifyAll();

			if (freeOnDestroy) {
				for (MemorySegment memorySegment : memorySegments) {
					memorySegment.free();
				}
			}

			memorySegments.clear();

			isDestroyed = true;
		}
	}

	@Override
	public boolean isDestroyed() {
		synchronized (memorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getNumberOfRequiredMemorySegments() {
		return numberOfBuffers;
	}

	@Override
	public int getMaxNumberOfMemorySegments() {
		return numberOfBuffers;
	}

	@Override
	public int getNumBuffers() {
		return numberOfBuffers;
	}

	@Override
	public void setNumBuffers(int numBuffers) throws IOException {
		throw new UnsupportedOperationException("The size of FixedLengthBufferPool could not be changed.");
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (memorySegments) {
			return memorySegments.size();
		}
	}

	@Override
	public int bestEffortGetNumOfUsedBuffers() {
		return numberOfBuffers - memorySegments.size();
	}

	@Override
	public BufferRecycler[] getSubpartitionBufferRecyclers() {
		return new BufferRecycler[0];
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		synchronized (memorySegments) {
			if (isDestroyed) {
				if (freeOnDestroy) {
					memorySegment.free();
				}
			} else {
				memorySegments.add(memorySegment);
				memorySegments.notifyAll();
			}
		}
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		try {
			return requestBufferBlocking();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Nullable
	@Override
	public BufferBuilder requestBufferBuilder() throws IOException {
		return null;
	}

	@Nullable
	@Override
	public BufferBuilder requestBufferBuilder(int targetChannel) throws IOException {
		return null;
	}

	public Buffer requestBufferBlocking() throws InterruptedException {
		MemorySegment memorySegment = requestMemorySegmentBlocking();
		if (memorySegment == null) {
			return null;
		}

		return new NetworkBuffer(memorySegment, this);
	}

	public Buffer requestBufferUnblocking() throws InterruptedException {
		MemorySegment memorySegment = requestMemorySegmentUnblocking();
		if (memorySegment == null) {
			return null;
		}

		return new NetworkBuffer(memorySegment, this);
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
		MemorySegment memorySegment = requestMemorySegmentBlocking();
		if (memorySegment == null) {
			return null;
		}

		return new BufferBuilder(memorySegment, this);
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking(int targetChannel) throws IOException, InterruptedException {
		return null;
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		return false;
	}

	private MemorySegment requestMemorySegmentUnblocking() throws InterruptedException {
		synchronized (memorySegments) {
			return memorySegments.poll();
		}
	}

	public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
		synchronized (memorySegments) {
			while (memorySegments.isEmpty()) {
				if (isDestroyed) {
					return null;
				}

				memorySegments.wait(2000);
			}

			return memorySegments.poll();
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return null;
	}
}
