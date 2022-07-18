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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Note this is just an abstract class.
 *
 * <p>The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances
 * for the network stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 */
public interface NetworkBufferPool extends BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {
	MemorySegment requestMemorySegment();

	void recycle(MemorySegment segment);

	@Override
	List<MemorySegment> requestMemorySegments() throws IOException;

	@Override
	void recycleMemorySegments(Collection<MemorySegment> segments) throws IOException;

	void destroy();

	boolean isDestroyed();

	int getTotalNumberOfMemorySegments();

	long getTotalMemory();

	int getNumTotalRequiredBuffers();

	int getNumberOfAvailableMemorySegments();

	long getAvailableMemory();

	int getNumberOfUsedMemorySegments();

	long getUsedMemory();

	int getNumberOfRegisteredBufferPools();

	int getNumberOfAllocatedMemorySegments();

	long getAllocatedMemory();

	int countBuffers();

	@Override
	CompletableFuture<?> getAvailableFuture();

	@Override
	BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException;

	@Override
	BufferPool createBufferPool(
		int numRequiredBuffers,
		int maxUsedBuffers,
		BufferPoolOwner bufferPoolOwner,
		int numSubpartitions,
		int maxBuffersPerChannel) throws IOException;

	@Override
	void destroyBufferPool(BufferPool bufferPool) throws IOException;

	void destroyAllBufferPools();

	void tryResizeLocalBufferPool(LocalBufferPool localBufferPool) throws IOException;

	void tryReturnExcessMemorySegments() throws IOException;

	long getRequestNetworkSegmentTimeoutMills();
}
