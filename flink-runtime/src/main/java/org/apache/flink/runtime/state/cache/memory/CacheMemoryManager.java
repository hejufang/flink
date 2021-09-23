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

package org.apache.flink.runtime.state.cache.memory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for managing the allocation and release of memory space
 * used by all {@link org.apache.flink.runtime.state.cache.Cache}.
 */
public class CacheMemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(CacheMemoryManager.class);
	/** The default memory block size. Currently set to 16MB. */
	public static final MemorySize DEFAULT_BLOCK_SIZE = MemorySize.ofMebiBytes(16L);
	/** The minimal memory block size. Currently set to 4MB. */
	public static final MemorySize MIN_BLOCK_SIZE = MemorySize.ofMebiBytes(4L);

	/** The total available memory space. */
	private final MemorySize totalSize;
	/** The minimum granularity of memory space management. */
	private final MemorySize blockSize;
	/** The ratio of each scale up. */
	private final double scaleUpRatio;
	/** The ratio of each scale down. */
	private final double scaleDownRatio;
	/** Locks used for memory allocation. */
	private final Object lock;
	/** The number of allocated blocks. */
	private long allocatedBlocks;
	/** The number of available blocks. */
	private long availableBlocks;
    /** Indicates whether the service is still running. */
	private volatile boolean running;

	public CacheMemoryManager(MemorySize totalSize, MemorySize blockSize, double scaleUpRatio, double scaleDownRatio) {
		Preconditions.checkArgument(blockSize.getBytes() > 0);
		Preconditions.checkArgument(totalSize.getBytes() >= 0);
		Preconditions.checkArgument(scaleUpRatio >= 0 && scaleUpRatio <= 1); // 0 means no scale up
		Preconditions.checkArgument(scaleDownRatio >= 0 && scaleDownRatio <= 1); // 0 means no scale down
		this.totalSize = totalSize;
		this.blockSize = blockSize;
		this.scaleUpRatio = scaleUpRatio;
		this.scaleDownRatio = scaleDownRatio;
		this.availableBlocks = this.totalSize.getBytes() / this.blockSize.getBytes();
		this.allocatedBlocks = 0L;
		this.running = true;
		this.lock = new Object();
	}

	/**
	 * Allocate memory space with block as the granularity.
	 * @param memorySize expect the memory space to be applied for.
	 * @return less than or equal to the requested memory space.
	 */
	public MemorySize allocateMemory(MemorySize memorySize) {
		Preconditions.checkState(running, "Memory manager not running");
		long requestBlocks = memorySize.getBytes() / blockSize.getBytes();
		synchronized (lock) {
			if (this.availableBlocks > 0 && requestBlocks > 0) {
				long allocateBlocks = Math.min(requestBlocks, availableBlocks);
				MemorySize allocatedMemorySize = blockSize.multiply(allocateBlocks);
				availableBlocks -= allocateBlocks;
				allocatedBlocks += allocateBlocks;
				return allocatedMemorySize;
			}
			return MemorySize.ZERO;
		}
	}

	/**
	 * Release the memory space of the specified size.
	 * @param memorySize expect the memory space to be released
	 */
	public void releaseMemory(MemorySize memorySize) {
		Preconditions.checkState(running, "Memory manager not running");
		long releaseBlocks = memorySize.getBytes() / blockSize.getBytes();
		synchronized (lock) {
			if (releaseBlocks > 0) {
				availableBlocks += releaseBlocks;
				allocatedBlocks -= releaseBlocks;
			}
		}
	}

	/**
	 * Calculate the scale up size based on the remaining memory space and the scale up ratio.
	 * @return need to allocate memory space.
	 */
	public Tuple2<Integer, MemorySize> computeScaleUpSize() {
		Preconditions.checkState(running, "Memory manager not running");
		synchronized (lock) {
			int scaleUpBlocks =  (int) Math.floor(scaleUpRatio * availableBlocks);
			return Tuple2.of(scaleUpBlocks, blockSize);
		}
	}

	/**
	 * Calculate the scale down size based on the allocated memory space and scale down ratio.
	 * @return need to release memory space.
	 */
	public Tuple2<Integer, MemorySize> computeScaleDownSize() {
		Preconditions.checkState(running, "Memory manager not running");
		synchronized (lock) {
			int scaleDownBlocks = (int) Math.ceil(scaleDownRatio * allocatedBlocks);
			return Tuple2.of(scaleDownBlocks, blockSize);
		}
	}

	public void shutdown() {
		synchronized (lock) {
			this.running = false;
		}
	}
}
