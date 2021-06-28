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

import org.apache.flink.configuration.MemorySize;

/**
 * Responsible for managing the allocation and release of memory space
 * used by all {@link org.apache.flink.runtime.state.cache.Cache}.
 */
public class MemoryManager {
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

	public MemoryManager(MemorySize totalSize, MemorySize blockSize, double scaleUpRatio, double scaleDownRatio) {
		this.totalSize = totalSize;
		this.blockSize = blockSize;
		this.scaleUpRatio = scaleUpRatio;
		this.scaleDownRatio = scaleDownRatio;
	}

	/**
	 * Allocate memory space with block as the granularity.
	 * @param memorySize expect the memory space to be applied for.
	 * @return less than or equal to the requested memory space.
	 */
	public MemorySize allocateMemory(MemorySize memorySize) {
		//TODO allocate memory after aligning according to the block size
		return memorySize;
	}

	/**
	 * Release the memory space of the specified size.
	 * @param memorySize expect the memory space to be released
	 */
	public void releaseMemory(MemorySize memorySize) {
		//TODO release memory
	}

	/**
	 * Calculate the scale up size based on the remaining memory space and the scale up ratio.
	 * @return need to allocate memory space.
	 */
	public MemorySize computeScaleUpSize() {
		//TODO calculate the scale up size according to a fixed ratio
		return MemorySize.ZERO;
	}

	/**
	 * Calculate the scale down size based on the allocated memory space and scale down ratio.
	 * @return need to release memory space.
	 */
	public MemorySize computeScaleDownSize() {
		//TODO calculate the scale down size according to a fixed ratio
		return MemorySize.ZERO;
	}
}
