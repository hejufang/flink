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

package org.apache.flink.runtime.memory;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cache memory manager for heap and off heap.
 */
public class MemoryPoolManager extends MemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

	/** If the segment can be lazy allocated. */
	private final boolean lazyAllocate;

	/** The total memory size. */
	private final long memorySize;

	/** The total number of segments the memory manager can allocate. */
	private final int totalNumberOfMemorySegments;

	/** The memory pool should be split to buckets for operator, the bucket count is slot count for global memory pool. */
	private final int memoryPoolBucketCount;

	/** Track the number of segments that have been allocated. */
	private final AtomicInteger numberOfAllocatedMemorySegments = new AtomicInteger(0);

	/** Track the number of available segments in availableMemorySegments. */
	private final AtomicInteger numberOfAvailableMemorySegments = new AtomicInteger(0);

	/** The segments queue managed by the memory manager. */
	private final Queue<MemorySegment> availableMemorySegments;

	/** Memory segments allocated per memory owner. */
	private final Map<Object, Set<MemorySegment>> allocatedSegments;

	/** The timeout for request memory segments. */
	private final Duration requestMemorySegmentsTimeout;

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize                 The total size of the memory to be managed by this memory manager.
	 * @param pageSize                   The size of the pages handed out by the memory manager.
	 * @param lazyAllocate               The memory manager allocate segments by lazy.
	 */
	public MemoryPoolManager(
			long memorySize,
			int pageSize,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			int memoryPoolBucketCount) {
		super(memorySize, pageSize, UnsafeMemoryBudget.MAX_SLEEPS_VERIFY_EMPTY);
		this.requestMemorySegmentsTimeout = requestMemorySegmentsTimeout;
		this.lazyAllocate = lazyAllocate;
		this.memoryPoolBucketCount = memoryPoolBucketCount;

		this.memorySize = memorySize;
		this.totalNumberOfMemorySegments = (int) (memorySize / pageSize);
		this.availableMemorySegments = new ConcurrentLinkedQueue<>();
		if (!lazyAllocate) {
			allocateMemorySegmentsEager();
		} else {
			long maxNetworkBufferMB = memorySize >> 20;
			LOG.info("Cache memory manager is run in lazy allocate mode, configed {} MB for "
					+ "buffer pool (number of memory segments: {}, bytes per segment: {})",
				maxNetworkBufferMB, totalNumberOfMemorySegments, pageSize);
		}
		this.allocatedSegments = new ConcurrentHashMap<>();
	}

	private void allocateMemorySegmentsEager() {
		try {
			for (int i = 0; i < totalNumberOfMemorySegments; i++) {
				availableMemorySegments.add(MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize()));
			}
			numberOfAllocatedMemorySegments.set(totalNumberOfMemorySegments);
			numberOfAvailableMemorySegments.set(totalNumberOfMemorySegments);
		}
		catch (OutOfMemoryError err) {
			int allocated = availableMemorySegments.size();

			// free some memory
			availableMemorySegments.clear();
			numberOfAllocatedMemorySegments.set(0);
			numberOfAvailableMemorySegments.set(0);

			long requiredMb = ((long) getPageSize() * totalNumberOfMemorySegments) >> 20;
			long allocatedMb = ((long) getPageSize() * allocated) >> 20;
			long missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for CacheMemoryManager " +
				"(required (Mb): " + requiredMb +
				", allocated (Mb): " + allocatedMb +
				", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}

		long allocatedMb = ((long) getPageSize() * availableMemorySegments.size()) >> 20;

		LOG.info("Allocated {} MB for cache memory manager (number of memory segments: {}, bytes per segment: {}).",
			allocatedMb, availableMemorySegments.size(), memorySize);
	}

	private MemorySegment allocateSegmentLazy() {
		if (numberOfAllocatedMemorySegments.get() < totalNumberOfMemorySegments) {
			if (numberOfAllocatedMemorySegments.incrementAndGet() <= totalNumberOfMemorySegments) {
				try {
					MemorySegment segment = null;
					segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize());
					LOG.debug("Allocated a segment success with memorySegmentSize: {}.", getPageSize());
					return segment;
				} catch (OutOfMemoryError err) {
					numberOfAllocatedMemorySegments.decrementAndGet();

					long sizeInLong = (long) getPageSize();
					long configedMb = sizeInLong * totalNumberOfMemorySegments >> 20;
					long allocatedMb = sizeInLong * numberOfAllocatedMemorySegments.get() >> 20;
					long missingMb = configedMb - allocatedMb;
					throw new OutOfMemoryError("Could not allocate enough memory segments for CacheMemoryManager " +
						"(configed (Mb): " + configedMb +
						", allocated (Mb): " + allocatedMb +
						", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
				}
			} else {
				numberOfAllocatedMemorySegments.decrementAndGet();
			}
		}

		return null;
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The total allocated memory will not exceed its size limit, announced in the constructor.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param numPages The number of pages to allocate.
	 * @return A list with the memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	@Override
	public List<MemorySegment> allocatePages(Object owner, int numPages) throws MemoryAllocationException {
		List<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages);
		return segments;
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The total allocated memory will not exceed its size limit, announced in the constructor.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numberOfPages The number of pages to allocate.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	@Override
	public void allocatePages(
			Object owner,
			Collection<MemorySegment> target,
			int numberOfPages) throws MemoryAllocationException {
		try {
			while (true) {
				final Deadline deadline = Deadline.fromNow(requestMemorySegmentsTimeout);

				MemorySegment segment = null;
				if ((segment = internalRequestMemorySegment()) == null) {
					if (lazyAllocate) {
						segment = allocateSegmentLazy();
						if (segment == null) {
							LOG.warn("Allocate pages wait with request {} available {} for {}", numberOfPages, numberOfAllocatedMemorySegments.get(), owner);
							Thread.sleep(10);
						}
					} else {
						LOG.warn("Allocate pages wait with request {} available {} without lazy for {}", numberOfPages, numberOfAllocatedMemorySegments.get(), owner);
						Thread.sleep(10);
					}
				}

				if (segment != null) {
					segment.assignOwner(owner);
					target.add(segment);
				}

				if (target.size() >= numberOfPages) {
					LOG.debug("Requested {} segments success", numberOfPages);
					break;
				}

				if (!deadline.hasTimeLeft()) {
					throw new MemoryAllocationException(String.format("Timeout triggered when requesting memory segments, " +
							" you may increase the timeout which is %dms by setting the key '%s'.",
						requestMemorySegmentsTimeout.toMillis(),
						TaskManagerOptions.ALLOCATE_MEMORY_SEGMENTS_TIMEOUT.key()));
				}
			}
		} catch (Exception e) {
			throw new MemoryAllocationException(e);
		}

		Set<MemorySegment> segments = allocatedSegments.computeIfAbsent(owner, k -> new HashSet<>());
		segments.addAll(target);
	}

	@Nullable
	private MemorySegment internalRequestMemorySegment() {
		MemorySegment segment = availableMemorySegments.poll();
		if (segment != null) {
			numberOfAvailableMemorySegments.decrementAndGet();
		}
		return segment;
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released, it is only freed. If it is null or has no owner, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC. The segment will be returned to
	 * the memory pool, increasing its available limit for the later allocations.
	 *
	 * @param segment The segment to be released.
	 */
	@Override
	public void release(MemorySegment segment) {
		for (Map.Entry<Object, Set<MemorySegment>> entry : allocatedSegments.entrySet()) {
			if (entry.getValue().remove(segment)) {
				if (entry.getValue().isEmpty()) {
					allocatedSegments.remove(entry.getKey());
				}
				break;
			}
		}

		segment.freeOwner();
		segment.clear();
		availableMemorySegments.add(segment);
		numberOfAvailableMemorySegments.incrementAndGet();
	}

	/**
	 * Tries to release many memory segments together.
	 *
	 * <p>The segment is only freed and made eligible for reclamation by the GC. Each segment will be returned to
	 * the memory pool, increasing its available limit for the later allocations.
	 *
	 * @param segments The segments to be released.
	 */
	@Override
	public void release(Collection<MemorySegment> segments) {
		for (MemorySegment segment : segments) {
			release(segment);
		}
		segments.clear();
	}

	/**
	 * Releases all memory segments for the given owner.
	 *
	 * @param owner The owner memory segments are to be released.
	 */
	@Override
	public void releaseAll(Object owner) {
		Set<MemorySegment> segments = allocatedSegments.remove(owner);
		if (segments != null) {
			for (MemorySegment segment : segments) {
				segment.freeOwner();
				segment.clear();
			}
			availableMemorySegments.addAll(segments);
			numberOfAvailableMemorySegments.addAndGet(segments.size());
		}
	}

	/**
	 * Reserves a memory chunk of a certain size for an owner from this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param size size of memory to reserve.
	 * @throws MemoryReservationException Thrown, if this memory manager does not have the requested amount
	 *                                    of memory any more.
	 */
	@Override
	public void reserveMemory(Object owner, long size) throws MemoryReservationException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Releases a memory chunk of a certain size from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param size size of memory to release.
	 */
	@Override
	public void releaseMemory(Object owner, long size) {
		releaseAll(owner);
	}

	/**
	 * Releases all reserved memory chunks from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 */
	@Override
	public void releaseAllMemory(Object owner) {
		releaseAll(owner);
	}

	/**
	 * Acquires a shared memory resource, that uses all the memory of this memory manager.
	 * This method behaves otherwise exactly as {@link #getSharedMemoryResourceForManagedMemory(String, LongFunctionWithException, double)}.
	 */
	@Override
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getSharedMemoryResourceForManagedMemory(
			String type,
			LongFunctionWithException<T, Exception> initializer) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Acquires a shared memory resource, identified by a type string. If the resource already exists, this
	 * returns a descriptor to the resource. If the resource does not yet exist, the given memory fraction
	 * is reserved and the resource is initialized with that size.
	 *
	 * <p>The memory for the resource is reserved from the memory budget of this memory manager (thus
	 * determining the size of the resource), but resource itself is opaque, meaning the memory manager
	 * does not understand its structure.
	 *
	 * <p>The OpaqueMemoryResource object returned from this method must be closed once not used any further.
	 * Once all acquisitions have closed the object, the resource itself is closed.
	 *
	 * <p><b>Important:</b> The failure semantics are as follows: If the memory manager fails to reserve
	 * the memory, the external resource initializer will not be called. If an exception is thrown when the
	 * opaque resource is closed (last lease is released), the memory manager will still un-reserve the
	 * memory to make sure its own accounting is clean. The exception will need to be handled by the caller of
	 * {@link OpaqueMemoryResource#close()}. For example, if this indicates that native memory was not released
	 * and the process might thus have a memory leak, the caller can decide to kill the process as a result.
	 */
	@Override
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getSharedMemoryResourceForManagedMemory(
			String type,
			LongFunctionWithException<T, Exception> initializer,
			double fractionToInitializeWith) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Acquires a shared resource, identified by a type string. If the resource already exists, this
	 * returns a descriptor to the resource. If the resource does not yet exist, the method initializes
	 * a new resource using the initializer function and given size.
	 *
	 * <p>The resource opaque, meaning the memory manager does not understand its structure.
	 *
	 * <p>The OpaqueMemoryResource object returned from this method must be closed once not used any further.
	 * Once all acquisitions have closed the object, the resource itself is closed.
	 */
	@Override
	public <T extends AutoCloseable> OpaqueMemoryResource<T> getExternalSharedMemoryResource(
			String type,
			LongFunctionWithException<T, Exception> initializer,
			long numBytes) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * Checks if the memory manager's memory is completely available (nothing allocated at the moment).
	 *
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	@Override
	public boolean verifyEmpty() {
		return numberOfAvailableMemorySegments.get() == numberOfAllocatedMemorySegments.get();
	}

	/**
	 * Shuts the memory manager down, trying to release all the memory it managed. Depending
	 * on implementation details, the memory does not necessarily become reclaimable by the
	 * garbage collector, because there might still be references to allocated segments in the
	 * code that allocated them from the memory manager.
	 */
	@Override
	public void shutdown() {
		super.shutdown();

		availableMemorySegments.clear();
		numberOfAvailableMemorySegments.set(0);
		numberOfAllocatedMemorySegments.set(0);

		allocatedSegments.clear();
	}

	// ------------------------------------------------------------------------
	//  Properties, sizes and size conversions
	// ------------------------------------------------------------------------

	/**
	 * Gets the size of the pages handled by the memory manager.
	 *
	 * @return The size of the pages handled by the memory manager.
	 */
	@Override
	public int getPageSize() {
		return super.getPageSize();
	}

	/**
	 * Returns the total size of memory handled by this memory manager.
	 *
	 * @return The total size of memory.
	 */
	@Override
	public long getMemorySize() {
		return memorySize;
	}

	/**
	 * Returns the available amount of memory handled by this memory manager.
	 *
	 * @return The available amount of memory.
	 */
	@Override
	public long availableMemory() {
		return memorySize -
			(((long) (numberOfAllocatedMemorySegments.get() - numberOfAvailableMemorySegments.get()))
				* getPageSize());
	}

	/**
	 * Computes to how many pages the given number of bytes corresponds. If the given number of bytes is not an
	 * exact multiple of a page size, the result is rounded down, such that a portion of the memory (smaller
	 * than the page size) is not included.
	 *
	 * @param fraction the fraction of the total memory per slot
	 * @return The number of pages to which
	 */
	@Override
	public int computeNumberOfPages(double fraction) {
		if (fraction <= 0 || fraction > 1) {
			throw new IllegalArgumentException("The fraction of memory to allocate must within (0, 1].");
		}

		return (int) ((totalNumberOfMemorySegments / memoryPoolBucketCount) * fraction);
	}

	/**
	 * Computes the memory size corresponding to the fraction of all memory governed by this MemoryManager.
	 *
	 * @param fraction The fraction of all memory governed by this MemoryManager
	 * @return The memory size corresponding to the memory fraction
	 */
	@Override
	public long computeMemorySize(double fraction) {
		Preconditions.checkArgument(
			fraction > 0 && fraction <= 1,
			"The fraction of memory to allocate must within (0, 1], was: %s", fraction);

		return (long) Math.floor((memorySize / memoryPoolBucketCount) * fraction);
	}
}
