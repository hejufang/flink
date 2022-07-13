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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentDelegate;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.LongFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
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
public class MemoryBatchPoolManager extends MemoryManager {
	private static final Logger LOG = LoggerFactory.getLogger(MemoryBatchPoolManager.class);

	/** If the segment can be lazy allocated. */
	private final boolean lazyAllocate;

	/** The total memory size. */
	private final long memorySize;

	/** The total number of segments the memory manager can allocate. */
	private final int totalNumberOfMemorySegments;

	/** The total number of segment batches the memory manager can allocate. */
	private final int totalNumberOfMemorySegmentBatches;

	/** The total number of segment fragments the memory manager can allocate. */
	private final int totalNumberOfMemorySegmentFragments;

	/** The memory pool should be split to buckets for operator, the bucket count is slot count for global memory pool. */
	private final int memoryPoolBucketCount;

	/** Whether check owner in segment. */
	private final boolean checkOwnerInSegment;

	/** Track the number of segment batches that have been allocated. */
	private final AtomicInteger numberOfAllocatedMemorySegmentBatches = new AtomicInteger(0);

	/** Track the number of segment fragments that have been allocated. */
	private final AtomicInteger numberOfAllocatedMemorySegmentFragments = new AtomicInteger(0);

	/** Track the number of available segment batches in {@link #availableMemorySegmentBatches}. */
	private final AtomicInteger numberOfAvailableMemorySegmentBatches = new AtomicInteger(0);

	/** The segment batches queue managed by the memory manager. */
	private final Queue<Queue<MemorySegment>> availableMemorySegmentBatches;

	/**
	 * The segment fragments queue managed by the memory manager.
	 * Whenever we want to add/poll from here, we need a block, to ensure that when we unpack a batch, the number of
	 * segment fragments will not be larger than batch size.
	 * */
	private Queue<MemorySegment> availableMemorySegmentFragments;

	private final Object segmentFragmentsLock = new Object();

	/** Memory segments allocated per memory owner. */
	private final Map<Object, Set<MemorySegment>> allocatedSegments;

	/** The timeout for request memory segments. */
	private final Duration requestMemorySegmentsTimeout;

	/** The number of memory segment to form a batch, and this should always be larger than 1. */
	private final int batchSize;

	/** If this is set, the memory batch pool will only actually release segments after the task invoke is
	 * finished, i.e. {@link #releaseAll(Object)}.
	 */
	private final boolean releaseSegmentsFinallyEnable;

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize                 The total size of the memory to be managed by this memory manager.
	 * @param pageSize                   The size of the pages handed out by the memory manager.
	 * @param lazyAllocate               The memory manager allocate segments by lazy.
	 */
	@VisibleForTesting
	public MemoryBatchPoolManager(
			long memorySize,
			int pageSize,
			Duration requestMemorySegmentsTimeout,
			boolean lazyAllocate,
			int memoryPoolBucketCount,
			boolean checkOwnerInSegment,
			int batchSize,
			boolean releaseSegmentsFinallyEnable) {
		super(memorySize, pageSize, UnsafeMemoryBudget.MAX_SLEEPS_VERIFY_EMPTY);
		this.requestMemorySegmentsTimeout = requestMemorySegmentsTimeout;
		this.lazyAllocate = lazyAllocate;
		this.memoryPoolBucketCount = memoryPoolBucketCount;
		this.checkOwnerInSegment = checkOwnerInSegment;
		this.batchSize = batchSize;

		this.memorySize = memorySize;
		this.totalNumberOfMemorySegments = (int) (memorySize / pageSize);
		this.totalNumberOfMemorySegmentBatches = totalNumberOfMemorySegments / batchSize;
		this.totalNumberOfMemorySegmentFragments = totalNumberOfMemorySegments % batchSize;
		this.availableMemorySegmentBatches = new ConcurrentLinkedQueue<>();
		this.availableMemorySegmentFragments = new ArrayDeque<>(batchSize);
		if (!lazyAllocate) {
			allocateMemorySegmentsEager();
		} else {
			long maxNetworkBufferMB = memorySize >> 20;
			LOG.info("Cache memory manager is run in lazy allocate mode, configed {} MB for "
					+ "buffer pool (number of memory segments: {}, bytes per segment: {})",
				maxNetworkBufferMB, totalNumberOfMemorySegments, pageSize);
		}
		this.allocatedSegments = new ConcurrentHashMap<>();
		this.releaseSegmentsFinallyEnable = releaseSegmentsFinallyEnable;
	}

	private void allocateMemorySegmentsEager() {
		try {
			for (int i = 0; i < totalNumberOfMemorySegmentBatches; ++i) {
				Queue<MemorySegment> batch = new ArrayDeque<>(batchSize);
				for (int j = 0; j < batchSize; ++j) {
					batch.add(MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize()));
				}
				availableMemorySegmentBatches.add(batch);
			}
			numberOfAllocatedMemorySegmentBatches.set(totalNumberOfMemorySegmentBatches);
			numberOfAvailableMemorySegmentBatches.set(totalNumberOfMemorySegmentBatches);
			for (int i = 0; i < totalNumberOfMemorySegmentFragments; ++i) {
				availableMemorySegmentFragments.add(MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize()));
			}
			numberOfAllocatedMemorySegmentFragments.set(totalNumberOfMemorySegmentFragments);
		}
		catch (OutOfMemoryError err) {
			int allocated = availableMemorySegmentBatches.size() * batchSize + availableMemorySegmentFragments.size();

			// free some memory
			availableMemorySegmentBatches.clear();
			numberOfAllocatedMemorySegmentBatches.set(0);
			numberOfAvailableMemorySegmentBatches.set(0);

			availableMemorySegmentFragments.clear();
			numberOfAllocatedMemorySegmentFragments.set(0);

			long requiredMb = ((long) getPageSize() * totalNumberOfMemorySegments) >> 20;
			long allocatedMb = ((long) getPageSize() * allocated) >> 20;
			long missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for CacheMemoryManager " +
				"(required (Mb): " + requiredMb +
				", allocated (Mb): " + allocatedMb +
				", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}

		long allocatedMb = ((long) getPageSize() * totalNumberOfMemorySegments) >> 20;

		LOG.info("Allocated {} MB for cache memory manager (number of memory segments: {}, bytes per segment: {}).",
			allocatedMb, totalNumberOfMemorySegments, memorySize);
	}

	private MemorySegment allocateSegmentLazy() {
		if (numberOfAllocatedMemorySegmentFragments.get() < totalNumberOfMemorySegmentFragments) {
			if (numberOfAllocatedMemorySegmentFragments.incrementAndGet() <= totalNumberOfMemorySegmentFragments) {
				try {
					MemorySegment segment = null;
					segment = MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize());
					LOG.debug("Allocated a segment success with memorySegmentSize: {}.", getPageSize());
					return segment;
				} catch (OutOfMemoryError err) {
					numberOfAllocatedMemorySegmentFragments.decrementAndGet();

					long sizeInLong = (long) getPageSize();
					long configedMb = sizeInLong * totalNumberOfMemorySegments >> 20;
					long allocatedMb = sizeInLong * (numberOfAllocatedMemorySegmentBatches.get() * batchSize + numberOfAllocatedMemorySegmentFragments.get()) >> 20;
					long missingMb = configedMb - allocatedMb;
					throw new OutOfMemoryError("Could not allocate enough memory segments for CacheMemoryManager " +
						"(configed (Mb): " + configedMb +
						", allocated (Mb): " + allocatedMb +
						", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
				}
			} else {
				numberOfAllocatedMemorySegmentFragments.decrementAndGet();
			}
		}

		return null;
	}

	private Queue<MemorySegment> allocateSegmentBatchLazy() {
		if (numberOfAllocatedMemorySegmentBatches.get() < totalNumberOfMemorySegmentBatches) {
			if (numberOfAllocatedMemorySegmentBatches.incrementAndGet() <= totalNumberOfMemorySegmentBatches) {
				try {
					Queue<MemorySegment> batch = new ArrayDeque<>(batchSize);
					for (int i = 0; i < batchSize; ++i) {
						batch.add(MemorySegmentFactory.allocateOffHeapUnsafeMemory(getPageSize()));
					}
					LOG.debug("Allocated {} segments success with memorySegmentSize: {}.", batchSize, getPageSize());
					return batch;
				} catch (OutOfMemoryError err) {
					numberOfAllocatedMemorySegmentBatches.decrementAndGet();

					long sizeInLong = (long) getPageSize();
					long configedMb = sizeInLong * totalNumberOfMemorySegments >> 20;
					long allocatedMb = sizeInLong * (numberOfAllocatedMemorySegmentBatches.get() * batchSize + numberOfAllocatedMemorySegmentFragments.get()) >> 20;
					long missingMb = configedMb - allocatedMb;
					throw new OutOfMemoryError("Could not allocate enough memory segments for CacheMemoryManager " +
						"(configed (Mb): " + configedMb +
						", allocated (Mb): " + allocatedMb +
						", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
				}
			} else {
				numberOfAllocatedMemorySegmentBatches.decrementAndGet();
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
		int numberOfSegmentBatchToAllocate = numberOfPages / batchSize;
		int numberOfSegmentFragmentToAllocate = numberOfPages % batchSize;
		try {
			final Deadline deadline = Deadline.fromNow(requestMemorySegmentsTimeout);
			while (true) {
				if (numberOfSegmentBatchToAllocate > 0) {
					Queue<MemorySegment> batch = null;
					if ((batch = internalRequestMemorySegmentBatch()) == null) {
						if (lazyAllocate) {
							batch = allocateSegmentBatchLazy();
							if (batch == null) {
								Thread.sleep(10);
							}
						} else {
							Thread.sleep(10);
						}
					}

					if (batch != null) {
						MemorySegment segment = null;
						while ((segment = batch.poll()) != null) {
							segment.assignOwner(owner);
							target.add(checkOwnerInSegment ? new MemorySegmentDelegate(owner, segment) : segment);
						}
						--numberOfSegmentBatchToAllocate;
					}
				}

				if (numberOfSegmentFragmentToAllocate > 0) {
					MemorySegment segment = null;

					synchronized (segmentFragmentsLock) {
						if ((segment = internalRequestMemorySegment()) == null) {
							if (lazyAllocate) {
								if ((segment = allocateSegmentLazy()) == null) {
									segment = internalUnpackMemorySegmentBatch();
								}
								if (segment == null) {
									Thread.sleep(10);
								}
							} else if ((segment = internalUnpackMemorySegmentBatch()) == null) {
								Thread.sleep(10);
							}
						}
					}

					if (segment != null) {
						segment.assignOwner(owner);
						target.add(checkOwnerInSegment ? new MemorySegmentDelegate(owner, segment) : segment);
						--numberOfSegmentFragmentToAllocate;
					}
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
		} catch (Throwable e) {
			LOG.error("Allocate {} segments from pool manager failed", numberOfPages, e);
			// Release the allocated pages to the pool.
			ArrayDeque<MemorySegment> segmentsToRecycle = new ArrayDeque<>(target.size());
			for (MemorySegment segment : target) {
				segmentsToRecycle.add(cleanupSegment(segment));
			}
			target.clear();
			internalRecycleMemorySegments(segmentsToRecycle);
			throw new MemoryAllocationException(e);
		}

		Set<MemorySegment> segments = allocatedSegments.computeIfAbsent(owner, k -> new HashSet<>());
		segments.addAll(target);
	}

	@Nullable
	private Queue<MemorySegment> internalRequestMemorySegmentBatch() {
		Queue<MemorySegment> batch = availableMemorySegmentBatches.poll();
		if (batch != null) {
			numberOfAvailableMemorySegmentBatches.decrementAndGet();
		}
		return batch;
	}

	@Nullable
	private MemorySegment internalRequestMemorySegment() {
		assert Thread.holdsLock(segmentFragmentsLock);

		MemorySegment segment = availableMemorySegmentFragments.poll();

		return segment;
	}

	private void internalRecycleMemorySegment(MemorySegment segment) {
		synchronized (segmentFragmentsLock) {
			availableMemorySegmentFragments.add(segment);
			if (availableMemorySegmentFragments.size() == batchSize) {
				internalPackMemorySegmentBatch();
			}
		}
	}

	private void internalRecycleMemorySegments(Queue<MemorySegment> segments) {
		while (segments.size() >= batchSize) {
			Queue<MemorySegment> batch = new ArrayDeque<>(batchSize);
			for (int i = 0; i < batchSize; ++i) {
				batch.add(segments.poll());
			}
			availableMemorySegmentBatches.add(batch);
			numberOfAvailableMemorySegmentBatches.getAndIncrement();
		}
		synchronized (segmentFragmentsLock) {
			MemorySegment segment = null;
			while ((segment = segments.poll()) != null) {
				availableMemorySegmentFragments.add(segment);
				if (availableMemorySegmentFragments.size() == batchSize) {
					internalPackMemorySegmentBatch();
				}
			}
		}
	}

	/**
	 * If we need a memory segment, and {@link #availableMemorySegmentFragments} is empty, we can try to
	 * unpack a batch from {@link #availableMemorySegmentBatches} for that.
	 *
	 * @return A memory segment, or null is no batch is available.
	 */
	@Nullable
	private MemorySegment internalUnpackMemorySegmentBatch() {
		assert Thread.holdsLock(segmentFragmentsLock);

		Queue<MemorySegment> batch = internalRequestMemorySegmentBatch();

		if (batch == null && lazyAllocate) {
			batch = allocateSegmentBatchLazy();
		}

		if (batch == null) {
			return null;
		}

		MemorySegment resultSegment = batch.poll();
		MemorySegment segment = null;
		while ((segment = batch.poll()) != null) {
			availableMemorySegmentFragments.add(segment);
		}

		return resultSegment;
	}

	private void internalPackMemorySegmentBatch() {
		assert Thread.holdsLock(segmentFragmentsLock);

		availableMemorySegmentBatches.add(availableMemorySegmentFragments);
		numberOfAvailableMemorySegmentBatches.getAndIncrement();

		availableMemorySegmentFragments = new ArrayDeque<>(batchSize);
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
		if (releaseSegmentsFinallyEnable) {
			return;
		}
		allocatedSegments.computeIfPresent(segment.getOwner(), (o, segsForOwner) -> {
			if (segsForOwner.remove(segment)) {
				internalRecycleMemorySegment(cleanupSegment(segment));
			}
			return segsForOwner.isEmpty() ? null : segsForOwner;
		});
	}

	private MemorySegment cleanupSegment(MemorySegment segment) {
		if (checkOwnerInSegment) {
			MemorySegmentDelegate delegate = (MemorySegmentDelegate) segment;
			MemorySegment realSegment = delegate.getSegment();
			delegate.clear();
			delegate.freeOwner();
			return realSegment;
		}
		segment.clear();
		segment.freeOwner();
		return segment;
	}

	/**
	 * Free the given segment with owner.
	 *
	 * @param owner the given owner
	 * @param segment the release segment
	 */
	@Override
	public void release(Object owner, MemorySegment segment) {
		if (releaseSegmentsFinallyEnable) {
			return;
		}
		allocatedSegments.computeIfPresent(owner, (o, segsForOwner) -> {
			if (segsForOwner.remove(segment)) {
				internalRecycleMemorySegment(cleanupSegment(segment));
			}

			return segsForOwner.isEmpty() ? null : segsForOwner;
		});
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
		if (releaseSegmentsFinallyEnable) {
			segments.clear();
			return;
		}
		Queue<MemorySegment> segmentsToRecycle = new ArrayDeque<>(segments.size());
		for (MemorySegment segment : segments) {
			allocatedSegments.computeIfPresent(segment.getOwner(), (o, segsForOwner) -> {
				if (segsForOwner.remove(segment)) {
					segmentsToRecycle.add(cleanupSegment(segment));
				}
				return segsForOwner.isEmpty() ? null : segsForOwner;
			});
		}
		segments.clear();
		internalRecycleMemorySegments(segmentsToRecycle);
	}

	/**
	 * Release segments with given owner, the owner will be used to check whether the segment is free multiple times.
	 *
	 * @param owner the given owner
	 * @param segments the segments to release
	 */
	@Override
	public void release(Object owner, Collection<MemorySegment> segments) {
		if (releaseSegmentsFinallyEnable) {
			segments.clear();
			return;
		}
		Queue<MemorySegment> segmentsToRecycle = new ArrayDeque<>(segments.size());
		for (MemorySegment segment : segments) {
			allocatedSegments.computeIfPresent(owner, (o, segsForOwner) -> {
				if (segsForOwner.remove(segment)) {
					segmentsToRecycle.add(cleanupSegment(segment));
				}
				return segsForOwner.isEmpty() ? null : segsForOwner;
			});
		}
		segments.clear();
		internalRecycleMemorySegments(segmentsToRecycle);
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
			Queue<MemorySegment> segmentsToRecycle = new ArrayDeque<>(segments.size());
			for (MemorySegment segment : segments) {
				segmentsToRecycle.add(cleanupSegment(segment));
			}
			segments.clear();
			internalRecycleMemorySegments(segmentsToRecycle);
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
		return numberOfAvailableMemorySegmentBatches.get() == numberOfAllocatedMemorySegmentBatches.get()
				&& availableMemorySegmentFragments.size() == numberOfAllocatedMemorySegmentFragments.get();
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

		availableMemorySegmentBatches.clear();
		numberOfAllocatedMemorySegmentBatches.set(0);
		numberOfAvailableMemorySegmentBatches.set(0);

		availableMemorySegmentFragments.clear();
		numberOfAllocatedMemorySegmentFragments.set(0);

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
			(((long) ((numberOfAllocatedMemorySegmentBatches.get() - numberOfAvailableMemorySegmentBatches.get()) * batchSize
				+ numberOfAllocatedMemorySegmentFragments.get() - availableMemorySegmentFragments.size()))
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

	@VisibleForTesting
	public Map<Object, Set<MemorySegment>> getAllocatedSegments() {
		return allocatedSegments;
	}
}
