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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is a {@link NetworkBufferPool} that uses memory segment package as the unit consisting of
 * several memory segments to manager. So, memory segment will never make sense,
 * please only focus on packages.
 * Only work in OLAP.
 */
public class NetworkBufferPackagePool implements NetworkBufferPool {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPackagePool.class);

	private final int memorySegmentSize;

	private volatile boolean isDestroyed;

	// ---- Managed buffer pools ----------------------------------------------

	private final Object factoryLock = new Object();

	private final Set<LocalBufferPackagePool> allBufferPools = new HashSet<>();

	private final Set<LocalBufferPackagePool> resizableBufferPools = new HashSet<>();

	private final int numberOfSegmentsToRequest;

	private final Duration requestSegmentsTimeout;

	private final boolean lazyAllocate;

	private final long requestNetworkSegmentTimeoutMills;

	private final boolean simpleRedistributeEnable;

	private final double simpleRedistributeHighWaterMark;

	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

	/**
	 * Use memory segment package as the unit consisting of several memory segments to manager.
	 * So, memory segment will never make sense, please only focus on packages.
	 */
	private final int numberOfMemorySegmentsPerPackage;

	private final int totalNumberOfMemorySegmentPackages;

	private int numTotalRequiredPackages;

	private AtomicInteger numberOfAllocatedMemorySegmentPackages = new AtomicInteger(0);

	private final ArrayDeque<ArrayDeque<MemorySegment>> availableMemorySegmentPackages;

	public NetworkBufferPackagePool(
			int numberOfSegmentsToAllocate,
			int segmentSize,
			int numberOfSegmentsToRequest,
			Duration requestSegmentsTimeout,
			boolean lazyAllocate,
			Duration requestNetworkSegmentTimeout,
			boolean simpleRedistributeEnable,
			double simpleRedistributeHighWaterMark,
			int numberOfMemorySegmentsPerPackage) {
		super();
		checkArgument(numberOfMemorySegmentsPerPackage > 1, "The number of memory segments per package should be larger than 1.");
		checkArgument(numberOfSegmentsToAllocate >= numberOfMemorySegmentsPerPackage, "The number of memory segments should NOT less then package size.");
		this.totalNumberOfMemorySegmentPackages = numberOfSegmentsToAllocate / numberOfMemorySegmentsPerPackage;

		this.memorySegmentSize = segmentSize;
		this.simpleRedistributeEnable = simpleRedistributeEnable;
		this.simpleRedistributeHighWaterMark = simpleRedistributeHighWaterMark;

		checkArgument(numberOfSegmentsToRequest > 0, "The number of required buffers should be larger than 0.");
		this.numberOfSegmentsToRequest = numberOfSegmentsToRequest;

		Preconditions.checkNotNull(requestSegmentsTimeout);
		checkArgument(requestSegmentsTimeout.toMillis() > 0,
			"The timeout for requesting exclusive buffers should be positive.");
		this.requestSegmentsTimeout = requestSegmentsTimeout;
		this.lazyAllocate = lazyAllocate;
		this.requestNetworkSegmentTimeoutMills = requestNetworkSegmentTimeout.toMillis();

		this.numberOfMemorySegmentsPerPackage = numberOfMemorySegmentsPerPackage;

		try {
			this.availableMemorySegmentPackages = new ArrayDeque<>(this.totalNumberOfMemorySegmentPackages);
			// To check whether an OutOfMemoryError will occur.
			for (int i = 0; i < totalNumberOfMemorySegmentPackages; ++i) {
				this.availableMemorySegmentPackages.add(new ArrayDeque<>(numberOfMemorySegmentsPerPackage));
			}
			this.availableMemorySegmentPackages.clear();
		}
		catch (OutOfMemoryError err) {
			throw new OutOfMemoryError("Could not allocate buffer queue of length "
				+ numberOfSegmentsToAllocate + " - " + err.getMessage());
		}

		if (!lazyAllocate) {
			allocateMemorySegmentPackagesEager();
		} else {
			long maxNetworkBufferMB = (memorySegmentSize * totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage) >> 20;
			LOG.info("Network buffer pool is run in lazy allocate mode, configed {} MB for network "
					+ "buffer pool (number of memory segments: {}, bytes per segment: {})",
				maxNetworkBufferMB, totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage, memorySegmentSize);
		}
	}

	private void allocateMemorySegmentPackagesEager() {
		final long sizeInLong = (long) memorySegmentSize;
		try {
			for (int i = 0; i < totalNumberOfMemorySegmentPackages; ++i) {
				ArrayDeque<MemorySegment> memorySegmentPackage = new ArrayDeque<>(numberOfMemorySegmentsPerPackage);
				for (int j = 0; j < numberOfMemorySegmentsPerPackage; ++j) {
					memorySegmentPackage.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(memorySegmentSize, null));
				}
				availableMemorySegmentPackages.add(memorySegmentPackage);
			}
			numberOfAllocatedMemorySegmentPackages.set(totalNumberOfMemorySegmentPackages);
		}
		catch (OutOfMemoryError err) {
			int allocated = availableMemorySegmentPackages.size() * numberOfMemorySegmentsPerPackage;

			// free some memory
			availableMemorySegmentPackages.clear();
			numberOfAllocatedMemorySegmentPackages.set(0);

			long requiredMb = (sizeInLong * totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage) >> 20;
			long allocatedMb = (sizeInLong * allocated) >> 20;
			long missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for NetworkBufferPool " +
				"(required (Mb): " + requiredMb +
				", allocated (Mb): " + allocatedMb +
				", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}

		availabilityHelper.resetAvailable();

		long allocatedMb = (sizeInLong * totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage) >> 20;

		LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
			allocatedMb, totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage, memorySegmentSize);
	}

	@Nullable
	@Override
	public MemorySegment requestMemorySegment() {
		throw new UnsupportedOperationException();
	}

	@Nullable
	public ArrayDeque<MemorySegment> requestMemorySegmentPackage() {
		ArrayDeque<MemorySegment> memorySegmentPackage = null;
		synchronized (availableMemorySegmentPackages) {
			memorySegmentPackage = internalRequestMemorySegmentPackage();
			if (memorySegmentPackage != null) {
				return memorySegmentPackage;
			}
		}

		if (lazyAllocate) {
			memorySegmentPackage = allocateMemorySegmentPackageLazy();
		}

		return memorySegmentPackage;
	}

	private ArrayDeque<MemorySegment> allocateMemorySegmentPackageLazy() {
		if (numberOfAllocatedMemorySegmentPackages.get() < totalNumberOfMemorySegmentPackages) {
			if (numberOfAllocatedMemorySegmentPackages.incrementAndGet() <= totalNumberOfMemorySegmentPackages) {
				try {
					ArrayDeque<MemorySegment> segmentPackage = new ArrayDeque<>(numberOfMemorySegmentsPerPackage);
					for (int i = 0; i < numberOfMemorySegmentsPerPackage; ++i) {
						segmentPackage.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(memorySegmentSize, null));
					}
					LOG.debug("Allocated {} segments success with memorySegmentSize: {}.", numberOfMemorySegmentsPerPackage, memorySegmentSize);
					return segmentPackage;
				} catch (OutOfMemoryError err) {
					numberOfAllocatedMemorySegmentPackages.decrementAndGet();

					long sizeInLong = (long) memorySegmentSize;
					long configedMb = sizeInLong * totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage >> 20;
					long allocatedMb = sizeInLong * numberOfAllocatedMemorySegmentPackages.get() * numberOfMemorySegmentsPerPackage >> 20;
					long missingMb = configedMb - allocatedMb;
					throw new OutOfMemoryError("Could not allocate enough memory segments for NetworkBufferPool " +
						"(configed (Mb): " + configedMb +
						", allocated (Mb): " + allocatedMb +
						", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
				}
			} else {
				numberOfAllocatedMemorySegmentPackages.decrementAndGet();
			}
		}

		return null;
	}

	@Override
	public void recycle(MemorySegment segment) {
		throw new UnsupportedOperationException();
	}

	public void recyclePackage(ArrayDeque<MemorySegment> segmentPackage) {
		internalRecycleMemorySegmentPackages(Collections.singleton(checkNotNull(segmentPackage)));
	}

	@Override
	public List<MemorySegment> requestMemorySegments() throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * This is only for exclusive segments.
	 *
	 * @param numPackages
	 * 		The number of packages to request.
	 * @return The packages that is requested.
	 * @throws IOException
	 */
	public ArrayDeque<ArrayDeque<MemorySegment>> requestMemorySegmentPackages(int numPackages) throws IOException {
		// The caller local pool may not increase its size.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer package pool has already been destroyed.");
			}

			tryRedistributePackages(numPackages);
		}

		final ArrayDeque<ArrayDeque<MemorySegment>> segmentPackages = new ArrayDeque<>(numPackages);
		try {
			final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
			while (true) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				if (segmentPackages.size() >= numPackages) {
					LOG.debug("Requested {} segments success", numPackages * numberOfMemorySegmentsPerPackage);
					break;
				}

				ArrayDeque<MemorySegment> segmentPackage = null;

				synchronized (availableMemorySegmentPackages) {
					if ((segmentPackage = internalRequestMemorySegmentPackage()) == null) {
						if (lazyAllocate) {
							segmentPackage = allocateMemorySegmentPackageLazy();
							if (segmentPackage == null) {
								availableMemorySegmentPackages.wait(2000);
							}
						} else {
							availableMemorySegmentPackages.wait(2000);
						}
					}
				}

				if (segmentPackage != null) {
					segmentPackages.add(segmentPackage);
				}

				if (!deadline.hasTimeLeft()) {
					throw new IOException(String.format("Timeout triggered when requesting exclusive buffers: %s, " +
							" or you may increase the timeout which is %dms by setting the key '%s'.",
						getConfigDescription(),
						requestSegmentsTimeout.toMillis(),
						NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS.key()));
				}
			}
		} catch (Throwable e) {
			try {
				recycleMemorySegmentPackages(segmentPackages, numPackages);
			} catch (IOException inner) {
				e.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(e);
		}

		return segmentPackages;
	}

	@Nullable
	private ArrayDeque<MemorySegment> internalRequestMemorySegmentPackage() {
		assert Thread.holdsLock(availableMemorySegmentPackages);

		final ArrayDeque<MemorySegment> segmentPackage = availableMemorySegmentPackages.poll();
		if (availableMemorySegmentPackages.isEmpty() && segmentPackage != null) {
			availabilityHelper.resetUnavailable();
		}
		return segmentPackage;
	}

	@Override
	public void recycleMemorySegments(Collection<MemorySegment> segments) throws IOException {
		throw new UnsupportedOperationException();
	}

	/**
	 * For exclusive segments only.
	 */
	public void recycleMemorySegmentPackages(Collection<ArrayDeque<MemorySegment>> segmentPackages) throws IOException {
		recycleMemorySegmentPackages(segmentPackages, segmentPackages.size());
	}

	private void recycleMemorySegmentPackages(Collection<ArrayDeque<MemorySegment>> segmentPackages, int size) throws IOException {
		internalRecycleMemorySegmentPackages(segmentPackages);
		synchronized (factoryLock) {
			numTotalRequiredPackages -= size;

			// note: if this fails, we're fine for the buffer pool since we already recycled the segments
			if (!simpleRedistributeEnable) {
				redistributePackages();
			}
		}
	}

	private void internalRecycleMemorySegmentPackages(Collection<ArrayDeque<MemorySegment>> segmentPackages) {
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegmentPackages) {
			if (availableMemorySegmentPackages.isEmpty() && !segmentPackages.isEmpty()) {
				toNotify = availabilityHelper.getUnavailableToResetAvailable();
			}
			availableMemorySegmentPackages.addAll(segmentPackages);
			availableMemorySegmentPackages.notifyAll();
		}

		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	@Override
	public void destroy() {
		synchronized (factoryLock) {
			isDestroyed = true;
		}

		synchronized (availableMemorySegmentPackages) {
			ArrayDeque<MemorySegment> segmentPackage;
			while ((segmentPackage = availableMemorySegmentPackages.poll()) != null) {
				MemorySegment segment;
				while ((segment = segmentPackage.poll()) != null) {
					segment.free();
				}
			}
			numberOfAllocatedMemorySegmentPackages.decrementAndGet();
		}
	}

	@Override
	public boolean isDestroyed() {
		return isDestroyed;
	}

	@Override
	public int getTotalNumberOfMemorySegments() {
		return isDestroyed() ? 0 : totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage;
	}

	@Override
	public long getTotalMemory() {
		return (long) getTotalNumberOfMemorySegments() * memorySegmentSize;
	}

	@Override
	public int getNumTotalRequiredBuffers() {
		return numTotalRequiredPackages * numberOfMemorySegmentsPerPackage;
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegmentPackages) {
			return availableMemorySegmentPackages.size() * numberOfMemorySegmentsPerPackage;
		}
	}

	@Override
	public long getAvailableMemory() {
		return (long) getNumberOfAvailableMemorySegments() * memorySegmentSize;
	}

	@Override
	public int getNumberOfUsedMemorySegments() {
		return getNumberOfAllocatedMemorySegments() - getNumberOfAvailableMemorySegments();
	}

	public long getUsedMemory() {
		return (long) getNumberOfUsedMemorySegments() * memorySegmentSize;
	}

	@Override
	public int getNumberOfRegisteredBufferPools() {
		synchronized (factoryLock) {
			return allBufferPools.size();
		}
	}

	@Override
	public int getNumberOfAllocatedMemorySegments() {
		return numberOfAllocatedMemorySegmentPackages.get() * numberOfMemorySegmentsPerPackage;
	}

	@Override
	public long getAllocatedMemory() {
		return (long) getNumberOfAllocatedMemorySegments() * memorySegmentSize;
	}

	@Override
	public int countBuffers() {
		int buffers = 0;

		synchronized (factoryLock) {
			for (BufferPool bp : allBufferPools) {
				buffers += bp.getNumBuffers();
			}
		}

		return buffers;
	}

	/**
	 * Returns a future that is completed when there are free segments
	 * in this pool.
	 */
	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (!lazyAllocate) {
			return availabilityHelper.getAvailableFuture();
		} else {
			// this should always be availabe because we use lazy mode
			return AVAILABLE;
		}
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException {
		return internalCreateBufferPool(
			numRequiredBuffers,
			maxUsedBuffers,
			null,
			0,
			Integer.MAX_VALUE);
	}

	@Override
	public BufferPool createBufferPool(
			int numRequiredBuffers,
			int maxUsedBuffers,
			BufferPoolOwner bufferPoolOwner,
			int numSubpartitions,
			int maxBuffersPerChannel) throws IOException {
		return internalCreateBufferPool(
			numRequiredBuffers,
			maxUsedBuffers,
			bufferPoolOwner,
			numSubpartitions,
			maxBuffersPerChannel);
	}

	private BufferPool internalCreateBufferPool(
			int numRequiredBuffers,
			int maxUsedBuffers,
			@Nullable BufferPoolOwner bufferPoolOwner,
			int numSubpartitions,
			int maxBuffersPerChannel) throws IOException {

		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			// Note in LocalBufferPackagePool, it will set the min/max size to adapt the package size,
			// that is (min/max size >= package size && min/max size % package size == 0) will always hold.
			// Since maxUsedBuffers may be Integer.MAX_VALUE and will cause overflow, we need long to calculate and an extra check.
			// If this happens, we try to adapt the max size of LocalBufferPackagePool lower to multiple of packageSize, instead of upper.
			if (((long) maxUsedBuffers + numberOfMemorySegmentsPerPackage) > Integer.MAX_VALUE) {
				maxUsedBuffers -= numberOfMemorySegmentsPerPackage;
			}
			int numRequiredPackages = (numRequiredBuffers + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage;
			int maxUsedPackages = (maxUsedBuffers + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage;
			if (simpleRedistributeEnable && numTotalRequiredPackages + numRequiredPackages > totalNumberOfMemorySegmentPackages) {
				// Return excess memorySegment in resizablePools if available memorySegmentPackages not enough.
				LOG.info("trigger return ExcessMemorySegments, numTotalRequiredBuffers: {}, numberOfSegmentsToRequest: {}, totalNumberOfMemorySegments: {}",
					numTotalRequiredPackages * numberOfMemorySegmentsPerPackage,
					numRequiredPackages * numberOfMemorySegmentsPerPackage,
					totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage);
				returnExcessMemorySegmentPackages();
			}

			// Ensure that the number of required buffers can be satisfied.
			// With dynamic memory management this should become obsolete.

			if (numTotalRequiredPackages + numRequiredPackages > totalNumberOfMemorySegmentPackages) {
				throw new IOException(String.format("Insufficient number of network buffers: " +
						"required %d, but only %d available. %s.",
					numRequiredPackages * numberOfMemorySegmentsPerPackage,
					(totalNumberOfMemorySegmentPackages - numTotalRequiredPackages) * numberOfMemorySegmentsPerPackage,
					getConfigDescription()));
			}

			this.numTotalRequiredPackages += numRequiredPackages;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.

			boolean useMaxUsedPackages = false;
			if (simpleRedistributeEnable) {
				final int numAvailableMemorySegmentPackage = totalNumberOfMemorySegmentPackages - numTotalRequiredPackages;
				int maxExcessPackage = Math.max(maxUsedPackages - numRequiredPackages, 0);
				if (1.0 * (numTotalRequiredPackages + maxExcessPackage) / totalNumberOfMemorySegmentPackages < simpleRedistributeHighWaterMark && numAvailableMemorySegmentPackage >= maxExcessPackage) {
					numTotalRequiredPackages += maxExcessPackage;
					useMaxUsedPackages = true;
				}
			}
			int initPoolSize = useMaxUsedPackages ? maxUsedBuffers : numRequiredBuffers;
			LocalBufferPackagePool localBufferPackagePool =
				new LocalBufferPackagePool(
					this,
					initPoolSize,
					numRequiredBuffers,
					maxUsedBuffers,
					bufferPoolOwner,
					numSubpartitions,
					maxBuffersPerChannel,
					numberOfSegmentsToRequest,
					numberOfMemorySegmentsPerPackage);

			allBufferPools.add(localBufferPackagePool);

			if (useMaxUsedPackages) {
				resizableBufferPools.add(localBufferPackagePool);
			}

			try {
				if (!simpleRedistributeEnable) {
					redistributePackages();
				}
			} catch (IOException e) {
				try {
					destroyBufferPool(localBufferPackagePool);
				} catch (IOException inner) {
					e.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(e);
			}

			return localBufferPackagePool;
		}
	}

	@Override
	public void destroyBufferPool(BufferPool bufferPool) throws IOException {
		if (!(bufferPool instanceof LocalBufferPackagePool)) {
			throw new IllegalArgumentException("bufferPool is no LocalBufferPackagePool");
		}

		synchronized (factoryLock) {
			if (allBufferPools.remove(bufferPool)) {
				if (!simpleRedistributeEnable) {
					numTotalRequiredPackages -= bufferPool.getNumberOfRequiredMemorySegments() / numberOfMemorySegmentsPerPackage;
					redistributePackages();
				} else {
					numTotalRequiredPackages -= bufferPool.getNumBuffers() / numberOfMemorySegmentsPerPackage;
					resizableBufferPools.remove(bufferPool);
				}
			}
		}
	}

	/**
	 * Destroys all buffer pools that allocate their buffers from this
	 * buffer pool (created via {@link #createBufferPool(int, int)}).
	 */
	@Override
	public void destroyAllBufferPools() {
		synchronized (factoryLock) {
			// create a copy to avoid concurrent modification exceptions
			LocalBufferPackagePool[] poolsCopy = allBufferPools.toArray(new LocalBufferPackagePool[allBufferPools.size()]);

			for (LocalBufferPackagePool pool : poolsCopy) {
				pool.lazyDestroy();
			}

			// some sanity checks
			if (allBufferPools.size() > 0 || numTotalRequiredPackages > 0) {
				throw new IllegalStateException("NetworkBufferPackagePool is not empty after destroying all LocalBufferPackagePools");
			}
		}
	}

	private void tryRedistributePackages(int numPackages) throws IOException {
		assert Thread.holdsLock(factoryLock);

		if (simpleRedistributeEnable && numTotalRequiredPackages + numPackages > totalNumberOfMemorySegmentPackages) {
			LOG.info("trigger return ExcessMemorySegments, numTotalRequiredBuffers: {}, numberOfSegmentsToRequest: {}, totalNumberOfMemorySegments: {}",
				numTotalRequiredPackages * numberOfMemorySegmentsPerPackage,
				numPackages * numberOfMemorySegmentsPerPackage,
				totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage);
			returnExcessMemorySegmentPackages();
		}

		if (numTotalRequiredPackages + numPackages > totalNumberOfMemorySegmentPackages) {
			throw new IOException(String.format("Insufficient number of network buffers: " +
					"required %d, but only %d available. %s.",
				numPackages * numberOfMemorySegmentsPerPackage,
				(totalNumberOfMemorySegmentPackages - numTotalRequiredPackages) * numberOfMemorySegmentsPerPackage,
				getConfigDescription()));
		}

		this.numTotalRequiredPackages += numPackages;

		try {
			if (!simpleRedistributeEnable) {
				redistributePackages();
			}
		} catch (Throwable t) {
			this.numTotalRequiredPackages -= numPackages;

			try {
				redistributePackages();
			} catch (IOException inner) {
				t.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(t);
		}
	}

	@Override
	public void tryResizeLocalBufferPool(LocalBufferPool localBufferPool) throws IOException {
		throw new UnsupportedOperationException();
	}

	public void tryResizeLocalBufferPool(LocalBufferPackagePool localBufferPackagePool) throws IOException {
		if (!simpleRedistributeEnable) {
			return;
		}
		synchronized (factoryLock) {
			LOG.debug("try resize local buffer package pool, numTotalRequiredBuffers: {}, totalNumberOfMemorySegments: {}",
				numberOfAllocatedMemorySegmentPackages.get() * numberOfMemorySegmentsPerPackage,
				totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage);
			if (1.0 * (numTotalRequiredPackages + 1) / totalNumberOfMemorySegmentPackages > simpleRedistributeHighWaterMark) {
				return;
			}
			if (localBufferPackagePool.tryIncNumPackages()) {
				++numTotalRequiredPackages;
				resizableBufferPools.add(localBufferPackagePool);
			}
		}
	}

	private void redistributePackages() throws IOException {
		assert Thread.holdsLock(factoryLock);

		// Note LocalBufferPackagePool will always keep min/max size multiple of package size,
		// so we can just keep the original logic, and just think that the unit is different.

		// All buffers, which are not among the required ones
		final int numAvailableMemorySegmentPackage = totalNumberOfMemorySegmentPackages - numTotalRequiredPackages;

		if (numAvailableMemorySegmentPackage == 0) {
			// in this case, we need to redistribute buffers so that every pool gets its minimum
			for (LocalBufferPackagePool bufferPool : allBufferPools) {
				bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
			}
			return;
		}

		/*
		 * With buffer pools being potentially limited, let's distribute the available memory
		 * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
		 * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
		 * it may be less. Based on this and the sum of all these values (totalCapacity), we build
		 * a ratio that we use to distribute the buffers.
		 */

		long totalCapacity = 0; // long to avoid int overflow

		for (LocalBufferPackagePool bufferPool : allBufferPools) {
			int excessMax = (bufferPool.getMaxNumberOfMemorySegments() - bufferPool.getNumberOfRequiredMemorySegments()) / numberOfMemorySegmentsPerPackage;
			totalCapacity += Math.min(numAvailableMemorySegmentPackage, excessMax);
		}

		// no capacity to receive additional buffers?
		if (totalCapacity == 0) {
			return; // necessary to avoid div by zero when nothing to re-distribute
		}

		// since one of the arguments of 'min(a,b)' is a positive int, this is actually
		// guaranteed to be within the 'int' domain
		// (we use a checked downCast to handle possible bugs more gracefully).
		final int memorySegmentPackagesToDistribute = MathUtils.checkedDownCast(
			Math.min(numAvailableMemorySegmentPackage, totalCapacity));

		long totalPartsUsed = 0; // of totalCapacity
		int numDistributedMemorySegmentPackage = 0;
		for (LocalBufferPackagePool bufferPool : allBufferPools) {
			int excessMax = (bufferPool.getMaxNumberOfMemorySegments() - bufferPool.getNumberOfRequiredMemorySegments()) / numberOfMemorySegmentsPerPackage;

			// shortcut
			if (excessMax == 0) {
				continue;
			}

			totalPartsUsed += Math.min(numAvailableMemorySegmentPackage, excessMax);

			// avoid remaining buffers by looking at the total capacity that should have been
			// re-distributed up until here
			// the downcast will always succeed, because both arguments of the subtraction are in the 'int' domain
			final int mySize = MathUtils.checkedDownCast(
				memorySegmentPackagesToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegmentPackage);

			numDistributedMemorySegmentPackage += mySize;
			bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize * numberOfMemorySegmentsPerPackage);
		}

		assert (totalPartsUsed == totalCapacity);
		assert (numDistributedMemorySegmentPackage == memorySegmentPackagesToDistribute);
	}

	@VisibleForTesting
	@Override
	public void tryReturnExcessMemorySegments() throws IOException {
		throw new UnsupportedOperationException();
	}

	@VisibleForTesting
	public void tryReturnExcessMemorySegmentPackages() throws IOException {
		synchronized (factoryLock){
			returnExcessMemorySegmentPackages();
		}
	}

	private void returnExcessMemorySegmentPackages() throws IOException {
		assert Thread.holdsLock(factoryLock);

		if (resizableBufferPools.isEmpty()) {
			return;
		}

		// in this case, we need to redistribute buffers so that every pool gets its minimum
		for (LocalBufferPackagePool bufferPool : resizableBufferPools) {
			int excess = bufferPool.getNumBuffers() - bufferPool.getNumberOfRequiredMemorySegments();
			if (excess > 0) {
				bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
				numTotalRequiredPackages -= excess / numberOfMemorySegmentsPerPackage;
			}
		}
		resizableBufferPools.clear();
	}

	private String getConfigDescription() {
		return String.format("The total number of network buffers is currently set to %d of %d bytes each. " +
				"You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
			totalNumberOfMemorySegmentPackages * numberOfMemorySegmentsPerPackage,
			memorySegmentSize,
			TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
			TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
			TaskManagerOptions.NETWORK_MEMORY_MAX.key());
	}

	@Override
	public long getRequestNetworkSegmentTimeoutMills() {
		return requestNetworkSegmentTimeoutMills;
	}
}
