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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Request/recycle memory segment packages with {@link NetworkBufferPackagePool}.
 * Only work in OLAP.
 */
public class LocalBufferPackagePool implements BufferPool {
	private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPackagePool.class);

	private static final int UNKNOWN_CHANNEL = -1;

	/** Global network buffer package pool to get buffers from. */
	private final NetworkBufferPackagePool networkBufferPackagePool;

	/** The minimum number of required segments for this pool. */
	private final int numberOfRequiredMemorySegments;

	/**
	 * The currently available memory segments. These are segments, which have been requested from
	 * the network buffer pool and are currently not handed out as Buffer instances.
	 *
	 * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
	 * locks acquired before entering this class vs. locks being acquired during calls to external
	 * code inside this class, e.g. with
	 * {@link org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue}
	 * via the {@link #registeredListeners} callback.
	 */
	private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<>();

	/**
	 * Buffer availability listeners, which need to be notified when a Buffer becomes available.
	 * Listeners can only be registered at a time/state where no Buffer instance was available.
	 */
	private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

	/** Maximum number of network buffers to allocate. */
	private final int maxNumberOfMemorySegments;

	/** The current size of this pool. */
	private int currentPoolSize;

	/**
	 * Number of all memory segments, which have been requested from the network buffer pool and are
	 * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	 */
	private int numberOfRequestedMemorySegments;

	private final int maxBuffersPerChannel;

	private final int[] subpartitionBuffersCount;

	private final BufferRecycler[] subpartitionBufferRecyclers;

	private int unavailableSubpartitionsCount = 0;

	/**
	 * If this is set, that means others cannot request from {@link LocalBufferPackagePool}, but can still recycle.
	 */
	private boolean isDestroyed;

	@Nullable
	private final BufferPoolOwner bufferPoolOwner;

	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();


	/** These are for {@link org.apache.flink.configuration.NettyShuffleEnvironmentOptions#NETWORK_BUFFER_POOL_SEGMENT_PACKAGE_ENABLE},
	 * at this time, {@link LocalBufferPackagePool} will request/recycle packages consisting of memory segments whit {@link NetworkBufferPackagePool}.
	 */
	private final int numberOfSegmentsToRequest;

	private final int numberOfMemorySegmentsPerPackage;

	/**
	 * When we recycle some memory segments, maybe it is not enough to form a package, so just store it.
	 */
	private final LinkedList<MemorySegment> segmentsToRecycle;

	// ------------------------------------------------------------------------
	//  Only make sense when this buffer pool is for InputGate in OLAP.
	// ------------------------------------------------------------------------

	/**
	 * The segments requested from {@link NetworkBufferPackagePool} that will be used as exclusive buffers.
	 */
	private ArrayDeque<MemorySegment> availableExclusiveSegments;

	/**
	 * The recycled exclusive buffers, may not be enough to form a package, so just sore it.
	 */
	private LinkedList<MemorySegment> exclusiveSegmentsToRecycle;

	/**
	 * Track the total number of {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel}.
	 * Note this value is only correct when in OLAP, others will cause error.
	 */


	private int numTotalRemoteChannels = 0;

	/**
	 * Track the number of {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel} that has not
	 * assigned exclusive buffers yet.
	 * Note this value is only correct when in OLAP, others will cause error.
	 */

	private int numLeftRemoteChannels = 0;

	/**
	 * Local buffer pool based on the given <tt>networkBufferPackagePool</tt> and <tt>bufferPoolOwner</tt>
	 * with a minimal and maximal number of network buffers being available.
	 *
	 * @param networkBufferPackagePool
	 * 		global network buffer pool to get buffers from
	 * @param currentPoolSize
	 * 		current pool size
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 * @param bufferPoolOwner
	 * 		the owner of this buffer pool to release memory when needed
	 * @param numberOfSubpartitions
	 * 		number of subpartitions
	 * @param maxBuffersPerChannel
	 * 		maximum number of buffers to use for each channel
	 * @param numberOfSegmentsToRequest
	 * 		the number of memory segments to form exclusive buffers
	 * @param numberOfMemorySegmentsPerPackage
	 * 		the number of memory segment to form a package
	 */
	LocalBufferPackagePool(
			NetworkBufferPackagePool networkBufferPackagePool,
			int currentPoolSize,
			int numberOfRequiredMemorySegments,
			int maxNumberOfMemorySegments,
			@Nullable BufferPoolOwner bufferPoolOwner,
			int numberOfSubpartitions,
			int maxBuffersPerChannel,
			int numberOfSegmentsToRequest,
			int numberOfMemorySegmentsPerPackage) {
		checkArgument(maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
			"Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
			maxNumberOfMemorySegments, numberOfRequiredMemorySegments);

		checkArgument(maxNumberOfMemorySegments > 0,
			"Maximum number of memory segments (%s) should be larger than 0.",
			maxNumberOfMemorySegments);

		LOG.debug("Using a local buffer pool with {}-{} buffers",
			numberOfRequiredMemorySegments, maxNumberOfMemorySegments);

		this.networkBufferPackagePool = networkBufferPackagePool;
		this.numberOfRequiredMemorySegments = (numberOfRequiredMemorySegments + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage * numberOfMemorySegmentsPerPackage;
		this.maxNumberOfMemorySegments = (maxNumberOfMemorySegments + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage * numberOfMemorySegmentsPerPackage;
		this.currentPoolSize = (currentPoolSize + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage * numberOfMemorySegmentsPerPackage;
		this.segmentsToRecycle = new LinkedList<>();

		this.bufferPoolOwner = bufferPoolOwner;

		if (numberOfSubpartitions > 0) {
			checkArgument(maxBuffersPerChannel > 0,
				"Maximum number of buffers for each channel (%s) should be larger than 0.",
				maxBuffersPerChannel);
		}

		this.subpartitionBuffersCount = new int[numberOfSubpartitions];
		subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
		for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
			subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
		}
		this.maxBuffersPerChannel = maxBuffersPerChannel;

		this.numberOfSegmentsToRequest = numberOfSegmentsToRequest;
		this.numberOfMemorySegmentsPerPackage = numberOfMemorySegmentsPerPackage;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getNumberOfRequiredMemorySegments() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int getMaxNumberOfMemorySegments() {
		return maxNumberOfMemorySegments;
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public int getNumBuffers() {
		synchronized (availableMemorySegments) {
			return currentPoolSize;
		}
	}

	@Override
	public int bestEffortGetNumOfUsedBuffers() {
		return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		return toBuffer(requestMemorySegment());
	}

	@Override
	public BufferBuilder requestBufferBuilder() throws IOException {
		return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
	}

	@Override
	public BufferBuilder requestBufferBuilder(int targetChannel) throws IOException {
		return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
		return toBufferBuilder(requestMemorySegmentBlocking(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking(int targetChannel) throws IOException, InterruptedException {
		return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
	}

	private Buffer toBuffer(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new NetworkBuffer(memorySegment, this);
	}

	private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
		if (memorySegment == null) {
			return null;
		}

		if (targetChannel == UNKNOWN_CHANNEL) {
			return new BufferBuilder(memorySegment, this);
		} else {
			return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
		}
	}

	private MemorySegment requestMemorySegmentBlocking(int targetChannel) throws InterruptedException, IOException {
		MemorySegment segment;
		long networkSegmentTimeoutMills = networkBufferPackagePool.getRequestNetworkSegmentTimeoutMills();
		while ((segment = requestMemorySegment(targetChannel)) == null) {
			try {
				if (networkSegmentTimeoutMills > 0) {
					getAvailableFuture().get(networkSegmentTimeoutMills, TimeUnit.MILLISECONDS);
				} else {
					// wait until available
					getAvailableFuture().get();
				}
			} catch (ExecutionException | TimeoutException e) {
				LOG.error("The available future is completed exceptionally.", e);
				ExceptionUtils.rethrow(e);
			}
		}
		return segment;
	}

	@Nullable
	private MemorySegment requestMemorySegment(int targetChannel) throws IOException {
		MemorySegment segment = null;
		boolean needResizePool = false;
		synchronized (availableMemorySegments) {

			if (isDestroyed) {
				throw new IllegalStateException("Buffer pool is destroyed.");
			}

			returnExcessMemorySegments();

			if (availableMemorySegments.isEmpty()) {
				segment = requestMemorySegmentFromGlobal();
			}
			// segment may have been released by buffer pool owner
			if (segment == null) {
				segment = availableMemorySegments.poll();
			}
			if (segment == null) {
				availabilityHelper.resetUnavailable();
			}

			if (segment != null && targetChannel != UNKNOWN_CHANNEL) {
				if (subpartitionBuffersCount[targetChannel]++ == maxBuffersPerChannel) {
					unavailableSubpartitionsCount++;
					availabilityHelper.resetUnavailable();
				}
			}
			if (segment == null && numberOfRequestedMemorySegments + numberOfMemorySegmentsPerPackage > currentPoolSize && numberOfRequestedMemorySegments + numberOfMemorySegmentsPerPackage < maxNumberOfMemorySegments) {
				needResizePool = true;
			}
		}

		if (needResizePool) {
			networkBufferPackagePool.tryResizeLocalBufferPool(this);
		}

		return segment;
	}

	@Nullable
	private MemorySegment requestMemorySegment() throws IOException {
		return requestMemorySegment(UNKNOWN_CHANNEL);
	}

	@Nullable
	private MemorySegment requestMemorySegmentFromGlobal() throws IOException {
		assert Thread.holdsLock(availableMemorySegments);

		if (isDestroyed) {
			throw new IllegalStateException("Buffer pool is destroyed.");
		}

		if (numberOfRequestedMemorySegments < currentPoolSize) {
			final ArrayDeque<MemorySegment> segmentPackage = networkBufferPackagePool.requestMemorySegmentPackage();
			if (segmentPackage != null) {
				// This may be always true? And since package size > 1, after polling a segment it is still not empty.
				if (availableMemorySegments.isEmpty() && unavailableSubpartitionsCount == 0) {
					availabilityHelper.getUnavailableToResetAvailable();
				}
				numberOfRequestedMemorySegments += segmentPackage.size();
				availableMemorySegments.addAll(segmentPackage);
			}
			// If we return null, then it will still try to poll from availableMemorySegments, so it's ok.
			return null;
		}

		if (bufferPoolOwner != null) {
			bufferPoolOwner.releaseMemory(1);
		}

		return null;
	}

	@Override
	public void recycle(MemorySegment segment) {
		recycle(segment, UNKNOWN_CHANNEL);
	}

	private void recycle(MemorySegment segment, int channel) {
		BufferListener listener;
		CompletableFuture<?> toNotify = null;
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;
		while (!notificationResult.isBufferUsed()) {
			synchronized (availableMemorySegments) {
				final int oldUnavailableSubpartitionsCount = unavailableSubpartitionsCount;
				if (channel != UNKNOWN_CHANNEL) {
					if (--subpartitionBuffersCount[channel] == maxBuffersPerChannel) {
						unavailableSubpartitionsCount--;
					}
				}

				if (isDestroyed || numberOfRequestedMemorySegments > currentPoolSize) {
					returnMemorySegment(segment);
					return;
				} else {
					listener = registeredListeners.poll();
					if (listener == null) {
						boolean wasUnavailable = availableMemorySegments.isEmpty() || oldUnavailableSubpartitionsCount > 0;
						availableMemorySegments.add(segment);
						// only need to check unavailableSubpartitionsCount here because availableMemorySegments is not empty
						if (wasUnavailable && unavailableSubpartitionsCount == 0) {
							toNotify = availabilityHelper.getUnavailableToResetAvailable();
						}
						break;
					}
				}
			}
			notificationResult = fireBufferAvailableNotification(listener, segment);
		}

		mayNotifyAvailable(toNotify);
	}

	private NotificationResult fireBufferAvailableNotification(BufferListener listener, MemorySegment segment) {
		// We do not know which locks have been acquired before the recycle() or are needed in the
		// notification and which other threads also access them.
		// -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock (FLINK-9676)
		NotificationResult notificationResult = listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
		if (notificationResult.needsMoreBuffers()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed) {
					// cleanup tasks how they would have been done if we only had one synchronized block
					listener.notifyBufferDestroyed();
				} else {
					registeredListeners.add(listener);
				}
			}
		}
		return notificationResult;
	}

	/**
	 * Destroy is called after the produce or consume phase of a task finishes.
	 */
	@Override
	public void lazyDestroy() {
		// NOTE: if you change this logic, be sure to update recycle() as well!
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			if (!isDestroyed) {
				MemorySegment segment;
				while ((segment = availableMemorySegments.poll()) != null) {
					returnMemorySegment(segment);
				}

				BufferListener listener;
				while ((listener = registeredListeners.poll()) != null) {
					listener.notifyBufferDestroyed();
				}

				if (!isAvailable()) {
					toNotify = availabilityHelper.getAvailableFuture();
				}

				isDestroyed = true;
			}
		}

		mayNotifyAvailable(toNotify);

		try {
			networkBufferPackagePool.destroyBufferPool(this);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		synchronized (availableMemorySegments) {
			if (!availableMemorySegments.isEmpty() || isDestroyed) {
				return false;
			}

			registeredListeners.add(listener);
			return true;
		}
	}

	@Override
	public void setNumBuffers(int numBuffers) throws IOException {
		int numExcessBuffers;
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numberOfRequiredMemorySegments,
				"Buffer pool needs at least %s buffers, but tried to set to %s",
				numberOfRequiredMemorySegments, numBuffers);

			if (numBuffers > maxNumberOfMemorySegments) {
				currentPoolSize = maxNumberOfMemorySegments;
			} else {
				currentPoolSize = numBuffers;
			}

			returnExcessMemorySegments();
			// PoolSize may be increased, and there may be some segments that need to be recycled, but not
			// enough to form a package, we can now take it back.
			takeExcessMemorySegments();

			numExcessBuffers = numberOfRequestedMemorySegments - currentPoolSize;
			if (numExcessBuffers < 0 && availableMemorySegments.isEmpty() && networkBufferPackagePool.isAvailable()) {
				toNotify = availabilityHelper.getUnavailableToResetUnavailable();
			}
		}

		mayNotifyAvailable(toNotify);

		// If there is a registered owner and we have still requested more buffers than our
		// size, trigger a recycle via the owner.
		if (bufferPoolOwner != null && numExcessBuffers > 0) {
			bufferPoolOwner.releaseMemory(numExcessBuffers);
		}
	}

	public boolean tryIncNumPackages() throws IOException {
		synchronized (availableMemorySegments) {
			if (currentPoolSize >= maxNumberOfMemorySegments || currentPoolSize < numberOfRequestedMemorySegments) {
				return false;
			}
			setNumBuffers(currentPoolSize + numberOfMemorySegmentsPerPackage);
			return true;
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (numberOfRequestedMemorySegments >= currentPoolSize || unavailableSubpartitionsCount > 0) {
			return availabilityHelper.getAvailableFuture();
		} else if (availabilityHelper.isApproximatelyAvailable() || networkBufferPackagePool.isApproximatelyAvailable()) {
			return AVAILABLE;
		} else {
			return CompletableFuture.anyOf(availabilityHelper.getAvailableFuture(), networkBufferPackagePool.getAvailableFuture());
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format(
				"[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d," +
					"subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
				currentPoolSize, numberOfRequiredMemorySegments, numberOfRequestedMemorySegments,
				availableMemorySegments.size(), maxNumberOfMemorySegments, registeredListeners.size(),
				subpartitionBuffersCount.length, maxBuffersPerChannel, isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Notifies the potential segment consumer of the new available segments by
	 * completing the previous uncompleted future.
	 */
	private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private void returnMemorySegment(MemorySegment segment) {
		assert Thread.holdsLock(availableMemorySegments);

		--numberOfRequestedMemorySegments;
		segmentsToRecycle.add(segment);
		while (segmentsToRecycle.size() >= numberOfMemorySegmentsPerPackage) {
			ArrayDeque<MemorySegment> segmentPackage = new ArrayDeque<>(numberOfMemorySegmentsPerPackage);
			for (int i = 0; i < numberOfMemorySegmentsPerPackage; ++i) {
				segmentPackage.add(segmentsToRecycle.poll());
			}
			networkBufferPackagePool.recyclePackage(segmentPackage);
		}
	}

	private void returnExcessMemorySegments() {
		assert Thread.holdsLock(availableMemorySegments);

		while (numberOfRequestedMemorySegments > currentPoolSize) {
			MemorySegment segment = availableMemorySegments.poll();
			if (segment == null) {
				return;
			}

			returnMemorySegment(segment);
		}
	}

	private void takeExcessMemorySegments() {
		assert Thread.holdsLock(availableMemorySegments);

		while (numberOfRequestedMemorySegments < currentPoolSize) {
			MemorySegment segment = segmentsToRecycle.poll();
			if (segment == null) {
				return;
			}

			++numberOfRequestedMemorySegments;
			if (availableMemorySegments.isEmpty() && unavailableSubpartitionsCount == 0) {
				availabilityHelper.getUnavailableToResetAvailable();
			}
			availableMemorySegments.add(segment);
		}
	}

	@VisibleForTesting
	@Override
	public BufferRecycler[] getSubpartitionBufferRecyclers() {
		return subpartitionBufferRecyclers;
	}

	private static class SubpartitionBufferRecycler implements BufferRecycler {

		private int channel;
		private LocalBufferPackagePool bufferPool;

		SubpartitionBufferRecycler(int channel, LocalBufferPackagePool bufferPool) {
			this.channel = channel;
			this.bufferPool = bufferPool;
		}

		@Override
		public void recycle(MemorySegment memorySegment) {
			bufferPool.recycle(memorySegment, channel);
		}
	}

	/**
	 * That means this buffer pool is for {@link org.apache.flink.runtime.io.network.partition.consumer.InputGate},
	 * now the exclusive segments will make sense.
	 */
	public void setNumRemoteChannels(int numRemoteChannels) {
		this.availableExclusiveSegments = new ArrayDeque<>(numberOfMemorySegmentsPerPackage);
		this.exclusiveSegmentsToRecycle = new LinkedList<>();
		this.numLeftRemoteChannels = numRemoteChannels;
		this.numTotalRemoteChannels = numRemoteChannels;
	}

	/**
	 * This should only be used by input channel to assign exclusive buffers,
	 * when {@link org.apache.flink.configuration.NettyShuffleEnvironmentOptions#NETWORK_BUFFER_POOL_SEGMENT_PACKAGE_ENABLE}
	 * is set.
	 */
	public List<MemorySegment> requestExclusiveSegments() throws IOException {
		if (numLeftRemoteChannels == 0) {
			throw new IllegalStateException("Buffer buffer receive exclusive segments segments, but the number" +
				"of remote channels larger than expected, make sure this is used in OLAP mode.");
		}

		List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);

		synchronized (availableExclusiveSegments) {
			if (numLeftRemoteChannels == 0) {
				throw new IllegalStateException("Buffer buffer receive exclusive segments segments, but the number" +
					"of remote channels larger than expected, make sure this is used in OLAP mode.");
			}
			--numLeftRemoteChannels;
			int numOfExtraPackageToRequest = (Math.max(0, numberOfSegmentsToRequest - availableExclusiveSegments.size()) + numberOfMemorySegmentsPerPackage - 1) / numberOfMemorySegmentsPerPackage;

			int segmentsToRequest = numberOfSegmentsToRequest;
			if (numOfExtraPackageToRequest > 0) {
				ArrayDeque<ArrayDeque<MemorySegment>> packages = networkBufferPackagePool.requestMemorySegmentPackages(numOfExtraPackageToRequest);
				ArrayDeque<MemorySegment> segmentPackage;
				MemorySegment segment;
				while ((segmentPackage = packages.poll()) != null) {
					while ((segment = segmentPackage.poll()) != null) {
						if (segmentsToRequest > 0) {
							segments.add(segment);
							--segmentsToRequest;
						} else {
							availableExclusiveSegments.add(segment);
						}
					}
				}
			}

			// We now should have enough exclusive segments.
			while (segmentsToRequest > 0) {
				segments.add(availableExclusiveSegments.poll());
				--segmentsToRequest;
			}

			// There is no more remote channels to use the left segments.
			if (numLeftRemoteChannels == 0) {
				MemorySegment segment;
				while ((segment = availableExclusiveSegments.poll()) != null) {
					exclusiveSegmentsToRecycle.add(segment);
				}
			}

		}
		return segments;
	}

	public void recycleExclusiveSegments(List<MemorySegment> segments) throws IOException {
		synchronized (availableExclusiveSegments) {
			exclusiveSegmentsToRecycle.addAll(segments);
			if (exclusiveSegmentsToRecycle.size() >= numberOfMemorySegmentsPerPackage) {
				List<ArrayDeque<MemorySegment>> packages = new ArrayList<>(exclusiveSegmentsToRecycle.size() / numberOfMemorySegmentsPerPackage);
				while (exclusiveSegmentsToRecycle.size() >= numberOfMemorySegmentsPerPackage) {
					ArrayDeque<MemorySegment> segmentPackage = new ArrayDeque<>(numberOfMemorySegmentsPerPackage);
					for (int i = 0; i < numberOfMemorySegmentsPerPackage; ++i) {
						segmentPackage.add(exclusiveSegmentsToRecycle.poll());
					}
					packages.add(segmentPackage);
				}
				networkBufferPackagePool.recycleMemorySegmentPackages(packages);
			}
		}
	}

	public int getNumTotalRemoteChannels() {
		return numTotalRemoteChannels;
	}

	public int getNumLeftRemoteChannels() {
		return numLeftRemoteChannels;
	}

	public int getNumAvailableExclusiveSegments() {
		synchronized (availableExclusiveSegments) {
			return availableExclusiveSegments.size();
		}
	}

	public int getNumExclusiveSegmentsToRecycle() {
		synchronized (availableExclusiveSegments) {
			return exclusiveSegmentsToRecycle.size();
		}
	}
}