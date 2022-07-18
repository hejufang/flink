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

import org.junit.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link NetworkBufferPackagePool}.
 */
public class NetworkBufferPackagePoolTest {

	@Test
	public void testRequestAndRecyclePackage() throws Exception {
		int numberOfSegmentsToRequest = 3;
		int numBuffers = 10;
		int memorySegmentPerPackage = 5;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);

		final NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0,
			memorySegmentPerPackage);

		assertTrue(globalPool.getAvailableFuture().isDone());
		ArrayDeque<MemorySegment> package1 = globalPool.requestMemorySegmentPackage();
		assertEquals(memorySegmentPerPackage, package1.size());
		assertEquals(memorySegmentPerPackage, globalPool.getNumberOfAvailableMemorySegments());
		assertTrue(globalPool.getAvailableFuture().isDone());

		ArrayDeque<MemorySegment> package2 = globalPool.requestMemorySegmentPackage();
		assertEquals(memorySegmentPerPackage, package2.size());
		assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());
		assertFalse(globalPool.getAvailableFuture().isDone());

		globalPool.recyclePackage(package1);
		assertEquals(memorySegmentPerPackage, globalPool.getNumberOfAvailableMemorySegments());
		assertTrue(globalPool.getAvailableFuture().isDone());

		globalPool.recyclePackage(package2);
		assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
		assertTrue(globalPool.getAvailableFuture().isDone());

		ArrayDeque<ArrayDeque<MemorySegment>> packages = globalPool.requestMemorySegmentPackages(2);
		assertEquals(2, packages.size());
		assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());
		assertFalse(globalPool.getAvailableFuture().isDone());

		globalPool.recycleMemorySegmentPackages(packages);
		assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
		assertTrue(globalPool.getAvailableFuture().isDone());
		assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(0, globalPool.getNumTotalRequiredBuffers());
	}

	@Test(timeout = 10000L)
	public void testBlockingRequestPackageFromMultiLocalBufferPool() throws Exception {
		final int localPoolRequiredSize = 10;
		final int localPoolMaxSize = 20;
		final int numLocalBufferPool = 2;
		final int numberOfSegmentsToRequest = 20;
		final int memorySegmentPerPackage = 2;
		final int numBuffers = numLocalBufferPool * localPoolMaxSize;
		final Duration requestSegmentsTimeout = Duration.ofMillis(5000L);

		final ExecutorService executorService = Executors.newFixedThreadPool(numLocalBufferPool);
		final NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			false,
			0,
			memorySegmentPerPackage);
		final List<BufferPool> localBufferPools = new ArrayList<>(numLocalBufferPool);

		try {
			// create local buffer pools
			for (int i = 0; i < numLocalBufferPool; ++i) {
				final BufferPool localPool = globalPool.createBufferPool(localPoolRequiredSize, localPoolMaxSize);
				localBufferPools.add(localPool);
				assertTrue(localPool.getAvailableFuture().isDone());
			}

			// request some segments from the global pool in two different ways
			final List<ArrayDeque<MemorySegment>> segmentPackages = new ArrayList<>(numberOfSegmentsToRequest / memorySegmentPerPackage - 1);
			for (int i = 0; i < numberOfSegmentsToRequest / memorySegmentPerPackage - 1; ++i) {
				segmentPackages.add(globalPool.requestMemorySegmentPackage());
			}
			final ArrayDeque<ArrayDeque<MemorySegment>> exclusiveSegmentPackages = globalPool.requestMemorySegmentPackages(numberOfSegmentsToRequest / memorySegmentPerPackage);
			assertTrue(globalPool.getAvailableFuture().isDone());
			for (final BufferPool localPool: localBufferPools) {
				assertTrue(localPool.getAvailableFuture().isDone());
			}

			// blocking request buffers form local buffer pools
			final CountDownLatch latch = new CountDownLatch(numLocalBufferPool);
			final BlockingQueue<BufferBuilder> segmentsRequested = new ArrayBlockingQueue<>(numBuffers);
			final AtomicReference<Throwable> cause = new AtomicReference<>();
			for (final BufferPool localPool: localBufferPools) {
				executorService.submit(() -> {
					try {
						for (int num = localPoolMaxSize; num > 0; --num) {
							segmentsRequested.add(localPool.requestBufferBuilderBlocking());
						}
					} catch (Exception e) {
						cause.set(e);
					} finally {
						latch.countDown();
					}
				});
			}

			// wait until all available buffers are requested
			while (globalPool.getNumberOfAvailableMemorySegments() > 0) {
				Thread.sleep(100);
				assertNull(cause.get());
			}

			final CompletableFuture<?> globalPoolAvailableFuture = globalPool.getAvailableFuture();
			assertFalse(globalPoolAvailableFuture.isDone());

			final List<CompletableFuture<?>> localPoolAvailableFutures = new ArrayList<>(numLocalBufferPool);
			for (BufferPool localPool: localBufferPools) {
				CompletableFuture<?> localPoolAvailableFuture = localPool.getAvailableFuture();
				localPoolAvailableFutures.add(localPoolAvailableFuture);
			}

			// recycle the previously requested segments
			for (ArrayDeque<MemorySegment> segmentPackage: segmentPackages) {
				globalPool.recyclePackage(segmentPackage);
			}
			globalPool.recycleMemorySegmentPackages(exclusiveSegmentPackages);

			assertTrue(globalPoolAvailableFuture.isDone());
			for (CompletableFuture<?> localPoolAvailableFuture: localPoolAvailableFutures) {
				assertTrue(localPoolAvailableFuture.isDone());
			}

			// wait until all blocking buffer requests finish
			latch.await();

			assertNull(cause.get());
			assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());
			assertFalse(globalPool.getAvailableFuture().isDone());
			for (BufferPool localPool: localBufferPools) {
				assertEquals(localPoolMaxSize, localPool.bestEffortGetNumOfUsedBuffers());
			}

			// recycle all the requested buffers
			for (BufferBuilder bufferBuilder: segmentsRequested) {
				bufferBuilder.createBufferConsumer().close();
			}

		} finally {
			for (BufferPool bufferPool: localBufferPools) {
				bufferPool.lazyDestroy();
			}
			executorService.shutdown();
			assertEquals(0, globalPool.getNumTotalRequiredBuffers());
			globalPool.destroy();
		}
	}

	@Test
	public void testRequestExclusiveBufferPackage() throws Exception {
		final int localPoolRequiredSize = 10;
		final int localPoolMaxSize = 20;
		final int numberOfSegmentsToRequest = 5;
		final int memorySegmentPerPackage = 3;
		final int numBuffers = 50;
		final Duration requestSegmentsTimeout = Duration.ofMillis(5000L);

		final NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			memorySegmentPerPackage);

		LocalBufferPackagePool localBufferPackagePool = (LocalBufferPackagePool) globalPool.createBufferPool(localPoolRequiredSize, localPoolMaxSize);
		localBufferPackagePool.setNumRemoteChannels(2);
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(0, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(2, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(0, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(48, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(21, globalPool.getNumTotalRequiredBuffers());

		List<MemorySegment> exclusiveBuffers1 = localBufferPackagePool.requestExclusiveSegments();
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(0, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(1, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(1, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(0, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(42, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(27, globalPool.getNumTotalRequiredBuffers());

		List<MemorySegment> exclusiveBuffers2 = localBufferPackagePool.requestExclusiveSegments();
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(0, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(2, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(36, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(33, globalPool.getNumTotalRequiredBuffers());

		final BlockingQueue<BufferBuilder> segments = new ArrayBlockingQueue<>(13);
		for (int i = 0; i < 13; ++i) {
			segments.add(localBufferPackagePool.requestBufferBuilderBlocking());
		}
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(2, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(13, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(2, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(21, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(33, globalPool.getNumTotalRequiredBuffers());

		localBufferPackagePool.recycleExclusiveSegments(exclusiveBuffers1);
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(2, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(13, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(1, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(27, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(27, globalPool.getNumTotalRequiredBuffers());

		for (BufferBuilder buffer : segments) {
			buffer.createBufferConsumer().close();
		}
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(15, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(1, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(27, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(27, globalPool.getNumTotalRequiredBuffers());

		localBufferPackagePool.lazyDestroy();
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(0, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(1, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(42, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(6, globalPool.getNumTotalRequiredBuffers());

		localBufferPackagePool.recycleExclusiveSegments(exclusiveBuffers2);
		assertEquals(21, localBufferPackagePool.getNumBuffers());
		assertEquals(21, localBufferPackagePool.getMaxNumberOfMemorySegments());
		assertEquals(12, localBufferPackagePool.getNumberOfRequiredMemorySegments());
		assertEquals(0, localBufferPackagePool.getNumberOfAvailableMemorySegments());
		assertEquals(0, localBufferPackagePool.bestEffortGetNumOfUsedBuffers());
		assertEquals(0, localBufferPackagePool.getNumLeftRemoteChannels());
		assertEquals(0, localBufferPackagePool.getNumAvailableExclusiveSegments());
		assertEquals(0, localBufferPackagePool.getNumExclusiveSegmentsToRecycle());
		assertEquals(48, globalPool.getNumberOfAvailableMemorySegments());
		assertEquals(0, globalPool.getNumTotalRequiredBuffers());

		assertEquals(2, localBufferPackagePool.getNumTotalRemoteChannels());
	}

	@Test(timeout = 10000L)
	public void testSimpleRedistributeResizePool() throws Exception {
		final int numBuffers = 1000;
		final int numberOfSegmentsToRequest = 1;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);
		NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			20);

		final ExecutorService executorService = Executors.newFixedThreadPool(2);
		BufferPool bufferPool = globalPool.createBufferPool(1, numBuffers);
		final CountDownLatch latch = new CountDownLatch(2);

		executorService.submit(() -> {
			try {
				for (int i = 0; i < numBuffers; i++) {
					bufferPool.requestBuffer();
				}
				latch.countDown();
			} catch (Exception e) {
			}
		});
		executorService.submit(() -> {
			try {
				for (int i = 0; i < 100; i++) {
					Thread.sleep(1);
					globalPool.tryReturnExcessMemorySegmentPackages();
				}
				latch.countDown();
			} catch (Exception e) {
			}
		});
		try {
			assertTrue(
				"request buffer timeout",
				latch.await(5000L, TimeUnit.MILLISECONDS));
			globalPool.destroyBufferPool(bufferPool);
			assertEquals(0, globalPool.getNumTotalRequiredBuffers());
		} finally {
			executorService.shutdown();
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testSimpleRedistributeResizePoolMultiLocalPool() throws Exception {
		final int numBuffers = 10000;
		final int numberOfSegmentsToRequest = 1;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);
		NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers * 2,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			20);

		final ExecutorService executorService = Executors.newFixedThreadPool(11);
		final CountDownLatch latch = new CountDownLatch(11);

		for (int i = 0; i < 10; ++i) {
			executorService.submit(() -> {
				try {
					LocalBufferPackagePool bufferPool = (LocalBufferPackagePool) globalPool.createBufferPool(1, numBuffers / 10);
					bufferPool.setNumRemoteChannels(1);
					List<MemorySegment> exclusiveSegments = bufferPool.requestExclusiveSegments();
					for (int j = 0; j < numBuffers / 10; j++) {
						bufferPool.requestBuffer();
					}
					bufferPool.recycleExclusiveSegments(exclusiveSegments);
					bufferPool.lazyDestroy();
					latch.countDown();
				} catch (Exception e) {
					fail(e.getMessage());
				}
			});
		}
		executorService.submit(() -> {
			try {
				for (int i = 0; i < 100; i++) {
					Thread.sleep(1);
					globalPool.tryReturnExcessMemorySegmentPackages();
				}
				latch.countDown();
			} catch (Exception e) {
				fail(e.getMessage());
			}
		});
		try {
			assertTrue(
				"request buffer timeout",
				latch.await(5000L, TimeUnit.MILLISECONDS));
			assertEquals(0, globalPool.getNumTotalRequiredBuffers());
		} finally {
			executorService.shutdown();
			globalPool.destroy();
		}
	}

	@Test(timeout = 10000L)
	public void testRequestExclusiveSegmentsWithoutEnoughRemoteChannels() throws Exception {
		final int numBuffers = 10000;
		final int numberOfSegmentsToRequest = 1;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);
		NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers * 2,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			20);

		LocalBufferPackagePool bufferPool = (LocalBufferPackagePool) globalPool.createBufferPool(1, numBuffers / 10);
		bufferPool.setNumRemoteChannels(20);
		for (int i = 0; i < 20; ++i) {
			List<MemorySegment> segments = bufferPool.requestExclusiveSegments();
			assertEquals(numberOfSegmentsToRequest, segments.size());
		}
		assertThrows(
			"Buffer buffer receive exclusive segments segments, but the number" +
				"of remote channels larger than expected, make sure this is used in OLAP mode.",
			IllegalStateException.class,
			() -> bufferPool.requestExclusiveSegments());
		assertEquals(0, bufferPool.getNumLeftRemoteChannels());
		assertEquals(20, bufferPool.getNumTotalRemoteChannels());
	}

	@Test(timeout = 10000L)
	public void testSimpleRedistribute() throws Exception {
		final int numBuffers = 1000;
		final int numberOfSegmentsToRequest = 10;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);
		NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			3);

		BufferPool bufferPool = globalPool.createBufferPool(10, numBuffers);

		assertEquals(12, bufferPool.getNumBuffers());
		assertEquals(12, bufferPool.getNumberOfRequiredMemorySegments());

		List<Buffer> buffers = new ArrayList<>();
		for (int i = 0; i < 12; ++i) {
			Buffer buffer = bufferPool.requestBuffer();
			assertNotNull(buffer);
			buffers.add(buffer);
			assertEquals(12, bufferPool.getNumBuffers());
		}
		for (int i = 12; i < 798; ++i) {
			Buffer buffer = bufferPool.requestBuffer();
			if (i % 3 == 0) {
				assertNull(buffer);
				buffer = bufferPool.requestBuffer();
			}
			assertNotNull(buffer);
			buffers.add(buffer);
			assertEquals((buffers.size() + 3 - 1) / 3 * 3, bufferPool.getNumBuffers());
			assertEquals(bufferPool.getNumBuffers(), globalPool.getNumTotalRequiredBuffers());
		}
		assertEquals(798, bufferPool.getNumBuffers());

		for (int i = 0; i < 100; ++i) {
			assertNull(bufferPool.requestBuffer());
		}
		assertEquals(798, bufferPool.getNumBuffers());

		assertEquals(201, globalPool.getNumberOfAvailableMemorySegments());
		bufferPool.lazyDestroy();
		assertEquals(201, globalPool.getNumberOfAvailableMemorySegments());
		for (Buffer buffer : buffers) {
			assertNotNull(buffer);
			buffer.recycleBuffer();
		}
		assertEquals(999, globalPool.getNumberOfAvailableMemorySegments());
	}

	@Test(timeout = 10000L)
	public void testRequestAndRecycleExclusiveBuffers() throws Exception {
		final int numBuffers = 100;
		final int numberOfSegmentsToRequest = 10;
		final int minSize = 10;
		final Duration requestSegmentsTimeout = Duration.ofMillis(50L);
		NetworkBufferPackagePool globalPool = new NetworkBufferPackagePool(
			numBuffers,
			128,
			numberOfSegmentsToRequest,
			requestSegmentsTimeout,
			false,
			Duration.ofMillis(0L),
			true,
			0.8,
			5);

		for (int i = 0; i < 100; ++i) {
			LocalBufferPackagePool localPool = (LocalBufferPackagePool) globalPool.createBufferPool(minSize, numBuffers);
			localPool.setNumRemoteChannels((numBuffers - minSize) / numberOfSegmentsToRequest);
			List<MemorySegment> segments = new ArrayList<>(numBuffers);
			for (int j = 0; j < numBuffers / numberOfSegmentsToRequest - 1; ++j) {
				segments.addAll(localPool.requestExclusiveSegments());
			}
			assertEquals(numBuffers - minSize, segments.size());
			assertEquals(0, localPool.getNumLeftRemoteChannels());
			assertEquals((numBuffers - minSize) / numberOfSegmentsToRequest, localPool.getNumTotalRemoteChannels());

			localPool.recycleExclusiveSegments(segments);
			assertEquals(minSize, globalPool.getNumTotalRequiredBuffers());
			assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());

			localPool.lazyDestroy();
			assertEquals(0, globalPool.getNumTotalRequiredBuffers());
			assertEquals(numBuffers, globalPool.getNumberOfAvailableMemorySegments());
		}
	}
}
