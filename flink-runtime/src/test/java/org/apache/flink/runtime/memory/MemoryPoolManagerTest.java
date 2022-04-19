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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the cache memory manager.
 */
public class MemoryPoolManagerTest {

	private static final long RANDOM_SEED = 643196033469871L;

	private static final int MEMORY_SIZE = 1024 * 1024 * 72; // 72 MiBytes

	private static final int PAGE_SIZE = 1024 * 32; // 32 KiBytes

	private static final int NUM_PAGES = MEMORY_SIZE / PAGE_SIZE;

	private MemoryManager memoryManager;

	private Random random;

	@Before
	public void setUp() {
		this.memoryManager = new MemoryPoolManager(MEMORY_SIZE, PAGE_SIZE, Duration.ofSeconds(10), true, 1);
		this.random = new Random(RANDOM_SEED);
	}

	@After
	public void tearDown() {
		if (!this.memoryManager.verifyEmpty()) {
			fail("Memory manager is not complete empty and valid at the end of the test.");
		}
		this.memoryManager = null;
		this.random = null;
	}

	@Test
	public void allocateAllSingle() {
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();
			List<MemorySegment> segments = new ArrayList<MemorySegment>();

			try {
				for (int i = 0; i < NUM_PAGES; i++) {
					segments.add(this.memoryManager.allocatePages(mockInvoke, 1).get(0));
				}
			}
			catch (MemoryAllocationException e) {
				fail("Unable to allocate memory");
			}

			this.memoryManager.release(segments);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateAllMulti() {
		try {
			final AbstractInvokable mockInvoke = new DummyInvokable();
			final List<MemorySegment> segments = new ArrayList<MemorySegment>();

			try {
				for (int i = 0; i < NUM_PAGES / 2; i++) {
					segments.addAll(this.memoryManager.allocatePages(mockInvoke, 2));
				}
			} catch (MemoryAllocationException e) {
				Assert.fail("Unable to allocate memory");
			}

			this.memoryManager.release(segments);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateMultipleOwners() {
		final int numOwners = 17;

		try {
			AbstractInvokable[] owners = new AbstractInvokable[numOwners];

			@SuppressWarnings("unchecked")
			List<MemorySegment>[] mems = (List<MemorySegment>[]) new List<?>[numOwners];

			for (int i = 0; i < numOwners; i++) {
				owners[i] = new DummyInvokable();
				mems[i] = new ArrayList<>(64);
			}

			// allocate all memory to the different owners
			for (int i = 0; i < NUM_PAGES; i++) {
				final int owner = this.random.nextInt(numOwners);
				mems[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1));
				for (MemorySegment segment : mems[owner]) {
					assertEquals(owners[owner], segment.getOwner());
				}
			}

			// free one owner at a time
			for (int i = 0; i < numOwners; i++) {
				this.memoryManager.releaseAll(owners[i]);
				owners[i] = null;
				mems[i].clear();
			}

			for (int i = 0; i < numOwners; i++) {
				owners[i] = new DummyInvokable();
				mems[i] = new ArrayList<>(64);
			}
			// allocate all memory to the different owners again
			for (int i = 0; i < NUM_PAGES; i++) {
				final int owner = this.random.nextInt(numOwners);
				mems[owner].addAll(this.memoryManager.allocatePages(owners[owner], 1));
				for (MemorySegment segment : mems[owner]) {
					assertEquals(owners[owner], segment.getOwner());
				}
			}

			// free one owner at a time
			for (int i = 0; i < numOwners; i++) {
				this.memoryManager.releaseAll(owners[i]);
				owners[i] = null;
				mems[i].clear();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Free the same segment list multiple times.
	 */
	@Test
	public void testMultipleFreeSameSegmentList() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		List<MemorySegment> segments1 = memoryManager.allocatePages(mockInvoke, 10);
		List<MemorySegment> segments2 = new ArrayList<>(segments1);

		memoryManager.release(mockInvoke, segments1);
		long availableSize1 = memoryManager.availableMemory();

		memoryManager.release(mockInvoke, segments2);
		long availableSize2 = memoryManager.availableMemory();
		assertEquals(availableSize1, availableSize2);
	}

	/**
	 * Free the same segment multiple times.
	 */
	@Test
	public void testMultipleFreeSameSegment() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		MemorySegment segment = memoryManager.allocatePages(mockInvoke, 1).iterator().next();

		memoryManager.release(mockInvoke, segment);
		long availableSize1 = memoryManager.availableMemory();

		memoryManager.release(mockInvoke, segment);
		long availableSize2 = memoryManager.availableMemory();
		assertEquals(availableSize1, availableSize2);
	}

	/**
	 * Free the same owner multiple times.
	 */
	@Test
	public void testMultipleFreeSameOwner() throws Exception {
		final AbstractInvokable mockInvoke = new DummyInvokable();
		memoryManager.allocatePages(mockInvoke, 1).iterator().next();

		memoryManager.releaseAll(mockInvoke);
		long availableSize1 = memoryManager.availableMemory();

		memoryManager.releaseAll(mockInvoke);
		long availableSize2 = memoryManager.availableMemory();
		assertEquals(availableSize1, availableSize2);
	}

	/**
	 * The owner modify free segment.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testMultipleOwnerModifySegment() throws Exception {
		final AbstractInvokable mockInvoke1 = new DummyInvokable();
		final AbstractInvokable mockInvoke2 = new DummyInvokable();

		List<MemorySegment> segmentList1 = memoryManager.allocatePages(mockInvoke1, NUM_PAGES);
		MemorySegment segment = segmentList1.iterator().next();
		memoryManager.release(segmentList1);

		List<MemorySegment> segmentList2 = memoryManager.allocatePages(mockInvoke2, NUM_PAGES);
		assertThrows("try to modify segment", RuntimeException.class, () -> {
			segment.putLong(0, 1L);
			return null;
		});
		memoryManager.release(segmentList2);
	}

	@Test
	public void testAllocatePagesTimeout() throws Exception {
		MemoryPoolManager pool = new MemoryPoolManager(MEMORY_SIZE, PAGE_SIZE, Duration.ofMillis(100), true, 1);

		final AbstractInvokable mockInvoke1 = new DummyInvokable();
		final AbstractInvokable mockInvoke2 = new DummyInvokable();
		List<MemorySegment> segmentList = pool.allocatePages(mockInvoke1, NUM_PAGES / 2);

		assertThrows(
			"Timeout triggered when requesting memory segments",
			MemoryAllocationException.class,
			() -> pool.allocatePages(mockInvoke2, NUM_PAGES));
		pool.release(segmentList);

		List<MemorySegment> allSegmentList = pool.allocatePages(mockInvoke1, NUM_PAGES);
		assertEquals(NUM_PAGES, allSegmentList.size());
		pool.release(allSegmentList);
	}
}
