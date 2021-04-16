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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockResultPartitionMeta;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleUtils;
import org.apache.flink.runtime.io.network.partition.external.FixedLengthBufferPool;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Test the view of external result partition.
 */
public class ExternalBlockSubpartitionViewTest {
	private static final int SEGMENT_SIZE = 128;

	private static final int NUM_BUFFERS = 20;

	private FixedLengthBufferPool bufferPool;

	private int subpartitionIndex;

	private int bytesRead;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() {
		this.bufferPool = new FixedLengthBufferPool(NUM_BUFFERS, SEGMENT_SIZE);
	}

	@After
	public void after() {
		assertEquals("Not all buffers returned", NUM_BUFFERS, bufferPool.getNumberOfAvailableMemorySegments());
		this.bufferPool.lazyDestroy();
	}

	@Test(timeout = 60000)
	public void testManagingCredit() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		Tuple2<ExternalBlockResultPartitionMeta, Path> metaAndFile = createFilesAndMeta();

		ExecutorService executor = null;

		try {
			executor = spy(Executors.newFixedThreadPool(1));

			ViewReader viewReader = new ViewReader();
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(
					metaAndFile.f0,
					subpartitionIndex,
					metaAndFile.f1,
					executor,
					metaAndFile.f0.getResultPartitionID(),
					bufferPool,
					0,
					viewReader);
			viewReader.setView(view);

			view.addCredit(2);

			// Check the executor is submitting on the first batch of credits.
			verify(executor).execute(eq(view));

			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());

			while (view.isRunning()) {
				Thread.sleep(500);
			}

			assertEquals(0, view.getCreditUnsafe());
			view.addCredit(2);
			verify(executor, times(2)).execute(eq(view));

			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
			checkBufferAndRecycle(viewReader.getNextBufferBlocking());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	@Test(timeout = 60000)
	public void testGetNextBuffer() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		Tuple2<ExternalBlockResultPartitionMeta, Path> metaAndFile = createFilesAndMeta();

		ExecutorService executor = null;

		try {
			executor = Executors.newFixedThreadPool(1);

			ViewReader viewReader = new ViewReader();
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(
				metaAndFile.f0,
				subpartitionIndex,
				metaAndFile.f1,
				executor,
				metaAndFile.f0.getResultPartitionID(),
				bufferPool,
				0,
				viewReader);
			viewReader.setView(view);

			Random random = new Random();

			int remainingNumber = NUM_BUFFERS;
			while (remainingNumber > 0) {
				int nextCredit = random.nextInt(remainingNumber) + 1;
				view.addCredit(nextCredit);

				for (int i = 0; i < nextCredit; ++i) {
					Buffer buffer = viewReader.getNextBufferBlocking();
					remainingNumber--;
					checkBufferAndRecycle(buffer);

					if (i == nextCredit - 1) {
						assertFalse(view.isAvailable());
					}
				}
			}

			view.addCredit(1);
			Buffer eof = viewReader.getNextBufferBlocking();
			assertFalse(eof.isBuffer());
			assertEquals(EndOfPartitionEvent.INSTANCE, EventSerializer.fromBuffer(eof, this.getClass().getClassLoader()));
			assertFalse(view.isAvailable());
			eof.recycleBuffer();

			view.releaseAllResources();
			while (view.isRunning()) {
				Thread.sleep(10);
			}
		} finally {
			if (executor != null) {
				executor.shutdown();
			}
		}
	}

	@Test
	public void testReadFail() throws Exception {
		final int subpartitionIndex = 2;
		setupCheckForSubpartition(subpartitionIndex);

		Tuple2<ExternalBlockResultPartitionMeta, Path> metaAndFile = createFilesAndMeta();

		ExecutorService executor = null;

		try {
			executor = spy(Executors.newFixedThreadPool(1));

			ViewReader viewReader = spy(new ViewReader());
			ExternalBlockSubpartitionView view = new ExternalBlockSubpartitionView(
					metaAndFile.f0,
					subpartitionIndex,
					metaAndFile.f1,
					executor,
					metaAndFile.f0.getResultPartitionID(),
					bufferPool,
					0,
					viewReader);
			viewReader.setView(view);

			// Remove the data files directly
			boolean success = metaAndFile.f1.toFile().delete();
			assertTrue("Delete the data file failed", success);

			view.addCredit(1);

			// Should be notified in expected period.
			verify(viewReader, timeout(10000)).notifyDataAvailable();

			assertNull(view.getNextBuffer());
			assertNotNull(view.getFailureCause());
		} finally {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
	}

	// -------------------------------- Internal Utilities ------------------------------------

	private Tuple2<ExternalBlockResultPartitionMeta, Path> createFilesAndMeta() throws Exception {
		ResultPartitionID resultPartitionID = new ResultPartitionID();
		String root = tempFolder.newFolder().getAbsolutePath();
		String rpRootDir = ExternalBlockShuffleUtils.generatePartitionRootPath(root, resultPartitionID);
		String subPartitionFile = ExternalBlockShuffleUtils.generateSubPartitionFile(rpRootDir, subpartitionIndex);
		Path tempSubPartitionFile = new File(rpRootDir + subpartitionIndex).toPath();
		Files.createDirectory(tempSubPartitionFile.getParent());

		YarnFileChannelBoundedData yarnChannel = YarnFileChannelBoundedData.create(
				tempSubPartitionFile,
				subPartitionFile);

		int totalWrite = 0;
		for (int i = 0; i < NUM_BUFFERS; ++i) {
			long nextLengthToWriteRnd = new Random().nextInt(SEGMENT_SIZE);
			long nextLengthToWrite = nextLengthToWriteRnd == 0 ? 1 : nextLengthToWriteRnd;
			MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
			NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
			for (int j = 0; j < nextLengthToWrite; j++) {
				buffer.asByteBuf().writeByte(totalWrite + j + subpartitionIndex);
			}

			yarnChannel.writeBuffer(buffer);
			totalWrite += nextLengthToWrite;
		}

		yarnChannel.writeBuffer(EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE).build());
		yarnChannel.finishWrite();

		return new Tuple2<>(new ExternalBlockResultPartitionMeta(new ResultPartitionID()), new File(subPartitionFile).toPath());
	}

	private void setupCheckForSubpartition(int subpartitionIndex) {
		this.subpartitionIndex = subpartitionIndex;
		this.bytesRead = 0;
	}

	private void checkBufferAndRecycle(Buffer buffer) {
		assertTrue(buffer.isBuffer());
		assertTrue(buffer.getSize() > 0);
		ByteBuf byteBuf = buffer.asByteBuf();
		for (int i = 0; i < buffer.getSize(); i++) {
			byte actualValue = byteBuf.readByte();
			assertEquals("bytesRead: " + bytesRead + ", offset: " + i,
				(byte) ((bytesRead + i + subpartitionIndex) & 0x0ff), actualValue);
		}
		bytesRead += buffer.getSize();
		buffer.recycleBuffer();
	}

	private class ViewReader implements BufferAvailabilityListener {
		private final BlockingQueue<Buffer> bufferRead = new LinkedBlockingQueue<>();
		private ExternalBlockSubpartitionView view;

		public void setView(ExternalBlockSubpartitionView view) {
			this.view = view;
		}

		@Override
		public void notifyDataAvailable() {
			while (true) {
				ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();

				if (bufferAndBacklog != null) {
					bufferRead.add(bufferAndBacklog.buffer());

					if (!bufferAndBacklog.isDataAvailable()) {
						break;
					}
				}
			}
		}

		public Buffer getNextBufferBlocking() throws InterruptedException {
			return bufferRead.take();
		}
	}
}
