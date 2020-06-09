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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.io.network.partition.ExternalBlockSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.io.network.util.TestConsumerCallback;
import org.apache.flink.runtime.io.network.util.TestSubpartitionConsumer;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createFilledBufferConsumer;

/**
 * Test the reading and writing of the files produced by external result partition.
 */
public class ExternalResultPartitionReadWriteTest {

	/*************************** Global configurations. ***********************/

	private static final ResultPartitionID partitionID = new ResultPartitionID();

	public static final int PAGE_SIZE = 4096;

	public static final int NUM_PAGES = 1024 * 1024 / PAGE_SIZE;

	public static final int MEMORY_SIZE = PAGE_SIZE * NUM_PAGES;

	public static final int NUM_PARTITIONS = 4;

	/*********************** Internal variables for cases *********************/

	/** Total directories on both SATA and SSD disks for external shuffle. */
	private String outputLocalDir;

	@Before
	public void before() {
		outputLocalDir = ExternalBlockShuffleUtils.generatePartitionRootPath(
				new File(System.getProperty("user.dir")).getParent(), partitionID);
		cleanExternalShuffleDirs();
	}

	@After
	public void after() {
		cleanExternalShuffleDirs();
	}

	@Test
	public void testWriteAndRead() throws Exception {
		runTest();
	}

	private void cleanExternalShuffleDirs() {
		try {
			if (outputLocalDir != null) {
				FileSystem.getLocalFileSystem().delete(new Path(outputLocalDir), true);
			}
		} catch (Throwable e) {
			// do nothing
		}
	}

	private void runTest() throws Exception {

		// 1. Each sub partition write 5 records.
		int numRecordsEachPartition = 5;

		// 2. write external shuffle files to disk
		writeRecordsToFile(numRecordsEachPartition);

		// 3. read external shuffle files and test data's validity
		assertFileContentConsistent(numRecordsEachPartition);
	}

	private void writeRecordsToFile(int numRecordsEachPartition) throws IOException {
		// choose a directory to write external shuffle, we can choose directory based on disk type
		ResultSubpartition[] resultSubpartitions = new ResultSubpartition[NUM_PARTITIONS];
		ResultPartition resultPartition = new ResultPartition("taskName",
				0,
				partitionID,
				ResultPartitionType.BLOCKING,
				resultSubpartitions,
				10,
				new ResultPartitionManager(),
				null,
				null);
		for (int i = 0; i < NUM_PARTITIONS; i++) {
			File tempFile = new File(outputLocalDir + i);
			resultSubpartitions[i] = BoundedBlockingSubpartitionType.YARN.create(
					i,
					resultPartition,
					tempFile,
					0);
		}

		for (int i = 0; i < NUM_PARTITIONS; i++) {
			for (int j = 0; j < numRecordsEachPartition; j++) {
				resultPartition.addBufferConsumer(createFilledBufferConsumer(PAGE_SIZE, PAGE_SIZE, true), i);
			}
		}

		resultPartition.finish();
		resultPartition.release();
	}

	private void assertFileContentConsistent(int numRecordsEachPartition) throws Exception {
		ExternalBlockResultPartitionMeta meta = new ExternalBlockResultPartitionMeta(partitionID);

		FixedLengthBufferPool bufferPool = new FixedLengthBufferPool(10, PAGE_SIZE);
		ExecutorService producerThreadPool = Executors.newFixedThreadPool(
			NUM_PARTITIONS, new DispatcherThreadFactory(new ThreadGroup("Disk IO Thread"), "IO thread for Disk"));
		ExecutorService consumerThreadPool = Executors.newFixedThreadPool(
			NUM_PARTITIONS, new DispatcherThreadFactory(new ThreadGroup("Netty IO Thread"), "IO thread for Network"));

		final List<Future<Boolean>> consumerResults = Lists.newArrayList();

		TestConsumerCallback.RecyclingCallback resultValidator =
				new TestConsumerCallback.RecyclingCallback();

		for (int i = 0; i < NUM_PARTITIONS; i++) {

			TestSubpartitionConsumer consumer = new TestSubpartitionConsumer(false, resultValidator);

			String outputFile = ExternalBlockShuffleUtils.generateSubPartitionFile(outputLocalDir, i);
			ExternalBlockSubpartitionView subpartitionView = new ExternalBlockSubpartitionView(
					meta,
					i,
					new File(outputFile).toPath(),
					producerThreadPool,
					partitionID,
					bufferPool,
					0,
					consumer);

			consumer.setSubpartitionView(subpartitionView);

			subpartitionView.addCredit(Integer.MAX_VALUE);
			consumerResults.add(consumerThreadPool.submit(consumer));
		}

		// Wait for the results
		for (Future<Boolean> res : consumerResults) {
			try {
				res.get(100, TimeUnit.SECONDS);
			} catch (TimeoutException e) {
				throw new TimeoutException("There has been a timeout in the test. This " +
					"indicates that there is a bug/deadlock in the tested subpartition " +
					"view.");
			}
		}

		Assert.assertEquals(numRecordsEachPartition * NUM_PARTITIONS, resultValidator.getNumberOfReadBuffers());
		Assert.assertEquals(NUM_PARTITIONS, resultValidator.getNumberOfReadEvents());

		consumerThreadPool.shutdown();
		consumerThreadPool.awaitTermination(10, TimeUnit.SECONDS);
		List<Runnable> leftProducers = producerThreadPool.shutdownNow();
		producerThreadPool.awaitTermination(10, TimeUnit.SECONDS);
		Assert.assertTrue("All the producers should finish while " + leftProducers.size() + " producer(s) is(are) left", leftProducers.isEmpty());
	}
}
