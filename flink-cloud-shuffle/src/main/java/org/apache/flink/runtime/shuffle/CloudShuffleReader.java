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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.SimpleHistogram;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.shuffle.buffer.CloudShuffleBuffer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.css.client.ShuffleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;

/**
 * CloudShuffleReader.
 */
public class CloudShuffleReader implements BufferRecycler {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleReader.class);

	private static final int NUM_BUFFERS = 2;
	public static final long RETRY_INTERVAL_TIME_MS = 10_000L;
	public static final int MAX_RETRY_TIMES = 60 * 5;

	private final ArrayDeque<MemorySegment> buffers;

	private InputStream inputStream;

	private boolean reachEnd;

	private boolean isStageReady = false;
	private int retryTimes = 0;
	private String applicationId;
	private int shuffleId;
	private int reducerId;
	private int numberOfMappers;
	private ShuffleClient shuffleClient;

	// css read latency metrics
	private final SimpleHistogram cloudShuffleReadLatency;

	public CloudShuffleReader(
			int memorySegmentSize,
			String applicationId,
			int shuffleId,
			int reducerId,
			int numberOfMappers,
			ShuffleClient shuffleClient) {
		this.buffers = new ArrayDeque<>(NUM_BUFFERS);
		this.applicationId = applicationId;
		this.shuffleId = shuffleId;
		this.reducerId = reducerId;
		this.numberOfMappers = numberOfMappers;
		this.shuffleClient = shuffleClient;

		for (int i = 0; i < NUM_BUFFERS; i++) {
			buffers.addLast(MemorySegmentFactory.allocateUnpooledOffHeapMemory(memorySegmentSize, null));
		}

		this.reachEnd = false;
		this.cloudShuffleReadLatency = new SimpleHistogram(SimpleHistogram.buildSlidingTimeWindowReservoirHistogram());
	}

	public CloudShuffleBuffer pollNext() throws IOException {
		if (!checkStageReady()) {
			return null;
		}

		final MemorySegment memory = buffers.pollFirst();
		if (memory == null) {
			return null;
		}

		long readStartTime = System.nanoTime();
		final CloudShuffleBuffer next = CloudShuffleReadWriterUtil.readFromCloudShuffleService(inputStream, memory, this);
		this.cloudShuffleReadLatency.update((System.nanoTime() - readStartTime) / 1000);
		if (next == null) {
			reachEnd = true;
			recycle(memory);
		}

		return next;
	}

	/**
	 * Check the status of the shuffle stage, if not ready, will retry later, if ready, here will sync the state of this shuffle stage.
	 */
	private boolean checkStageReady() {
		if (!isStageReady) {
			if (retryTimes >= MAX_RETRY_TIMES) {
				throw new FlinkRuntimeException(String
					.format("This shuffle %s is not ready, and has retried %s times, reach the max retry times.", shuffleId, retryTimes));
			} else {
				boolean isReady;
				try {
					isReady = shuffleClient.validShuffleStageReady(applicationId, shuffleId);
				} catch (IOException e) {
					LOG.error("Some error throws while checking the cloud shuffle stage ready.", e);
					throw new FlinkRuntimeException("Some error throws while css client check the shuffle ready.", e);
				}

				if (isReady) {
					LOG.info("The shuffle-{} is ready to read now.", shuffleId);
					isStageReady = true;
					try {
						inputStream = shuffleClient.readPartitions(
							applicationId,
							shuffleId,
							new int[]{reducerId},
							0,
							numberOfMappers);
					} catch (IOException e) {
						LOG.error("Fail to connect to remote shuffle service.", e);
						throw new FlinkRuntimeException("Fail to connect to remote shuffle service.", e);
					}
				} else {
					try {
						retryTimes++;
						Thread.sleep(RETRY_INTERVAL_TIME_MS);
						LOG.info("Wait css shuffle ready for {}ms, and retryTimes is {}.", RETRY_INTERVAL_TIME_MS, retryTimes);
					} catch (InterruptedException e) {
						LOG.warn("Wait css shuffle ready has some error.", e);
						throw new FlinkRuntimeException("Wait css shuffle ready has some error.", e);
					}
				}
			}
		}
		return isStageReady;
	}

	public boolean isReachEnd() {
		return reachEnd;
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		// this will be called after bytes are copied to StreamTaskNetworkInput
		buffers.addLast(memorySegment);
	}

	public void close() throws IOException {
		while (!buffers.isEmpty()) {
			MemorySegment segment = buffers.pollFirst();
			segment.free();
		}
		if (inputStream != null) {
			inputStream.close();
		}
	}

	public SimpleHistogram getCloudShuffleReadLatency() {
		return this.cloudShuffleReadLatency;
	}
}
