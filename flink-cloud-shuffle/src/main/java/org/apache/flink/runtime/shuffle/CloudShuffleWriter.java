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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.SimpleHistogram;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CloudShuffleVerifierEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;
import org.apache.flink.util.Preconditions;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.common.protocol.PartitionGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Writer in {@link CloudShuffleResultPartition}.
 */
public class CloudShuffleWriter implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleWriter.class);

	private final String applicationId;

	private final int shuffleId;

	private final int mapperId;

	private final int mapperAttemptId;

	private final int numberOfMappers;

	private final int numberOfReducers;

	private final ShuffleClient shuffleClient;

	private final ByteBuffer[] headerAndBufferArray;

	// used to send verifier event
	private final long[] sendBytes;

	private long outBytes;

	private final PartitionGroup[] reducerIdToGroups;

	private final PartitionGroup[] groups;

	// mapping between reducerId and its unsent data
	private final byte[][] currentBatchReducerBytes;

	// written positions per reducer
	private final int[] currentBatchReducerWrittenPositions;

	// record current written bytes per group
	private final int[] currentBatchWrittenBytesPerGroup;

	// record written reducers per group
	private final int[] currentBatchWrittenReducersPerGroup;

	private int currentBatchWrittenBytes;

	private final long segmentSize;

	private final long maxBatchSize;

	private final long maxBatchSizePerGroup;

	private final int maxDataBytesPerReducer;

	private final int initialSizePerReducer;

	// css write latency metrics
	private final SimpleHistogram cloudShuffleWriteLatency;

	// performance info
	private int numberOfFlushBySegmentSize;
	private int numberOfFlushByBatchSizePerGroup;
	private int numberOfFlushByBatchSize;

	public CloudShuffleWriter(
			String applicationId,
			int shuffleId,
			int mapperId,
			int mapperAttemptId,
			int numberOfMappers,
			int numberOfReducers,
			ShuffleClient shuffleClient,
			long segmentSize,
			long maxBatchSize,
			long maxBatchSizePerGroup,
			long initialSizePerReducer,
			Map<Integer, PartitionGroup> reducerIdToGroups) {
		this.applicationId = applicationId;
		this.shuffleId = shuffleId;
		this.mapperId = mapperId;
		this.mapperAttemptId = mapperAttemptId;
		this.numberOfMappers = numberOfMappers;
		this.numberOfReducers = numberOfReducers;
		this.shuffleClient = shuffleClient;
		this.headerAndBufferArray = CloudShuffleReadWriterUtil.allocatedWriteBufferArray();
		this.sendBytes = new long[numberOfReducers];
		this.currentBatchReducerBytes = new byte[numberOfReducers][];
		this.currentBatchReducerWrittenPositions = new int[numberOfReducers];
		this.segmentSize = segmentSize;
		this.maxBatchSize = maxBatchSize;
		this.maxBatchSizePerGroup = maxBatchSizePerGroup;

		final int numberOfGroups = reducerIdToGroups.values().size();
		this.currentBatchWrittenBytesPerGroup = new int[numberOfGroups];
		this.currentBatchWrittenReducersPerGroup = new int[numberOfGroups];
		this.maxDataBytesPerReducer = (int) segmentSize - CloudShuffleReadWriterUtil.calculateHeaderSize(1);

		Preconditions.checkArgument(initialSizePerReducer <= maxDataBytesPerReducer);
		this.initialSizePerReducer = (int) initialSizePerReducer;

		this.reducerIdToGroups = new PartitionGroup[reducerIdToGroups.size()];
		final Set<PartitionGroup> distinctGroups = new HashSet<>();
		for (Map.Entry<Integer, PartitionGroup> entry : reducerIdToGroups.entrySet()) {
			this.reducerIdToGroups[entry.getKey()] = entry.getValue();
			distinctGroups.add(entry.getValue());
		}

		int size = distinctGroups.size();
		this.groups = distinctGroups.toArray(new PartitionGroup[size]);

		this.numberOfFlushBySegmentSize = 0;
		this.numberOfFlushByBatchSizePerGroup = 0;
		this.numberOfFlushByBatchSize = 0;

		this.cloudShuffleWriteLatency = new SimpleHistogram(SimpleHistogram.buildSlidingWindowReservoirHistogram(CloudShuffleOptions.CLOUD_HISTOGRAM_SIZE));
	}

	public void broadcastRecord(ByteBuffer record) throws IOException {
		for (int i = 0; i < numberOfReducers; i++) {
			record.rewind();
			addRecord(record, i);
		}
	}

	public void addRecord(ByteBuffer record, int reducerId) throws IOException {
		int recordSize = record.remaining();

		if (recordSize >= maxDataBytesPerReducer) {
			final String message = String.format("Record size is too large(%s bytes), but segment size is (%s bytes).", record, segmentSize);
			throw new UnsupportedOperationException(message);
		}

		final PartitionGroup group = reducerIdToGroups[reducerId];
		final int groupId = reducerIdToGroups[reducerId].partitionGroupId;

		if (currentBatchWrittenBytesPerGroup[groupId] + recordSize >= maxBatchSizePerGroup) {
			numberOfFlushByBatchSizePerGroup++;
			// flush current group if exceeding max size per group
			flushCurrentGroupToCloudShuffleService(group);
		}

		if (currentBatchWrittenBytes + recordSize >= maxBatchSize) {
			numberOfFlushByBatchSize++;
			// flush all if exceeding max size
			flushAllToCloudShuffleService();
		}

		if (currentBatchReducerBytes[reducerId] == null) {
			final byte[] dst = new byte[Math.max(recordSize, initialSizePerReducer)];
			record.get(dst, 0, recordSize);
			currentBatchReducerBytes[reducerId] = dst;
			currentBatchWrittenBytes += recordSize;
			currentBatchWrittenBytesPerGroup[groupId] += recordSize;
			currentBatchWrittenReducersPerGroup[groupId]++;
			currentBatchReducerWrittenPositions[reducerId] = recordSize;
		} else {
			if (recordSize + currentBatchReducerWrittenPositions[reducerId] > currentBatchReducerBytes[reducerId].length) {
				if (!grow(reducerId)) {
					numberOfFlushBySegmentSize++;
					// flush current group if failing to grow the data array
					flushCurrentGroupToCloudShuffleService(group);
				}
				addRecord(record, reducerId);
				return;
			}

			record.get(currentBatchReducerBytes[reducerId], currentBatchReducerWrittenPositions[reducerId], recordSize);
			currentBatchReducerWrittenPositions[reducerId] += recordSize;
			currentBatchWrittenBytes += recordSize;
			currentBatchWrittenBytesPerGroup[groupId] += recordSize;
		}
	}

	private boolean grow(int reducerId) {
		byte[] currentReducerBytes = currentBatchReducerBytes[reducerId];
		if (maxDataBytesPerReducer == currentReducerBytes.length) {
			return false;
		}

		int targetSize = Math.min(currentReducerBytes.length << 1, maxDataBytesPerReducer);
		currentBatchReducerBytes[reducerId] = Arrays.copyOf(currentReducerBytes, targetSize);

		return true;
	}

	/**
	 * (1) do not collect metrics.
	 * (2) do not use any mutable variables.
	 */
	public void broadcastEvent(AbstractEvent event) throws IOException {
		byte[] eventBytes;
		Buffer buffer = EventSerializer.toBufferConsumer(event).build();
		try {
			eventBytes = CloudShuffleReadWriterUtil.writeToCloudShuffleService(
				mapperId,
				buffer,
				headerAndBufferArray);
		} finally {
			buffer.recycleBuffer();
		}

		// send group by group
		for (PartitionGroup group : groups) {
			final int numberOfReducersInGroup = group.endPartition - group.startPartition;
			byte[] data = new byte[eventBytes.length * numberOfReducersInGroup];
			int[] reducerIdArray = new int[numberOfReducersInGroup];
			int[] offsetArray = new int[numberOfReducersInGroup];
			int[] lengthArray = new int[numberOfReducersInGroup];

			for (int i = 0; i < group.endPartition - group.startPartition; i++) {
				System.arraycopy(eventBytes, 0, data, i * eventBytes.length, eventBytes.length);
				reducerIdArray[i] = i + group.startPartition;
				offsetArray[i] = i * eventBytes.length;
				lengthArray[i] = eventBytes.length;
			}

			long writeStartTime = System.nanoTime();
			shuffleClient.batchPushData(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				reducerIdArray,
				data,
				offsetArray,
				lengthArray,
				numberOfMappers,
				numberOfReducers,
				false);
			this.cloudShuffleWriteLatency.update((System.nanoTime() - writeStartTime) / 1000);
		}
	}

	@VisibleForTesting
	public void broadcastVerifierEvent() throws IOException {
		Buffer buffer = EventSerializer.toBufferConsumer(new CloudShuffleVerifierEvent(sendBytes[0])).build();
		byte[] tempBytes;
		try {
			tempBytes = CloudShuffleReadWriterUtil.writeToCloudShuffleService(
				mapperId,
				buffer,
				headerAndBufferArray);
		} finally {
			buffer.recycleBuffer();
		}

		// send group by group
		for (PartitionGroup group : groups) {
			final int numberOfReducersInGroup = group.endPartition - group.startPartition;
			byte[] data = new byte[tempBytes.length * numberOfReducersInGroup];
			int[] reducerIdArray = new int[numberOfReducersInGroup];
			int[] offsetArray = new int[numberOfReducersInGroup];
			int[] lengthArray = new int[numberOfReducersInGroup];

			for (int i = 0; i < numberOfReducersInGroup; i++) {
				int reducerId = i + group.startPartition;

				byte[] eventBytes;
				Buffer eventBuffer = EventSerializer.toBufferConsumer(new CloudShuffleVerifierEvent(sendBytes[reducerId])).build();
				try {
					eventBytes = CloudShuffleReadWriterUtil.writeToCloudShuffleService(
						mapperId,
						eventBuffer,
						headerAndBufferArray);
				} finally {
					eventBuffer.recycleBuffer();
				}
				System.arraycopy(eventBytes, 0, data, i * eventBytes.length, eventBytes.length);
				reducerIdArray[i] = reducerId;
				offsetArray[i] = i * eventBytes.length;
				lengthArray[i] = eventBytes.length;
			}

			long writeStartTime = System.nanoTime();
			shuffleClient.batchPushData(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				reducerIdArray,
				data,
				offsetArray,
				lengthArray,
				numberOfMappers,
				numberOfReducers,
				false);
			this.cloudShuffleWriteLatency.update((System.nanoTime() - writeStartTime) / 1000);
		}
	}

	private void flushCurrentGroupToCloudShuffleService(PartitionGroup group) throws IOException {
		final int groupId = group.partitionGroupId;

		if (currentBatchWrittenBytesPerGroup[groupId] == 0) {
			return;
		}

		final int numberOfReducersInGroup = group.endPartition - group.startPartition;
		byte[] data = new byte[currentBatchWrittenBytesPerGroup[groupId] + CloudShuffleReadWriterUtil.calculateHeaderSize(currentBatchWrittenReducersPerGroup[groupId])];
		int[] reducerIdArray = new int[numberOfReducersInGroup];
		int[] offsetArray = new int[numberOfReducersInGroup];
		int[] lengthArray = new int[numberOfReducersInGroup];

		int writtenPos = 0;
		for (int i = group.startPartition; i < group.endPartition; i++) {
			final int index = i - group.startPartition;
			reducerIdArray[index] = i;
			if (currentBatchReducerBytes[i] != null) {
				int length = CloudShuffleReadWriterUtil.writeToCloudShuffleService(
					mapperId,
					currentBatchReducerBytes[i],
					currentBatchReducerWrittenPositions[i],
					headerAndBufferArray,
					data,
					writtenPos);

				offsetArray[index] = writtenPos;
				lengthArray[index] = length;

				writtenPos += length;

				// metrics
				sendBytes[i] += currentBatchReducerWrittenPositions[i];

				// reset
				currentBatchReducerBytes[i] = null;
				currentBatchReducerWrittenPositions[i] = 0;
			} else {
				offsetArray[index] = writtenPos;
				lengthArray[index] = 0;
			}
		}
		// collect metrics
		outBytes += currentBatchWrittenBytesPerGroup[groupId];

		long writeStartTime = System.nanoTime();
		shuffleClient.batchPushData(
			applicationId,
			shuffleId,
			mapperId,
			mapperAttemptId,
			reducerIdArray,
			data,
			offsetArray,
			lengthArray,
			numberOfMappers,
			numberOfReducers,
			false);
		this.cloudShuffleWriteLatency.update((System.nanoTime() - writeStartTime) / 1000);

		// reset
		currentBatchWrittenBytes -= currentBatchWrittenBytesPerGroup[groupId];
		currentBatchWrittenBytesPerGroup[groupId] = 0;
		currentBatchWrittenReducersPerGroup[groupId] = 0;
	}

	private void flushAllToCloudShuffleService() throws IOException {
		if (currentBatchWrittenBytes == 0) {
			return;
		}

		for (PartitionGroup group : groups) {
			flushCurrentGroupToCloudShuffleService(group);
		}

		// reset
		currentBatchWrittenBytes = 0;
	}

	public void flushAll() throws IOException {
		flushAllToCloudShuffleService();
	}

	public void finishWrite() throws IOException {
		flushAll();

		broadcastVerifierEvent();

		shuffleClient.mapperEnd(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				numberOfMappers);
		LOG.info("Writer finish the mapper(" +
				"appId={},shuffleId={},mapperId={},mapperAttemptId={},numberOfMappers={}," +
				"numberOfFlushBySegmentSize={},numberOfFlushByBatchSizePerGroup={},numberOfFlushByBatchSize={}" +
				").",
			applicationId,
			shuffleId,
			mapperId,
			mapperAttemptId,
			numberOfMappers,
			numberOfFlushBySegmentSize,
			numberOfFlushByBatchSizePerGroup,
			numberOfFlushByBatchSize);
	}

	@Override
	public void close() throws IOException {
		shuffleClient.shutDown();
	}

	public long getOutBytes() {
		return outBytes;
	}

	public SimpleHistogram getCloudShuffleWriteLatency() {
		return cloudShuffleWriteLatency;
	}

	@VisibleForTesting
	public byte[][] getCurrentBatchReducerBytes() {
		return currentBatchReducerBytes;
	}

	@VisibleForTesting
	public int[] getCurrentBatchReducerWrittenPositions() {
		return currentBatchReducerWrittenPositions;
	}

	@VisibleForTesting
	public int[] getCurrentBatchWrittenBytesPerGroup() {
		return currentBatchWrittenBytesPerGroup;
	}

	@VisibleForTesting
	public int[] getCurrentBatchWrittenReducersPerGroup() {
		return currentBatchWrittenReducersPerGroup;
	}

	@VisibleForTesting
	public int getCurrentBatchWrittenBytes() {
		return currentBatchWrittenBytes;
	}
}
