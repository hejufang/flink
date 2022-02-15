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
import org.apache.flink.runtime.io.network.api.CloudShuffleVerifierEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.shuffle.buffer.CloudShuffleBuffer;
import org.apache.flink.runtime.shuffle.util.CloudShuffleReadWriterUtil;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.common.protocol.PartitionGroup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link CloudShuffleWriter}.
 */
public class CloudShuffleWriterTest {

	@Test
	public void testSegmentSizeFull() throws IOException {
		final byte[][] received = new byte[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setSegmentSize(4 * 1024)
			.setNumberOfReducers(2)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(2, 1))
			.build();

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), 0);
		Assert.assertNull(received[0]);

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[2 * 1024]), 0);
		Assert.assertEquals(3 * 1024 + CloudShuffleReadWriterUtil.calculateHeaderSize(1), received[0].length);
	}

	@Test
	public void testGroupSizeFull() throws IOException {
		final byte[][] received = new byte[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setMaxBatchSize(6 * 1024L)
			.setNumberOfReducers(4)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(4, 4))
			.build();

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), 0);
		Assert.assertNull(received[0]);

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[2 * 1024]), 1);
		Assert.assertNull(received[0]);

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), 2);
		Assert.assertEquals((3 + 2) * 1024 + CloudShuffleReadWriterUtil.calculateHeaderSize(2), received[0].length);
	}

	@Test
	public void testMaxBatchSizeExceeded() throws IOException {
		final byte[][] received = new byte[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setNumberOfReducers(11)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(11, 11))
			.build();

		for (int i = 0; i < 10; i++) {
			shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), i);
		}
		// write 30KB already, write 3KB more
		Assert.assertNull(received[0]);
		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), 10);
		Assert.assertEquals(10 * 3 * 1024 + CloudShuffleReadWriterUtil.calculateHeaderSize(10), received[0].length);
	}

	@Test
	public void testBroadcastRecord() throws IOException {
		final byte[][] received = new byte[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setNumberOfReducers(11)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(11, 11))
			.build();
		shuffleWriter.broadcastRecord(ByteBuffer.wrap(new byte[3 * 1024]));
		Assert.assertEquals(10 * 3 * 1024 + CloudShuffleReadWriterUtil.calculateHeaderSize(10), received[0].length);
	}

	@Test
	public void testBroadcastEvent() throws IOException {
		final byte[][] received = new byte[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setNumberOfReducers(11)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(11, 11))
			.build();
		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[3 * 1024]), 0);
		Assert.assertNull(received[0]);

		shuffleWriter.broadcastEvent(EndOfPartitionEvent.INSTANCE);
		int bytes = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE).remaining();
		Assert.assertEquals(11L * bytes + CloudShuffleReadWriterUtil.calculateHeaderSize(11), received[0].length);
	}

	@Test
	public void testVerifierEvent() throws IOException {
		final byte[][] received = new byte[1][];
		final int[][] offset = new int[1][];
		final int[][] length = new int[1][];
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				verifyBatchPushParams(reducerIdArray, data, offsetArray, lengthArray);
				received[0] = data;
				offset[0] = offsetArray;
				length[0] = lengthArray;
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setNumberOfReducers(11)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(11, 11))
			.build();
		for (int i = 0; i < 11; i++) {
			shuffleWriter.addRecord(ByteBuffer.wrap(new byte[i]), i);
		}

		shuffleWriter.flushAll();
		shuffleWriter.broadcastVerifierEvent();
		byte[] data = received[0];
		InputStream inputStream = new ByteArrayInputStream(data);
		for (int i = 0; i < 11; i++) {
			CloudShuffleBuffer csb = CloudShuffleReadWriterUtil.readFromCloudShuffleService(
				inputStream,
				MemorySegmentFactory.wrap(new byte[4 * 1024]),
				MemorySegment::free);
			CloudShuffleVerifierEvent event = (CloudShuffleVerifierEvent) EventSerializer.fromBuffer(csb.getNetworkBuffer(), getClass().getClassLoader());
			Assert.assertEquals(i, event.getSendBytes());
		}
	}

	@Test
	public void testStatistics() throws IOException {
		final TestShuffleClient shuffleClient = new TestShuffleClient() {
			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				return new int[]{};
			}
		};
		final CloudShuffleWriter shuffleWriter = new TestCloudShuffleWriterBuilder()
			.setNumberOfReducers(10)
			.setInitialSizePerReducer(0L)
			.setMaxBatchSizePerGroup(3 * 1024)
			.setShuffleClient(shuffleClient)
			.setReducerIdToGroups(buildGroups(10, 2))
			.build();

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[1024]), 0);
		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[512]), 1);
		Assert.assertEquals(1024 + 512, shuffleWriter.getCurrentBatchWrittenBytes());
		Assert.assertEquals(1024, shuffleWriter.getCurrentBatchReducerWrittenPositions()[0]);
		Assert.assertEquals(512, shuffleWriter.getCurrentBatchReducerWrittenPositions()[1]);
		Assert.assertEquals(2, shuffleWriter.getCurrentBatchWrittenReducersPerGroup()[0]);
		Assert.assertEquals(1024 + 512, shuffleWriter.getCurrentBatchWrittenBytesPerGroup()[0]);

		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[1024]), 1);
		Assert.assertEquals(1024 + 512 + 1024, shuffleWriter.getCurrentBatchWrittenBytes());
		Assert.assertEquals(512 + 1024, shuffleWriter.getCurrentBatchReducerWrittenPositions()[1]);
		Assert.assertEquals(1024 + 512 + 1024, shuffleWriter.getCurrentBatchWrittenBytesPerGroup()[0]);

		// flush group-0
		shuffleWriter.addRecord(ByteBuffer.wrap(new byte[768]), 0);
		Assert.assertEquals(768, shuffleWriter.getCurrentBatchWrittenBytes());
		Assert.assertEquals(768, shuffleWriter.getCurrentBatchReducerWrittenPositions()[0]);
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchReducerWrittenPositions()[1]);
		Assert.assertEquals(1, shuffleWriter.getCurrentBatchWrittenReducersPerGroup()[0]);
		Assert.assertEquals(768, shuffleWriter.getCurrentBatchWrittenBytesPerGroup()[0]);

		// flush all
		shuffleWriter.flushAll();
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchWrittenBytes());
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchReducerWrittenPositions()[0]);
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchReducerWrittenPositions()[1]);
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchWrittenReducersPerGroup()[0]);
		Assert.assertEquals(0, shuffleWriter.getCurrentBatchWrittenBytesPerGroup()[0]);
	}

	public static Map<Integer, PartitionGroup> buildGroups(int numberOfReducers, int maxReducersPerGroup) {
		Map<Integer, PartitionGroup> reducerIdToGroups = new HashMap<>(numberOfReducers);
		int groups = (numberOfReducers + maxReducersPerGroup - 1) / maxReducersPerGroup;
		for (int i = 0; i < groups; i++) {
			int startPartition = i * maxReducersPerGroup;
			int endPartition = Math.min((i + 1) * maxReducersPerGroup, numberOfReducers);
			PartitionGroup group = new PartitionGroup(i, 1, startPartition, endPartition, "masterHost", 0, "slaveHost", 0);
			for (int j = startPartition; j < endPartition; j++) {
				reducerIdToGroups.put(j, group);
			}
		}
		return reducerIdToGroups;
	}

	private void verifyBatchPushParams(int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray) {
		Assert.assertEquals(reducerIdArray.length, offsetArray.length);
		Assert.assertEquals(offsetArray.length, lengthArray.length);
		int pos = 0;
		for (int i = 0; i < reducerIdArray.length; i++) {
			int offset = offsetArray[i];
			int length = lengthArray[i];
			Assert.assertEquals(pos, offset);
			pos += length;
		}
	}

	/**
	 * Builder for {@link CloudShuffleWriter}.
	 */
	public static class TestCloudShuffleWriterBuilder {

		String applicationId = "appId";
		int shuffleId = 1;
		int mapperId = 1;
		int mapperAttemptId = 1;
		int numberOfMappers = 10;
		int numberOfReducers = 10;
		long segmentSize = 4 * 1024;
		long maxBatchSize = 32 * 1024;
		long maxBatchSizePerGroup = 32 * 1024;
		long initialSizePerReducer = 1024;
		ShuffleClient shuffleClient = null;
		Map<Integer, PartitionGroup> reducerIdToGroups = null;

		public TestCloudShuffleWriterBuilder setApplicationId(String applicationId) {
			this.applicationId = applicationId;
			return this;
		}

		public TestCloudShuffleWriterBuilder setShuffleId(int shuffleId) {
			this.shuffleId = shuffleId;
			return this;
		}

		public TestCloudShuffleWriterBuilder setMapperId(int mapperId) {
			this.mapperId = mapperId;
			return this;
		}

		public TestCloudShuffleWriterBuilder setMapperAttemptId(int mapperAttemptId) {
			this.mapperAttemptId = mapperAttemptId;
			return this;
		}

		public TestCloudShuffleWriterBuilder setNumberOfMappers(int numberOfMappers) {
			this.numberOfMappers = numberOfMappers;
			return this;
		}

		public TestCloudShuffleWriterBuilder setNumberOfReducers(int numberOfReducers) {
			this.numberOfReducers = numberOfReducers;
			return this;
		}

		public TestCloudShuffleWriterBuilder setShuffleClient(ShuffleClient shuffleClient) {
			this.shuffleClient = shuffleClient;
			return this;
		}

		public TestCloudShuffleWriterBuilder setSegmentSize(long segmentSize) {
			this.segmentSize = segmentSize;
			return this;
		}

		public TestCloudShuffleWriterBuilder setMaxBatchSize(long maxBatchSize) {
			this.maxBatchSize = maxBatchSize;
			return this;
		}

		public TestCloudShuffleWriterBuilder setMaxBatchSizePerGroup(long maxBatchSizePerGroup) {
			this.maxBatchSizePerGroup = maxBatchSizePerGroup;
			return this;
		}

		public TestCloudShuffleWriterBuilder setInitialSizePerReducer(long initialSizePerReducer) {
			this.initialSizePerReducer = initialSizePerReducer;
			return this;
		}

		public TestCloudShuffleWriterBuilder setReducerIdToGroups(Map<Integer, PartitionGroup> reducerIdToGroups) {
			this.reducerIdToGroups = reducerIdToGroups;
			return this;
		}

		CloudShuffleWriter build() {
			return new CloudShuffleWriter(
				applicationId,
				shuffleId,
				mapperId,
				mapperAttemptId,
				numberOfMappers,
				numberOfReducers,
				shuffleClient,
				segmentSize,
				maxBatchSize,
				maxBatchSizePerGroup,
				initialSizePerReducer,
				reducerIdToGroups);
		}
	}
}
