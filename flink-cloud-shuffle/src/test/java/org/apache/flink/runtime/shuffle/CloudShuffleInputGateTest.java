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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.client.stream.CssInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for {@link CloudShuffleInputGate}.
 */
public class CloudShuffleInputGateTest {

	@Test
	public void testPollNext() throws IOException, InterruptedException {
		final ShuffleClient shuffleClient = createShuffleClient(1);
		final CloudShuffleWriter shuffleWriter = new CloudShuffleWriterTest.TestCloudShuffleWriterBuilder()
			.setShuffleClient(shuffleClient)
			.setMapperId(0)
			.setNumberOfMappers(1)
			.setNumberOfReducers(1)
			.setReducerIdToGroups(CloudShuffleWriterTest.buildGroups(1, 1))
			.build();

		byte[] data = new byte[]{1, 2};
		shuffleWriter.addRecord(ByteBuffer.wrap(data), 0);
		shuffleWriter.finishWrite();

		final CloudShuffleInputGate inputGate = new TestCloudShuffleInputGateBuilder()
			.setShuffleClient(shuffleClient)
			.setReducerId(0)
			.setNumberOfMappers(1)
			.build();

		BufferOrEvent buf1 = inputGate.pollNext().get();
		Assert.assertEquals(data.length, buf1.getSize());
		Assert.assertEquals(1, buf1.getBuffer().getNioBufferReadable().get(0));
		Assert.assertEquals(2, buf1.getBuffer().getNioBufferReadable().get(1));

		BufferOrEvent buf2 = inputGate.pollNext().get();
		Assert.assertTrue(buf2.getEvent() instanceof EndOfPartitionEvent);
	}

	private ShuffleClient createShuffleClient(int numberOfReducers) {
		return new TestShuffleClient(numberOfReducers) {
			byte[][] reducerData = new byte[numberOfReducers][];

			@Override
			public int[] batchPushData(String applicationId, int shuffleId, int mapperId, int mapperAttemptId, int[] reducerIdArray, byte[] data, int[] offsetArray, int[] lengthArray, int numMappers, int numPartitions, boolean skipCompress) throws IOException {
				for (int i = 0; i < reducerIdArray.length; i++) {
					byte[] cur = reducerData[i];
					if (cur == null) {
						cur = new byte[0];
					}
					byte[] dst = new byte[cur.length + lengthArray[i]];
					System.arraycopy(cur, 0, dst, 0, cur.length);
					System.arraycopy(data, offsetArray[i], dst, cur.length, lengthArray[i]);
					reducerData[i] = dst;
				}
				return new int[]{};
			}

			@Override
			public CssInputStream readPartitions(String applicationId, int shuffleId, int[] reduceIds, int startMapIndex, int endMapIndex) throws IOException {
				return new TestCssInputStream(reducerData[reduceIds[0]]);
			}

			@Override
			public void mapperEnd(String s, int i, int i1, int i2, int i3) throws IOException {}
		};
	}

	private static class TestCloudShuffleInputGateBuilder {
		String taskName = "task-name";
		int gateIndex = 1;
		String applicationId = "app-id";
		ShuffleClient shuffleClient = null;
		int shuffleId = 1;
		int reducerId = 1;
		int numberOfMappers = 10;
		int segmentSize = 4 * 1024;

		public TestCloudShuffleInputGateBuilder setTaskName(String taskName) {
			this.taskName = taskName;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setGateIndex(int gateIndex) {
			this.gateIndex = gateIndex;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setApplicationId(String applicationId) {
			this.applicationId = applicationId;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setShuffleClient(ShuffleClient shuffleClient) {
			this.shuffleClient = shuffleClient;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setShuffleId(int shuffleId) {
			this.shuffleId = shuffleId;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setReducerId(int reducerId) {
			this.reducerId = reducerId;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setNumberOfMappers(int numberOfMappers) {
			this.numberOfMappers = numberOfMappers;
			return this;
		}

		public TestCloudShuffleInputGateBuilder setSegmentSize(int segmentSize) {
			this.segmentSize = segmentSize;
			return this;
		}

		CloudShuffleInputGate build() {
			return new CloudShuffleInputGate(
				taskName,
				gateIndex,
				applicationId,
				shuffleClient,
				shuffleId,
				reducerId,
				numberOfMappers,
				segmentSize);
		}
	}
}
