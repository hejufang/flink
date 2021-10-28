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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CloudShuffleVerifierEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.shuffle.buffer.CloudShuffleBuffer;

import com.bytedance.css.client.ShuffleClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * CloudShuffleInputGate.
 */
public class CloudShuffleInputGate extends IndexedInputGate {
	private static final Logger LOG = LoggerFactory.getLogger(CloudShuffleInputGate.class);

	private final int gateIndex;

	private final int numberOfMappers;

	private final CloudShuffleReader cloudShuffleReader;

	private boolean hasReceivedAllEndOfPartitionEvents;

	private final InputChannelInfo[] inputChannelInfos;

	private final BitSet channelsWithEndOfPartitionEvents;

	private final long[] receiveBytes;

	public CloudShuffleInputGate(
			int gateIndex,
			String applicationId,
			ShuffleClient shuffleClient,
			int shuffleId,
			int reducerId,
			int numberOfMappers,
			int segmentSize) {
		this.gateIndex = gateIndex;
		this.numberOfMappers = numberOfMappers;

		LOG.info("Reader(appId={},shuffleId={},reducerId={},numberOfMappers={})",
				applicationId,
				shuffleClient,
				reducerId,
				numberOfMappers);
		this.cloudShuffleReader = new CloudShuffleReader(
				segmentSize,
				applicationId,
				shuffleId,
				reducerId,
				numberOfMappers,
				shuffleClient);

		this.inputChannelInfos = new InputChannelInfo[numberOfMappers];
		this.channelsWithEndOfPartitionEvents = new BitSet(numberOfMappers);
		this.receiveBytes = new long[numberOfMappers];
		this.hasReceivedAllEndOfPartitionEvents = false;
		for (int i = 0; i < numberOfMappers; i++) {
			this.inputChannelInfos[i] = new InputChannelInfo(gateIndex, i);
		}

		// mark the InputGate as available first
		markAvailable();
	}

	@Override
	public int getGateIndex() {
		return gateIndex;
	}

	@Override
	public int getNumberOfInputChannels() {
		return numberOfMappers;
	}

	@Override
	public boolean isFinished() {
		return hasReceivedAllEndOfPartitionEvents;
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		// this method is only used in DataSet Jobs and Iterative Jobs
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		if (hasReceivedAllEndOfPartitionEvents) {
			return Optional.empty();
		}

		CloudShuffleBuffer cloudBuffer = cloudShuffleReader.pollNext();
		if (cloudBuffer == null) {
			return Optional.empty();
		}

		return Optional.of(transformToBufferOrEvent(cloudBuffer.getNetworkBuffer(), cloudBuffer.getMapperId()));
	}

	private BufferOrEvent transformToBufferOrEvent(
			Buffer buffer,
			int mapperId) throws IOException, InterruptedException {
		if (buffer.isBuffer()) {
			receiveBytes[mapperId] += buffer.getSize();
			return new BufferOrEvent(buffer, inputChannelInfos[mapperId], true);
		} else {
			return transformEvent(buffer, mapperId);
		}
	}

	private BufferOrEvent transformEvent(
			Buffer buffer,
			int mapperId) throws IOException {
		final AbstractEvent event;
		try {
			event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
		} finally {
			buffer.recycleBuffer();
		}

		if (event.getClass() == EndOfPartitionEvent.class) {
			channelsWithEndOfPartitionEvents.set(mapperId);

			if (channelsWithEndOfPartitionEvents.cardinality() == numberOfMappers) {
				LOG.info("Receive All EndOfPartitionEvent from upstream tasks.");
				hasReceivedAllEndOfPartitionEvents = true;
				markAvailable();
			}

			LOG.info("Receive EndOfPartitionEvent from mapper(id={}).", mapperId);
			final boolean moreAvailable = !hasReceivedAllEndOfPartitionEvents;
			return new BufferOrEvent(event, inputChannelInfos[mapperId], moreAvailable, buffer.getSize());
		} else if (event.getClass() == CloudShuffleVerifierEvent.class) {
			final CloudShuffleVerifierEvent verifierEvent = (CloudShuffleVerifierEvent) event;
			final long expectBytes = verifierEvent.getSendBytes();
			if (expectBytes != receiveBytes[mapperId]) {
				throw new IllegalStateException("The length of sent bytes is not equal to the length of received bytes.");
			}
		}

		return new BufferOrEvent(event, inputChannelInfos[mapperId], true, buffer.getSize());
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		// used in iterative jobs
		throw new UnsupportedOperationException();
	}

	@Override
	public void resumeConsumption(int channelIndex) throws IOException {
		// used in checkpoint jobs
		throw new UnsupportedOperationException();
	}

	@Override
	public InputChannel getChannel(int channelIndex) {
		// this is bug in Flink
		// InputChannel is bind to SingleInputGate, which is Netty's thing
		// we should never let InputGate return InputChannel instance
		throw new UnsupportedOperationException();
	}

/*
	TODO. this part will be submitted in another MR.
	@Override
	public String getChannelType(int channelIndex) {
		return "CLOUD";
	}

	@Override
	public InputChannelInfo getChannelInfo(int channelIndex) {
		return inputChannelInfos[channelIndex];
	}
*/

	@Override
	public List<InputChannelInfo> getChannelInfos() {
		return java.util.Arrays.stream(inputChannelInfos, 0, getNumberOfInputChannels())
				.collect(Collectors.toList());
	}

	@Override
	public void setup() throws IOException {
		// do nothing
	}

	@Override
	public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) throws IOException {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void requestPartitions() throws IOException {
		// do nothing
	}

	@Override
	public void registerBufferReceivedListener(BufferReceivedListener listener) {
		// do nothing (this is only used in Checkpoint Aligner)
	}

	private void markAvailable() {
		CompletableFuture<?> toNotify;
		toNotify = availabilityHelper.getUnavailableToResetAvailable();
		toNotify.complete(null);
	}

	@Override
	public void close() throws Exception {
		cloudShuffleReader.close();
	}
}
