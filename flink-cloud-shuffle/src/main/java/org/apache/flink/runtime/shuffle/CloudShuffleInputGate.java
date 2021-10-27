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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

import com.bytedance.css.client.ShuffleClient;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * CloudShuffleInputGate.
 */
public class CloudShuffleInputGate extends IndexedInputGate {

	public CloudShuffleInputGate(
			int gateIndex,
			String applicationId,
			ShuffleClient shuffleClient,
			int shuffleId,
			int reducerId,
			int numberOfMappers,
			int numberOfReducers) {
		// add implementation
	}

	@Override
	public int getGateIndex() {
		return 0;
	}

	@Override
	public int getNumberOfInputChannels() {
		return 0;
	}

	@Override
	public boolean isFinished() {
		return false;
	}

	@Override
	public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
		return Optional.empty();
	}

	@Override
	public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
		return Optional.empty();
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {

	}

	@Override
	public void resumeConsumption(int channelIndex) throws IOException {

	}

	@Override
	public InputChannel getChannel(int channelIndex) {
		return null;
	}

	@Override
	public void setup() throws IOException {

	}

	@Override
	public CompletableFuture<?> readRecoveredState(ExecutorService executor, ChannelStateReader reader) throws IOException {
		return null;
	}

	@Override
	public void requestPartitions() throws IOException {

	}

	@Override
	public void registerBufferReceivedListener(BufferReceivedListener listener) {

	}

	@Override
	public void close() throws Exception {

	}
}
