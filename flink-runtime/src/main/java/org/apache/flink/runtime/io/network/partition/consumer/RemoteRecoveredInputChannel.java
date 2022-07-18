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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel reads recovered state from previous unaligned checkpoint snapshots
 * via {@link ChannelStateReader} and then converts into {@link RemoteInputChannel} finally.
 */
public class RemoteRecoveredInputChannel extends RecoveredInputChannel {
	private final ConnectionID connectionId;
	private final ConnectionManager connectionManager;

	private boolean exclusiveBuffersAssigned;

	RemoteRecoveredInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ConnectionID connectionId,
			ConnectionManager connectionManager,
			int initialBackOff,
			int maxBackoff,
			InputChannelMetrics metrics,
			long maxDelayTimeMs,
			ScheduledExecutorService executor,
			boolean isRecoverable,
			boolean memorySegmentPackageEnable) {
		super(inputGate,
				channelIndex,
				partitionId,
				initialBackOff,
				maxBackoff,
				metrics.getNumBytesInRemoteCounter(),
				metrics.getNumBuffersInRemoteCounter(),
				maxDelayTimeMs,
				executor,
				isRecoverable,
				memorySegmentPackageEnable);

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
	}

	@Override
	public InputChannel toInputChannel() throws IOException {
		RemoteInputChannel remoteInputChannel = new RemoteInputChannel(
			inputGate,
			getChannelIndex(),
			partitionId,
			connectionId,
			connectionManager,
			initialBackoff,
			maxBackoff,
			numBytesIn,
			numBuffersIn,
			new SimpleCounter(),
			maxDelayTimeMs,
			executor,
			isRecoverable,
			memorySegmentPackageEnable);
		remoteInputChannel.assignExclusiveSegments();
		return remoteInputChannel;
	}

	void assignExclusiveSegments() throws IOException {
		checkState(!exclusiveBuffersAssigned, "Exclusive buffers should be assigned only once.");

		// This option is for OLAP, where {@link RecoveredInputChannel} will make no use, so actually don't need to assign buffers.
		if (!memorySegmentPackageEnable) {
			bufferManager.requestExclusiveBuffers();
		}
		exclusiveBuffersAssigned = true;
	}
}
