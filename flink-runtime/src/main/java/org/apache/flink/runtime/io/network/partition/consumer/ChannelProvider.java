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

import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;

import java.io.IOException;

/**
 * This is used to provide channel transformation.
 */
public class ChannelProvider {

	private final ConnectionManager connectionManager;

	private final InputChannelMetrics metrics;

	private final MemorySegmentProvider memorySegmentProvider;

	private final ResultPartitionManager resultPartitionManager;

	private final TaskEventPublisher taskEventPublisher;

	private final boolean isRecoverable;

	public ChannelProvider(ConnectionManager connectionManager, InputChannelMetrics metrics, NetworkBufferPool networkBufferPool,
			ResultPartitionManager partitionManager, TaskEventPublisher taskEventPublisher, boolean isRecoverable) {
		this.connectionManager = connectionManager;
		this.metrics = metrics;
		this.memorySegmentProvider = networkBufferPool;
		this.resultPartitionManager = partitionManager;
		this.taskEventPublisher = taskEventPublisher;
		this.isRecoverable = isRecoverable;
	}

	public RemoteInputChannel transformToRemoteInputChannel(SingleInputGate inputGate, InputChannel current,
			ConnectionID connectionID) throws IOException {
		current.releaseAllResources();

		return new RemoteInputChannel(inputGate, current.channelIndex, current.partitionId, connectionID,
				connectionManager, current.getInitialBackoff(), current.getMaxBackoff(), metrics, memorySegmentProvider, isRecoverable);
	}

	public LocalInputChannel transformToLocalInputChannel(SingleInputGate inputGate, InputChannel current) throws IOException {
		current.releaseAllResources();

		return new LocalInputChannel(inputGate, current.channelIndex, current.partitionId, resultPartitionManager,
				taskEventPublisher, current.getInitialBackoff(), current.getMaxBackoff(), metrics, isRecoverable);
	}
}
