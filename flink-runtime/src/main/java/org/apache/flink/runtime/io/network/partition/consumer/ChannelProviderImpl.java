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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is used to provide channel transformation.
 */
public class ChannelProviderImpl implements ChannelProvider {
	private static final Logger LOG = LoggerFactory.getLogger(ChannelProviderImpl.class);

	private final ConnectionManager connectionManager;

	private final InputChannelMetrics metrics;

	private final ResultPartitionManager resultPartitionManager;

	private final TaskEventPublisher taskEventPublisher;

	private final boolean isRecoverable;

	private final ScheduledExecutorService executor;

	private final Map<Integer, PartitionInfo> cachedPartitionInfos;

	private long maxDelayTimeMs;

	public ChannelProviderImpl(
			ConnectionManager connectionManager,
			InputChannelMetrics metrics,
			ResultPartitionManager partitionManager,
			TaskEventPublisher taskEventPublisher,
			long maxDelayTimeMs,
			ScheduledExecutorService executor,
			boolean isRecoverable) {
		this.connectionManager = connectionManager;
		this.metrics = metrics;
		this.resultPartitionManager = partitionManager;
		this.taskEventPublisher = taskEventPublisher;
		this.isRecoverable = isRecoverable;
		this.executor = executor;
		this.maxDelayTimeMs = maxDelayTimeMs;

		this.cachedPartitionInfos = new HashMap<>();
	}

	@Override
	public RemoteInputChannel transformToRemoteInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ConnectionID newConnectionID,
			ResultPartitionID newPartitionID) throws IOException {
		current.releaseAllResources();

		return new RemoteInputChannel(inputGate,
				current.channelIndex,
				newPartitionID,
				newConnectionID,
				connectionManager,
				current.getInitialBackoff(),
				current.getMaxBackoff(),
				metrics.getNumBytesInRemoteCounter(),
				metrics.getNumBuffersInRemoteCounter(),
				maxDelayTimeMs,
				executor,
				isRecoverable);
	}

	@Override
	public LocalInputChannel transformToLocalInputChannel(
			SingleInputGate inputGate,
			InputChannel current,
			ResultPartitionID newPartitionID) throws IOException {
		current.releaseAllResources();

		return new LocalInputChannel(inputGate,
				current.channelIndex,
				newPartitionID,
				resultPartitionManager,
				taskEventPublisher,
				current.getInitialBackoff(),
				current.getMaxBackoff(),
				metrics.getNumBytesInLocalCounter(),
				metrics.getNumBuffersInRemoteCounter(),
				maxDelayTimeMs,
				executor,
				isRecoverable);
	}

	@Override
	public PartitionInfo getPartitionInfoAndRemove(int channelIndex) {
		return cachedPartitionInfos.remove(channelIndex);
	}

	@Override
	public void cachePartitionInfo(int channelIndex, ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor) {
		if (cachedPartitionInfos.containsKey(channelIndex)) {
			LOG.warn("ChannelProvider has already cached this partitionInfo.(index={}, timestamp={})", channelIndex, cachedPartitionInfos.get(channelIndex).timestamp);
		}

		cachedPartitionInfos.put(channelIndex, new PartitionInfo(localLocation, shuffleDescriptor));
	}
}
