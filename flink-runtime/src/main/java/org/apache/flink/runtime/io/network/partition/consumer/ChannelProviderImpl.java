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

import org.apache.flink.metrics.Counter;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

	private final Counter numChannelsCached;
	private final Counter numChannelsInjectedError;
	private final AtomicLong injectErrorCount;

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
		this.injectErrorCount = new AtomicLong(0);
		this.numChannelsInjectedError = metrics.getNumChannelsInjectedError();
		this.numChannelsCached = metrics.getNumChannelsCached();
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
				metrics.getNumBuffersInDropped(),
				maxDelayTimeMs,
				executor,
				isRecoverable,
				current.memorySegmentPackageEnable);
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
				isRecoverable,
				current.memorySegmentPackageEnable);
	}

	@Override
	public PartitionInfo getPartitionInfoAndRemove(int channelIndex) {
		return cachedPartitionInfos.remove(channelIndex);
	}

	@Override
	public void cachePartitionInfo(InputChannel inputChannel, ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor) {
		final int channelIndex = inputChannel.channelIndex;
		if (cachedPartitionInfos.containsKey(channelIndex)) {
			LOG.warn("ChannelProvider has already cached this partitionInfo.(index={}, timestamp={})", channelIndex, cachedPartitionInfos.get(channelIndex).timestamp);
		}

		final PartitionInfo partitionInfo = new PartitionInfo(localLocation, shuffleDescriptor);
		cachedPartitionInfos.put(channelIndex, partitionInfo);

		// the cached partitionInfo should be used very soon except for cases that yarn container becomes wild
		executor.schedule(new DisableChannelRunnable(inputChannel, partitionInfo), 3 * maxDelayTimeMs, TimeUnit.MILLISECONDS);
		this.numChannelsCached.inc();
		this.numChannelsInjectedError.inc(injectErrorCount.get() - this.numChannelsInjectedError.getCount());
	}

	private class DisableChannelRunnable implements Runnable {

		private final int channelIndex;
		private final PartitionInfo partitionInfo;
		private final InputChannel inputChannel;

		DisableChannelRunnable(InputChannel inputChannel, PartitionInfo partitionInfo) {
			this.inputChannel = inputChannel;
			this.channelIndex = inputChannel.channelIndex;
			this.partitionInfo = partitionInfo;
		}

		@Override
		public void run() {
			if (cachedPartitionInfos.containsKey(channelIndex)
				&& cachedPartitionInfos.get(channelIndex).equals(partitionInfo)
				&& inputChannel.isChannelAvailable()) {
				// usually this means the channel cannot sense the upstream failure
				LOG.info("The channel {} is still available, inject error.", inputChannel);
				if (inputChannel instanceof RemoteInputChannel) {
					RemoteInputChannel remoteInputChannel = (RemoteInputChannel) inputChannel;
					remoteInputChannel.onError(new IllegalStateException("There may be a wild yarn container, fail this channel."));
					injectErrorCount.incrementAndGet();
				}
			}
		}
	}
}
