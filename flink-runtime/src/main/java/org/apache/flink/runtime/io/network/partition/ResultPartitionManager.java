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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a
 * task manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

	private final Map<ResultPartitionID, ResultPartition> registeredPartitions = new HashMap<>(16);

	private final Map<ResultPartitionID, Map<InputChannelID, PartitionRequestNotifier>> requestPartitionNotifiers = new HashMap<>(16);

	private final ScheduledExecutor requestPartitionNotifiersCheckExecutor;

	private final long notifyPartitionRequestTimeout;

	private final long notifyPartitionRequestCheckInterval;

	public ResultPartitionManager() {
		this(false, -1, -1);
	}

	public ResultPartitionManager(boolean notifyPartitionRequestEnable, long notifyPartitionRequestTimeout, long notifyPartitionRequestCheckInterval) {

		this.notifyPartitionRequestTimeout = notifyPartitionRequestTimeout;
		this.notifyPartitionRequestCheckInterval = notifyPartitionRequestCheckInterval;
		if (notifyPartitionRequestEnable) {
			this.requestPartitionNotifiersCheckExecutor = new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor(
				new ExecutorThreadFactory("check requestPartition timeout")));
			checkArgument(notifyPartitionRequestTimeout > 0, "notifyPartitionRequestTimeout must greater than zero");
			checkArgument(notifyPartitionRequestCheckInterval > 0, "notifyPartitionRequestCheckInterval must greater than zero");
			requestPartitionNotifiersCheckExecutor.schedule(this::checkRequestPartitionNotifiers, notifyPartitionRequestCheckInterval, TimeUnit.MILLISECONDS);
		} else {
			this.requestPartitionNotifiersCheckExecutor = null;
		}
	}

	private boolean isShutdown;

	@VisibleForTesting
	public Map<ResultPartitionID, Map<InputChannelID, PartitionRequestNotifier>> getRequestPartitionNotifiers() {
		return requestPartitionNotifiers;
	}

	public void registerResultPartition(ResultPartition partition) {
		synchronized (registeredPartitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");

			ResultPartition previous = registeredPartitions.put(partition.getPartitionId(), partition);

			if (previous != null) {
				throw new IllegalStateException("Result partition already registered.");
			}

			// Request subpartition view after partition setup.
			Map<InputChannelID, PartitionRequestNotifier> notifiers = requestPartitionNotifiers.remove(partition.getPartitionId());
			if (notifiers != null) {
				for (PartitionRequestNotifier notifier : notifiers.values()) {
					try {
						notifier.notifyPartitionRequest(partition);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}

			LOG.debug("Registered {}.", partition);
		}
	}

	@Override
	public ResultSubpartitionView createSubpartitionView(
			ResultPartitionID partitionId,
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener) throws IOException {

		synchronized (registeredPartitions) {
			final ResultPartition partition = registeredPartitions.get(partitionId);

			if (partition == null) {
				throw new PartitionNotFoundException(partitionId);
			}

			LOG.debug("Requesting subpartition {} of {}.", subpartitionIndex, partition);

			return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		}
	}

	@Override
	public ResultSubpartitionView createSubpartitionViewOrNotify(
			ResultPartitionID partitionId,
			int subpartitionIndex,
			BufferAvailabilityListener availabilityListener,
			PartitionRequestNotifier notifier) throws IOException {
		synchronized (registeredPartitions) {
			checkState(!isShutdown, "Result partition manager already shut down.");
			final ResultPartition partition = registeredPartitions.get(partitionId);
			if (partition == null) {
				requestPartitionNotifiers.computeIfAbsent(partitionId, key -> new HashMap<>()).put(notifier.getReceiverId(), notifier);
				return null;
			}
			return partition.createSubpartitionView(subpartitionIndex, availabilityListener);
		}
	}

	@Override
	public void cancelSubpartitionRequestNotify(NetworkSequenceViewReader networkSequenceViewReader) {
		synchronized (registeredPartitions) {
			Map<InputChannelID, PartitionRequestNotifier> inputChannelIDPartitionRequestNotifierMap = requestPartitionNotifiers.get(networkSequenceViewReader.getResultPartitionID());
			if (MapUtils.isEmpty(inputChannelIDPartitionRequestNotifierMap)) {
				return;
			}
			inputChannelIDPartitionRequestNotifierMap.remove(networkSequenceViewReader.getReceiverId());
			if (MapUtils.isEmpty(inputChannelIDPartitionRequestNotifierMap)) {
				requestPartitionNotifiers.remove(networkSequenceViewReader.getResultPartitionID());
			}
		}
	}

	private void checkRequestPartitionNotifiers() {

		List<PartitionRequestNotifier> timeoutPartitionRequestNotifiers = new LinkedList<>();
		synchronized (registeredPartitions) {
			if (isShutdown) {
				return;
			}
			long now = System.currentTimeMillis();
			Iterator<Map.Entry<ResultPartitionID, Map<InputChannelID, PartitionRequestNotifier>>> mapIterator = requestPartitionNotifiers.entrySet().iterator();
			while (mapIterator.hasNext()) {
				Map.Entry<ResultPartitionID, Map<InputChannelID, PartitionRequestNotifier>> entry = mapIterator.next();
				Map<InputChannelID, PartitionRequestNotifier> partitionRequestNotifiers = entry.getValue();
				Iterator<Map.Entry<InputChannelID, PartitionRequestNotifier>> iterator = partitionRequestNotifiers.entrySet().iterator();
				while (iterator.hasNext()) {
					PartitionRequestNotifier partitionRequestNotifier = iterator.next().getValue();
					if ((now - partitionRequestNotifier.getReceiveTimestamp()) > notifyPartitionRequestTimeout) {
						timeoutPartitionRequestNotifiers.add(partitionRequestNotifier);
						iterator.remove();
					}
				}
				if (MapUtils.isEmpty(partitionRequestNotifiers)) {
					mapIterator.remove();
				}
			}
		}
		for (PartitionRequestNotifier partitionRequestNotifier : timeoutPartitionRequestNotifiers) {
			try {
				partitionRequestNotifier.notifyPartitionRequestNotifyTimeout();
			} catch (Exception e) {
				LOG.error("NotifyPartitionRequestNotify {} send partition not found msg failed", partitionRequestNotifier, e);
			}
		}
		requestPartitionNotifiersCheckExecutor.schedule(this::checkRequestPartitionNotifiers, notifyPartitionRequestCheckInterval, TimeUnit.MILLISECONDS);
	}

	public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
		synchronized (registeredPartitions) {
			requestPartitionNotifiers.remove(partitionId);
			ResultPartition resultPartition = registeredPartitions.remove(partitionId);
			if (resultPartition != null) {
				resultPartition.release(cause);
				LOG.debug("Released partition {} produced by {}.",
					partitionId.getPartitionId(), partitionId.getProducerId());
			}
		}
	}

	public void shutdown() {
		synchronized (registeredPartitions) {

			LOG.debug("Releasing {} partitions because of shutdown.",
				registeredPartitions.values().size());

			for (ResultPartition partition : registeredPartitions.values()) {
				partition.release();
			}

			registeredPartitions.clear();

			requestPartitionNotifiers.clear();

			isShutdown = true;

			LOG.debug("Successful shutdown.");
		}
	}

	// ------------------------------------------------------------------------
	// Notifications
	// ------------------------------------------------------------------------

	void onConsumedPartition(ResultPartition partition) {
		LOG.debug("Received consume notification from {}.", partition);

		synchronized (registeredPartitions) {
			final ResultPartition previous = registeredPartitions.remove(partition.getPartitionId());
			requestPartitionNotifiers.remove(partition.getPartitionId());
			// Release the partition if it was successfully removed
			if (partition == previous) {
				partition.release();
				ResultPartitionID partitionId = partition.getPartitionId();
				LOG.debug("Released partition {} produced by {}.",
					partitionId.getPartitionId(), partitionId.getProducerId());
			}
		}
	}

	public Collection<ResultPartitionID> getUnreleasedPartitions() {
		synchronized (registeredPartitions) {
			return registeredPartitions.keySet();
		}
	}
}
