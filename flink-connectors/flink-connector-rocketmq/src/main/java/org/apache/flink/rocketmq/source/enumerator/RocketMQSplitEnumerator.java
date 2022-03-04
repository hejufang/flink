/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.rocketmq.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQConsumerFactory;
import org.apache.flink.connector.rocketmq.RocketMQUtils;
import org.apache.flink.rocketmq.source.split.RocketMQSplit;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.consumer.QueryTopicQueuesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.getRocketMQProperties;

/**
 * The RocketMQ source enumerator.
 * */
public class RocketMQSplitEnumerator implements SplitEnumerator<RocketMQSplit, RocketMQEnumState> {
	// jobName, cluster_topic, consumer_group, uuid
	public static final String ENUMERATOR_INSTANCE_NAME_TEMPLATE = "FlinkRmqEnumerator:%s:%s_%s_%s:%s";

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSplitEnumerator.class);

	private final SplitEnumeratorContext<RocketMQSplit> context;

	private final Map<String, String> props;

	private final String cluster;
	private final String topic;
	private final String group;
	private final String tag;
	private final RocketMQConsumerFactory consumerFactory;
	private final String jobName;
	private final long discoveryIntervalMs;
	private final boolean batchMode;

	/**
	 * Set of assigned MessageQueue. We should convert MessageQueuePb to MessageQueue here.
	 */
	private final Set<RocketMQSplitBase> assignedSplitBase;
	private final Set<RocketMQSplitBase> userSpecificSplitSet;

	// key: subtaskId(ReaderId), value: the RocketMQSplit that should be assigned to Reader.
	private final Map<Integer, Set<RocketMQSplit>> pendingRocketMQSplitAssignment;

	// RocketMQ consumer: use for MessageQueue discovery.
	private transient DefaultMQPullConsumer consumer;
	private transient EnumSplitIdManager splitIdManager;
	private transient String instanceName;
	// it is different from context.registeredReaders(), readers in context will recover from checkpoint
	private transient Set<Integer> currentRegisteredTasks;

	public RocketMQSplitEnumerator(
			SplitEnumeratorContext<RocketMQSplit> context,
			Map<String, String> props,
			RocketMQConfig<?> config,
			String jobName,
			Boundedness boundedness) {
		this.context = context;
		this.props = props;
		this.cluster = config.getCluster();
		this.topic = config.getTopic();
		this.group = config.getGroup();
		this.tag = config.getTag();
		this.consumerFactory = config.getConsumerFactory();
		this.discoveryIntervalMs = config.getDiscoverIntervalMs();
		this.jobName = jobName;
		this.pendingRocketMQSplitAssignment = new HashMap<>();
		this.assignedSplitBase = new HashSet<>();
		this.batchMode = boundedness == Boundedness.BOUNDED;
		userSpecificSplitSet = RocketMQUtils.getClusterSplitBaseSet(
			config.getCluster(), config.getRocketMqBrokerQueueList());
	}

	@Override
	public void start() {
		this.splitIdManager = new EnumSplitIdManager();
		// Init the consumer.
		instanceName = String.format(ENUMERATOR_INSTANCE_NAME_TEMPLATE,
			jobName, cluster, topic, group, UUID.randomUUID());
		this.consumer = consumerFactory.createRocketMqConsumer(
			cluster, topic, group, instanceName, getRocketMQProperties(props));
		this.currentRegisteredTasks = new HashSet<>();
		if (tag != null) {
			consumer.setSubExpr(tag);
		}
		consumer.start();
		if (discoveryIntervalMs > 0) {
			context.callAsync(
				this::getSubscribedMessageQueue,
				this::handleQueueChanges,
				0,
				discoveryIntervalMs);
		} else {
			context.callAsync(this::getSubscribedMessageQueue, this::handleQueueChanges);
		}
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		// The RocketMQ source pushes splits eagerly, rather than act upon split request.
	}

	@Override
	public synchronized void addSplitsBack(List<RocketMQSplit> splits, int subtaskId) {
		// Add these splits back to pending assign.
		// These splits are always belong to this subtask.
		for (RocketMQSplit split : splits) {
			pendingRocketMQSplitAssignment
					.computeIfAbsent(subtaskId, r -> new HashSet<>())
					.add(split);
		}
		// If the failed subtask has already restarted, we need to assign these splits to it.
		if (context.registeredReaders().containsKey(subtaskId)) {
			assignPendingRocketMQSplits(Collections.singleton(subtaskId));
		}
		LOG.info("Subtask {} add {} splits back", subtaskId, splits.size());
	}

	@Override
	public void addReader(int subtaskId) {
		LOG.info("Add reader :{} to RocketMQSplitEnumerator {}.", subtaskId, instanceName);
		currentRegisteredTasks.add(subtaskId);
		// Restart the reader and assign the splits to it.
		assignPendingRocketMQSplits(Collections.singleton(subtaskId));
	}

	@Override
	public RocketMQEnumState snapshotState() throws Exception {
		// We do not need any state to store in enumerator.
		return new RocketMQEnumState();
	}

	@Override
	public void close() throws IOException {
		if (consumer != null) {
			consumer.shutdown();
			consumer = null;
		}
	}

	// ----------------- private methods -------------------


	/**
	 * List subscribed topic MessageQueuePbs on RocketMQ brokers.
	 * This Method should only be invoked in the worker executor thread, because it requires network
	 * I/O with RocketMQ brokers.
	 * @return Set of subscribed {@link MessageQueuePb}
	 */
	private List<MessageQueuePb> getSubscribedMessageQueue() throws InterruptedException {
		QueryTopicQueuesResult queryTopicQueuesResult = consumer.queryTopicQueues(topic);
		RocketMQUtils.validateResponse(queryTopicQueuesResult.getErrorCode(), queryTopicQueuesResult.getErrorMsg());
		LOG.info("Topic {} get queue size: {}", topic, queryTopicQueuesResult.getMessageQueues().size());
		return queryTopicQueuesResult.getMessageQueues();
	}

	/**
	 * Check if there is any MessageQueue changes within subscribed topic MQ fetched by worker thread.
	 * Include new MessageQueue and removed MessageQueue.
	 * @param fetchedMessageQueue
	 * @param t
	 */
	private void handleQueueChanges(List<MessageQueuePb> fetchedMessageQueue, Throwable t) {
		if (t != null) {
			throw new FlinkRuntimeException(
					"Failed to list subscribed topic messageQueue due to: ", t);
		}
		final RocketmqSplitBaseChange messageQueueChange = getMessageQueueChange(fetchedMessageQueue);
		// We only handle queue added set
		if (messageQueueChange.getAddedSplitBaseSet().isEmpty()) {
			LOG.info("Not find any message queue change");
			return;
		}

		// handle these changes.
		handleRocketMQSplitChanges(messageQueueChange.addedSplitBaseSet);
	}

	private void handleRocketMQSplitChanges(Set<RocketMQSplitBase> rocketMQSplitChangeSet) {
		if (discoveryIntervalMs < 0) {
			LOG.info("MessageQueue discovery is disabled.");
		}

		addSplitChangeToPendingAssignment(rocketMQSplitChangeSet);
		LOG.info("Start assign task to readers {}", currentRegisteredTasks);
		assignPendingRocketMQSplits(currentRegisteredTasks);
	}

	/**
	 * Check and compute the changes, add them to pendingAssignment.
	 * @param newRocketMQSplits
	 */
	private synchronized void addSplitChangeToPendingAssignment(Set<RocketMQSplitBase> newRocketMQSplits) {
		int numReader = context.currentParallelism();
		splitIdManager.addAll(newRocketMQSplits);
		newRocketMQSplits.stream().sorted().forEach(
			splitBase -> {
				int subtaskId = splitIdManager.getSplitId(splitBase) % numReader;
				pendingRocketMQSplitAssignment
					.computeIfAbsent(subtaskId, r -> new HashSet<>())
					.add(new RocketMQSplit(
							splitBase,
							RocketMQSplit.INIT_STARTING_OFFSET,
							RocketMQSplit.NO_STOPPING_OFFSET));
				LOG.info("Rocketmq split [topic: {}, broker: {}, queue: {} assigned to {}]",
					splitBase.getTopic(), splitBase.getBrokerName(), splitBase.getQueueId(), subtaskId);
			}
		);
		LOG.info("Assigned {} to {} readers.", newRocketMQSplits, numReader);
	}

	private RocketmqSplitBaseChange getMessageQueueChange(List<MessageQueuePb> fetchedMessageQueue) {
		// Convert MessageQueuePbs to MessageQueues.
		Stream<RocketMQSplitBase> splitBaseStream = fetchedMessageQueue.stream()
			.map(pb -> new RocketMQSplitBase(pb.getTopic(), pb.getBrokerName(), pb.getQueueId()));
		if (!userSpecificSplitSet.isEmpty()) {
			splitBaseStream = splitBaseStream.filter(userSpecificSplitSet::contains);
		}
		Set<RocketMQSplitBase> currentSplitBaseSet = splitBaseStream.collect(Collectors.toSet());
		LOG.info("Before split filter size {}, after size {}",
			fetchedMessageQueue.size(), currentSplitBaseSet.size());

		final Set<RocketMQSplitBase> removedSplitBaseSet = new HashSet<>();
		final Set<RocketMQSplitBase> addedSplitBaseSet = new HashSet<>();
		// Remove the same queues between fetchedMessageQueue and assignedMessageQueue.
		// The same queues will be moved to removedMessageQueue.
		Consumer<RocketMQSplitBase> dedupOrMarkAsRemoved =
			(splitBase) -> {
				if (!currentSplitBaseSet.remove(splitBase)) {
					removedSplitBaseSet.add(splitBase);
				}
			};
		assignedSplitBase.forEach(dedupOrMarkAsRemoved);
		addedSplitBaseSet.addAll(currentSplitBaseSet);
		// now, fetchedMessageQueue only contains new messageQueues.
		if (!addedSplitBaseSet.isEmpty()) {
			LOG.info("Discovered new MessageQueueList: {}", addedSplitBaseSet);
		}
		if (!removedSplitBaseSet.isEmpty()) {
			LOG.info("Discovered removed MessageQueueList: {}", removedSplitBaseSet);
		}
		return new RocketmqSplitBaseChange(addedSplitBaseSet, removedSplitBaseSet);
	}

	/**
	 * Get pending RocketMQ splits and send assignSplits to the readers.
	 * @param pendingReaders
	 */
	private synchronized void assignPendingRocketMQSplits(Set<Integer> pendingReaders) {
		Map<Integer, List<RocketMQSplit>> incrementalAssignment = new HashMap<>();
		for (int pendingReader : pendingReaders) {
			checkReaderRegistered(pendingReader);
			final Set<RocketMQSplit> pendingAssignmentForReader =
					pendingRocketMQSplitAssignment.remove(pendingReader);
			if (pendingAssignmentForReader == null) {
				LOG.info("Readers {} still waiting for rocketMq split assignment.", pendingReader);
				continue;
			}
			if (!pendingAssignmentForReader.isEmpty()) {
				// put pending assignment to incremental.
				incrementalAssignment
						.computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
						.addAll(pendingAssignmentForReader);
			}
			// Add Splits' MessageQueue to assignedMessageQueue.
			pendingAssignmentForReader.forEach(
					split -> assignedSplitBase.add(split.getRocketMQBaseSplit()));
		}
		// assign pending splits to the readers.
		if (!incrementalAssignment.isEmpty()) {
			LOG.info("Assigning splits to readers: {},", incrementalAssignment);
			context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
			LOG.info("Finish send assign event.");
		}

		if (batchMode) {
			pendingReaders.forEach(
				reader -> context.sendEventToSourceReader(reader, new NoMoreSplitsEvent())
			);
		}
	}

	private void checkReaderRegistered(int readerId) {
		if (!context.registeredReaders().containsKey(readerId)) {
			throw new IllegalStateException(
				String.format("Reader %d is not registered to source coordinator", readerId));
		}
	}

	// --------------- private class ---------------
	private static class RocketmqSplitBaseChange {
		private final Set<RocketMQSplitBase> addedSplitBaseSet;
		private final Set<RocketMQSplitBase> removedSplitBaseSet;

		RocketmqSplitBaseChange(
				Set<RocketMQSplitBase> newMessageQueue,
				Set<RocketMQSplitBase> removedMessageQueue) {
			this.addedSplitBaseSet = newMessageQueue;
			this.removedSplitBaseSet = removedMessageQueue;
		}

		public Set<RocketMQSplitBase> getAddedSplitBaseSet() {
			return addedSplitBaseSet;
		}

		public Set<RocketMQSplitBase> getRemovedSplitBaseSet() {
			return removedSplitBaseSet;
		}

		public boolean isEmpty() {
			return addedSplitBaseSet.isEmpty() && removedSplitBaseSet.isEmpty();
		}
	}
}
