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

package org.apache.flink.rocketmq.source.reader;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQConsumerFactory;
import org.apache.flink.connector.rocketmq.RocketMQOptions;
import org.apache.flink.connector.rocketmq.RocketMQUtils;
import org.apache.flink.connector.rocketmq.TopicAndQueuesGauge;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricsConstants;
import org.apache.flink.rocketmq.source.split.RocketMQSplit;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.rocketmq.source.split.RocketMQSplitState;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.RetryManager;
import org.apache.flink.util.RetryManager.Strategy;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQConsumer.ROCKET_MQ_CONSUMER_METRICS_GROUP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONSUMER_RECORDS_METRICS_RATE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.getRocketMQProperties;

/**
 * A {@link SplitReader} implementation that reads records from RocketMQ queues.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 */
public class RocketMQSplitReader<OUT> implements SplitReader<Tuple3<OUT, Long, Long>, RocketMQSplit> {
	// jobName, cluster_topic, taskId, uuid
	public static final String INSTANCE_NAME_TEMPLATE = "FlinkRmqSplitReader:%s:%s_%s:taskId_%s:%s";

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSplitReader.class);
	private static final long CONSUMER_DEFAULT_POLL_LATENCY_MS = 10000;
	private static final int DEFAULT_SLEEP_MILLISECONDS = 1;
	private static final String CONSUMER_TOPIC_QUEUES = "sourceTopicPartitions";

	private final RocketMQDeserializationSchema<OUT> schema;
	private final RocketMQConsumerFactory consumerFactory;
	private final Map<String, String> props;
	private final String jobName;

	private final String cluster;
	private final String topic;
	private final String group;
	private final String tag;
	private final Strategy strategy;
	private final int offsetFlushInterval;
	private final int pullBatchSize;
	private final long pollLatencyMs;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient MeterView recordsNumMeterView;

	private DefaultMQPullConsumer consumer;
	private volatile boolean wakeup = false;
	private transient String consumerInstanceName;
	private transient SourceReaderContext sourceReaderContext;
	private transient Set<MessageQueue> assignedQueues;
	private transient Set<String> finishedSplit;
	private transient Counter skipDirtyCounter;
	private transient Counter emptyPollCounter;
	private transient Map<MessageQueuePb, Tuple1<Double>> fetchLatencyMap;
	private transient TopicAndQueuesGauge topicAndQueuesGauge;

	public RocketMQSplitReader(
			RocketMQDeserializationSchema<OUT> schema,
			Map<String, String> props,
			RocketMQConfig<OUT> config,
			String jobName,
			SourceReaderContext readerContext) {
		this.schema = schema;
		this.props = props;
		this.cluster = config.getCluster();
		this.topic = config.getTopic();
		this.group = config.getGroup();
		this.tag = config.getTag();
		this.offsetFlushInterval = config.getOffsetFlushInterval();
		this.strategy =
			RetryManager.createStrategy(RetryManager.StrategyType.EXPONENTIAL_BACKOFF.name(),
				RocketMQOptions.CONSUMER_RETRY_TIMES_DEFAULT,
				RocketMQOptions.CONSUMER_RETRY_INIT_TIME_MS_DEFAULT);
		this.sourceReaderContext = readerContext;
		this.consumerFactory = config.getConsumerFactory();
		this.jobName = jobName;
		this.pollLatencyMs = config.getPollLatencyMs();
		this.pullBatchSize = config.getPollBatchSize();
		this.rateLimiter = config.getRateLimiter();
		if (rateLimiter != null) {
			this.rateLimiter.openWithParallel(readerContext.getReaderParallelism());
		}

		MetricGroup metricGroup = readerContext.metricGroup().addGroup(ROCKET_MQ_CONSUMER_METRICS_GROUP)
			.addGroup(RocketMQOptions.TOPIC_METRICS_GROUP, this.topic)
			.addGroup(RocketMQOptions.CONSUMER_GROUP_METRICS_GROUP, this.group)
			.addGroup(MetricsConstants.METRICS_CONNECTOR_TYPE, RocketMQOptions.CONNECTOR_TYPE_VALUE_ROCKETMQ)
			.addGroup(MetricsConstants.METRICS_FLINK_VERSION, MetricsConstants.FLINK_VERSION_VALUE);
		recordsNumMeterView = metricGroup.meter(CONSUMER_RECORDS_METRICS_RATE, new MeterView(60));
		initialRocketMQSplitReader();
	}

	@Override
	public RecordsWithSplitIds<Tuple3<OUT, Long, Long>> fetch() throws InterruptedException {
		RocketMQSplitRecords<Tuple3<OUT, Long, Long>> recordsBySplits = new RocketMQSplitRecords<>();
		if (wakeup) {
			LOG.info("Wake up pulling message.");
			wakeup = false;
			return recordsBySplits;
		}
		// Here, we should use consumer.poll() to get the MessageExt.
		AtomicReference<List<MessageExt>> messageExtListsRef = new AtomicReference<>();
		long start = System.currentTimeMillis();
		synchronized (consumer) {
			RetryManager.retry(() ->
				messageExtListsRef.set(consumer.poll(pullBatchSize, pollLatencyMs)), strategy);
			LOG.trace("Group {} Subtask {} pull size is {}",
				group, sourceReaderContext.getSubTaskId(), messageExtListsRef.get().size());
		}
		long pollCost = System.currentTimeMillis() - start;
		if (messageExtListsRef.get().size() > 0) {
			fetchLatencyMap.get(messageExtListsRef.get().get(0).getMessageQueue()).f0 = (double) pollCost;
			recordsNumMeterView.markEvent(messageExtListsRef.get().size());
		} else {
			emptyPollCounter.inc();
		}

		// Then we should convert MessageExt to RecordsWithSplitIds.
		for (MessageExt messageExt: messageExtListsRef.get()) {
			MessageQueue messageQueue = createMessageQueueFromMessageQueuePb(messageExt.getMessageQueue());
			String splitId = RocketMQSplitBase.getSplitId(
				messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId());
			if (rateLimiter != null) {
				rateLimiter.acquire(1);
			}

			if (finishedSplit.contains(splitId)) {
				continue;
			}

			// Get records collection by splitId. Then add the messageExt result in it.
			Collection<Tuple3<OUT, Long, Long>> recordsForSplit =
					recordsBySplits.recordsForSplit(splitId);
			try {
				// Deserialize the messageExt to RowData output.
				OUT rowData = schema.deserialize(messageQueue, messageExt);
				if (schema.isEndOfQueue(messageExt, rowData)) {
					finishedSplit.add(splitId);
					recordsBySplits.addFinishedSplit(splitId);
					LOG.info("Split id {} reached end of stream", splitId);
					continue;
				}
				if (rowData == null) {
					MessageQueuePb messageQueuePb = messageExt.getMessageQueue();
					skipDirtyCounter.inc();
					LOG.warn("{} offset {} is invalid",
						RocketMQUtils.formatQueue(messageQueuePb), messageExt.getQueueOffset());
					continue;
				}
				// Tuple3<RowData, offset, timestamp>
				recordsForSplit.add(new Tuple3<>(
						rowData,
						messageExt.getQueueOffset(),
						messageExt.getBornTimestamp()));
				// need ack every msg
				synchronized (consumer) {
					RetryManager.retry(() -> consumer.ack(messageExt), strategy);
				}
			} catch (Exception e) {
				throw new FlinkRuntimeException(String.format("Failed to deserialize consumer record %s, offset %s",
					RocketMQUtils.formatQueue(messageExt.getMessageQueue()), messageExt.getQueueOffset()), e);
			}
		}
		// ACK the messageExts.
		final List<MessageExt> ackExts = messageExtListsRef.get();
		if (ackExts.size() == 0) {
			Thread.sleep(DEFAULT_SLEEP_MILLISECONDS);
		}
		return recordsBySplits;
	}

	/**
	 * All the splits changes in one reader will be handle in here, because it's a single thread fetcher.
	 * @param splitsChanges a queue with split changes that has not been handled by this SplitReader.
	 */
	@Override
	public void handleSplitsChanges(Queue<SplitsChange<RocketMQSplit>> splitsChanges) {
		// Parse the offset and reset it.
		List<MessageQueuePb> newMessageQueues = new ArrayList<>();
		while (!splitsChanges.isEmpty()) {
			SplitsChange<RocketMQSplit> splitsChange = splitsChanges.poll();
			if (!(splitsChange instanceof SplitsAddition)) {
				throw new UnsupportedOperationException(
						String.format(
								"The SplitChange type of %s is not supported.",
								splitsChange.getClass()));
			}

			splitsChange.splits().forEach(
				split -> {
					try {
						parseStartingOffsets(split);
						// Get MessageQueuePb from split, then add it to newMessageQueues list.
						newMessageQueues.add(createMessageQueuePbFromSplit(split));
					} catch (Exception e) {
						throw new FlinkRuntimeException("SplitReader parses split error: ", e);
					}
				});
		}

		if (!newMessageQueues.isEmpty()) {
			synchronized (consumer) {
				if (assignedQueues != null) {
					// Add old assigned queues.
					newMessageQueues.addAll(assignedQueues.stream().map(
						queue -> createMessageQueuePb(queue.getTopic(), queue.getBrokerName(), queue.getQueueId())
					).collect(Collectors.toList()));
				}
				consumer.assign(newMessageQueues);
				if (finishedSplit == null) {
					finishedSplit = new HashSet<>();
				}
				assignedQueues = newMessageQueues.stream()
					.map(queue -> new MessageQueue(queue.getTopic(), queue.getBrokerName(), queue.getQueueId()))
					.collect(Collectors.toSet());
			}

			initMetrics(newMessageQueues);
			newMessageQueues.forEach(queue -> topicAndQueuesGauge.addTopicAndQueue(
				queue.getTopic(),
				String.format("%s:%d", queue.getBrokerName(), queue.getQueueId())));
		}
	}

	@Override
	public void wakeUp() {
		LOG.debug("Wake up the split reader in case the fetcher thread is blocking in fetch().");
		wakeup = true;
	}

	// --------------- private helper method ----------------------

	private MessageQueue createMessageQueueFromMessageQueuePb(MessageQueuePb queuePb) {
		return new MessageQueue(queuePb.getTopic(), queuePb.getBrokerName(), queuePb.getQueueId());
	}

	private MessageQueuePb createMessageQueuePbFromSplit(RocketMQSplit split) {
		return createMessageQueuePb(split.getTopic(), split.getBrokerName(), split.getQueueId());
	}

	private MessageQueuePb createMessageQueuePb(String topic, String broker, int queueId) {
		return MessageQueuePb.newBuilder()
					.setTopic(topic)
					.setBrokerName(broker)
					.setQueueId(queueId)
					.build();
	}

	/**
	 * Parse starting offset from split information. Reset the offset.
	 * @param split
	 */
	private void parseStartingOffsets(RocketMQSplit split) throws InterruptedException {
		RocketMQSplitState splitState = (RocketMQSplitState) split;
		MessageQueuePb messageQueuePb = createMessageQueuePbFromSplit(split);
		Long offset = splitState.getCurrentOffset();
		if (offset == null) {
			offset = RocketMQUtils.resetAndGetOffset(topic, group, messageQueuePb, props, consumer);
		} else {
			synchronized (consumer) {
				consumer.resetOffsetToSpecified(topic, group,
					Collections.singletonList(messageQueuePb), offset, false);
			}
		}
		splitState.setCurrentOffset(offset);
	}

	public void close() {
		if (consumer != null) {
			try {
				consumer.commitSync();
			} catch (Exception e) {
				LOG.warn("Receive interrupted exception.");
			} finally {
				consumer.shutdown();
				consumer = null;
			}
		}
	}

	private void initMetrics(List<MessageQueuePb> messageQueuePbs) {
		if (skipDirtyCounter == null) {
			skipDirtyCounter = sourceReaderContext.metricGroup()
				.addGroup("task", String.valueOf(sourceReaderContext.getSubTaskId()))
				.counter(FactoryUtil.SOURCE_SKIP_DIRTY);
		}

		if (emptyPollCounter == null) {
			emptyPollCounter = sourceReaderContext.metricGroup()
				.addGroup("task", String.valueOf(sourceReaderContext.getSubTaskId()))
				.counter("rmqEmptyPoll");
		}

		if (fetchLatencyMap == null) {
			fetchLatencyMap = new HashMap<>();
		}
		messageQueuePbs.forEach(queue -> {
			fetchLatencyMap.computeIfAbsent(queue, key -> {
				final Tuple1<Double> value = new Tuple1<>();
				value.f0 = -1.0;

				sourceReaderContext.metricGroup()
					.addGroup("broker", queue.getBrokerName())
					.addGroup("queue", String.valueOf(queue.getQueueId()))
					.addGroup("topic", queue.getTopic())
					.addGroup("task", String.valueOf(sourceReaderContext.getSubTaskId()))
					.gauge("rmqConsumerLatency", (Gauge<Double>) () -> value.f0);
				return value;
			});
		});

		this.topicAndQueuesGauge = sourceReaderContext.metricGroup()
			.addGroup("task", String.valueOf(sourceReaderContext.getSubTaskId()))
			.gauge(CONSUMER_TOPIC_QUEUES, new TopicAndQueuesGauge(cluster, group));
	}

	private void initialRocketMQSplitReader() {
		try {
			consumerInstanceName = String.format(INSTANCE_NAME_TEMPLATE, jobName,
				cluster, topic, sourceReaderContext.getSubTaskId(), UUID.randomUUID());
			this.consumer = consumerFactory.createRocketMqConsumer(
				cluster, topic, group, consumerInstanceName, getRocketMQProperties(props));
			this.consumer.setInstanceName(consumerInstanceName);
			if (tag != null) {
				consumer.setSubExpr(tag);
			}
			// offset sync to server interval
			consumer.setFlushOffsetInterval(offsetFlushInterval);
			consumer.start();
			schema.open(() -> sourceReaderContext.metricGroup());
		} catch (Exception e) {
			LOG.error("Failed to initial RocketMQ consumer. ", e);
			consumer.shutdown();
			throw new FlinkRuntimeException("Init split reader failed", e);
		}
	}

	// --------------- private helper class ----------------------

	private static class RocketMQSplitRecords<OUT> implements RecordsWithSplitIds<OUT> {
		private final Map<String, Collection<OUT>> recordsBySplits;
		private final Set<String> finishedSplits;

		private RocketMQSplitRecords() {
			this.recordsBySplits = new HashMap<>();
			this.finishedSplits = new HashSet<>();
		}

		private Collection<OUT> recordsForSplit(String splitId) {
			return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
		}

		private void addFinishedSplit(String splitId) {
			finishedSplits.add(splitId);
		}

		@Override
		public Collection<String> splitIds() {
			return recordsBySplits.keySet();
		}

		@Override
		public Map<String, Collection<OUT>> recordsBySplits() {
			return recordsBySplits;
		}

		@Override
		public Set<String> finishedSplits() {
			return finishedSplits;
		}
	}
}
