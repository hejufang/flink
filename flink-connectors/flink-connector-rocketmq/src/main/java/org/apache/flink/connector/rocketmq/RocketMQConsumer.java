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

package org.apache.flink.connector.rocketmq;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricsConstants;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.RetryManager;
import org.apache.flink.util.function.SupplierWithException;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.mqproxy.proto.ResponseCode;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.consumer.QueryTopicQueuesResult;
import com.bytedance.rocketmq.clientv2.consumer.ResetOffsetResult;
import com.bytedance.rocketmq.clientv2.exception.RMQClientException;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONSUMER_RECORDS_METRICS_RATE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.OFFSETS_STATE_NAME;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.getRocketMQProperties;

/**
 * RocketMQConsumer.
 */
public class RocketMQConsumer<T> extends RichParallelSourceFunction<T> implements
		CheckpointListener,
		ResultTypeQueryable<T>,
		CheckpointedFunction,
		SpecificParallelism {
	private static final long serialVersionUID = 1L;
	private static final int DEFAULT_SLEEP_MILLISECONDS = 1;
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQConsumer.class);
	private static final String CONSUMER_TOPIC_QUEUES = "sourceTopicPartitions";
	private static final String INSTANCE_ID_TEMPLATE = "flink_%s_rmq_%s_%s_%s";
	public static final String ROCKET_MQ_CONSUMER_METRICS_GROUP = "RocketMQConsumer";

	private final String cluster;
	private final String topic;
	private final String group;
	private final String tag;
	private final Map<String, String> props;
	private final RocketMQDeserializationSchema<T> schema;
	private final String brokerQueueList;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final long sourceIdleTimeMs;
	private final String jobName;
	private final RocketMQConsumerFactory consumerFactory;
	private int parallelism;
	private final int offsetFlushInterval;
	private final long discoveryIntervalMs;
	private final int pollBatchSize;
	private final long pollLatencyMs;

	private transient MeterView recordsNumMeterView;
	private transient TopicAndQueuesGauge topicAndQueuesGauge;
	private transient DefaultMQPullConsumer consumer;
	private transient volatile List<MessageQueuePb> assignedMessageQueuePbs;
	private transient volatile Set<MessageQueue> assignedMessageQueueSet;
	private transient Set<MessageQueue> lastSnapshotQueues;
	private transient Thread updateThread;
	private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
	private transient volatile boolean running;
	private transient Throwable backgroundException;
	private transient boolean isRestored;
	private transient boolean hasRun;
	private transient Map<MessageQueue, Long> offsetTable;
	private transient Map<MessageQueue, Long> restoredOffsets;
	private transient int subTaskId;
	private transient Counter skipDirtyCounter;
	private transient int runtimeParallelism;
	private transient RetryManager.Strategy retryStrategy;
	private transient List<MessageQueuePb> userSpecificQueuePbs;

	public RocketMQConsumer(
			RocketMQDeserializationSchema<T> schema,
			Map<String, String> props,
			RocketMQConfig<T> config) {
		this.schema = schema;
		this.props = props;
		this.cluster = config.getCluster();
		this.group = config.getGroup();
		this.topic = config.getTopic();
		this.tag = config.getTag();
		this.parallelism = config.getParallelism();
		this.rateLimiter = config.getRateLimiter();
		this.sourceIdleTimeMs = config.getIdleTimeOut();
		this.consumerFactory = config.getConsumerFactory();
		this.jobName = System.getProperty(ConfigConstants.JOB_NAME_KEY, ConfigConstants.JOB_NAME_DEFAULT);
		this.offsetFlushInterval = config.getOffsetFlushInterval();
		this.discoveryIntervalMs = config.getDiscoverIntervalMs();
		this.pollBatchSize = config.getPollBatchSize();
		this.pollLatencyMs = config.getPollLatencyMs();

		String dc = System.getProperty(ConfigConstants.DC_KEY, "cn").toUpperCase();
		String user = System.getProperty(ConfigConstants.OWNER_KEY, "unknown");
		int restAPIRetryTimes = config.getRestAPIRetryTimes();
		int restAPIRetryInitTimeMs = config.getRestAPIRetryInitTimeMs();
		try (RocketMQRestClient client = new RocketMQRestClient(dc, user, restAPIRetryTimes, restAPIRetryInitTimeMs)) {
			client.registerToToolbox(cluster, topic, group);
		}
		RocketMQUtils.saveConfigurationToSystemProperties(config);
		RocketMQUtils.validateAndSetBrokerQueueList(config);
		this.brokerQueueList = config.getRocketMqBrokerQueueList();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		Properties properties = getRocketMQProperties(props);
		String instanceName = String.format(INSTANCE_ID_TEMPLATE, jobName, topic, subTaskId, UUID.randomUUID());
		LOG.info("Current rocketmq instance name is {}", instanceName);
		this.consumer = consumerFactory.createRocketMqConsumer(cluster, topic, group, instanceName, properties);

		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(ROCKET_MQ_CONSUMER_METRICS_GROUP)
			.addGroup(RocketMQOptions.TOPIC_METRICS_GROUP, this.topic)
			.addGroup(RocketMQOptions.CONSUMER_GROUP_METRICS_GROUP, this.group)
			.addGroup(MetricsConstants.METRICS_CONNECTOR_TYPE, RocketMQOptions.CONNECTOR_TYPE_VALUE_ROCKETMQ)
			.addGroup(MetricsConstants.METRICS_FLINK_VERSION, MetricsConstants.FLINK_VERSION_VALUE);

		this.recordsNumMeterView = metricGroup.meter(CONSUMER_RECORDS_METRICS_RATE, new MeterView(60));
		this.topicAndQueuesGauge = metricGroup.gauge(CONSUMER_TOPIC_QUEUES, new TopicAndQueuesGauge(cluster, group));
		schema.open(() -> getRuntimeContext().getMetricGroup());
		this.skipDirtyCounter = getRuntimeContext().getMetricGroup().counter(FactoryUtil.SOURCE_SKIP_DIRTY);
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
		this.retryStrategy = RetryManager.createStrategy(RetryManager.StrategyType.EXPONENTIAL_BACKOFF.name(),
			RocketMQOptions.CONSUMER_RETRY_TIMES_DEFAULT,
			RocketMQOptions.CONSUMER_RETRY_INIT_TIME_MS_DEFAULT);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return schema.getProducedType();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		if (hasRun && !running) {
			LOG.info("snapshotState() called on closed source");
		} else {
			unionOffsetStates.clear();
			Set<MessageQueue> snapshotSets = new HashSet<>(lastSnapshotQueues);
			snapshotSets.addAll(offsetTable.keySet());
			for (MessageQueue messageQueue: snapshotSets) {
				Long offset = offsetTable.get(messageQueue);
				if (offset == null) {
					// If it not exists in current offset table, get from last snapshot
					offset = restoredOffsets.get(messageQueue);
				}
				if (offset != null) {
					LOG.info("Queue {} store offset {} to state", messageQueue.toString(), offset);
					unionOffsetStates.add(new Tuple2<>(messageQueue, offset));
				} else {
					LOG.warn("{} offset is null.", messageQueue.toString());
				}
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		this.runtimeParallelism = getRuntimeContext().getNumberOfParallelSubtasks();

		this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
			OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {})
		));

		List<Tuple2<MessageQueue, Long>> tuple2List =
			RocketMQUtils.getDtsState(context.getOperatorStateStore());
		lastSnapshotQueues = new HashSet<>();
		offsetTable = new ConcurrentHashMap<>();
		restoredOffsets = new HashMap<>();
		isRestored = context.isRestored();
		unionOffsetStates.get().forEach(tuple2List::add);
		List<MessageQueue> thisTaskQueues = parseMessageQueueSet();
		userSpecificQueuePbs = thisTaskQueues.stream().map(this::queue2queuePb).collect(Collectors.toList());
		Set<MessageQueue> thisTaskQueueSet = new HashSet<>(thisTaskQueues);
		tuple2List.forEach(
			queueAndOffset -> restoredOffsets.compute(queueAndOffset.f0, (queue, offset) -> {
				if (offset != null) {
					return Math.max(queueAndOffset.f1, offset);
				}
				if (hashBelongToThisTask(queueAndOffset.f0) || thisTaskQueueSet.contains(queueAndOffset.f0)) {
					lastSnapshotQueues.add(queueAndOffset.f0);
				}
				return queueAndOffset.f1;
			})
		);
		LOG.info("Recovered lastSnapshotQueues {}", lastSnapshotQueues);
		LOG.info("Recovered restoredOffsets {}", restoredOffsets.entrySet());
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		running = true;
		hasRun = true;
		if (tag != null) {
			synchronized (RocketMQConsumer.class) {
				if (consumer != null) {
					consumer.setSubExpr(tag);
				}
			}
		}
		synchronized (RocketMQConsumer.class) {
			if (consumer != null) {
				// offset sync to server interval
				consumer.setFlushOffsetInterval(offsetFlushInterval);
				RetryManager.retry(() -> consumer.start(), retryStrategy);
			}
		}

		if (userSpecificQueuePbs.isEmpty()) {
			assignMessageQueues(this::allocFixedMessageQueue);
			updateThread = createUpdateThread();
			updateThread.start();
		} else {
			assignMessageQueues(() -> userSpecificQueuePbs);
			LOG.info("[Cluster: {}, topic: {}, group:{}, taskId: {}] user specific queue {}",
				cluster, topic, group, subTaskId,
				userSpecificQueuePbs.stream().map(this::formatQueue).collect(Collectors.joining(",")));
		}

		long lastTimestamp = System.currentTimeMillis();
		while (running) {
			if (userSpecificQueuePbs.isEmpty() && !updateThread.isAlive()) {
				if (!running && (backgroundException == null || backgroundException instanceof InterruptedException)) {
					LOG.info("Not in running state, we will exit task {}", subTaskId);
					break;
				}
				throw new FlinkRuntimeException(
					"Subtask " + subTaskId + " RocketMQ partition discovery thread is not alive", backgroundException);
			}

			List<List<MessageExt>> messageExtsList = new ArrayList<>();
			synchronized (RocketMQConsumer.this) {
				if (assignedMessageQueuePbs == null || assignedMessageQueuePbs.size() == 0) {
					ctx.markAsTemporarilyIdle();
					this.wait();
				}
				if (consumer != null) {
					RetryManager.retry(() -> messageExtsList.add(consumer.poll(pollBatchSize, pollLatencyMs)), retryStrategy);
				}
			}
			List<MessageExt> messageExts = messageExtsList.isEmpty() ? Collections.emptyList() : messageExtsList.get(0);
			for (MessageExt messageExt: messageExts) {
				if (rateLimiter != null) {
					rateLimiter.acquire(1);
				}
				MessageQueue messageQueue = createMessageQueue(messageExt.getMessageQueue());
				offsetTable.put(messageQueue, messageExt.getQueueOffset() + 1);
				T rowData = schema.deserialize(messageQueue, messageExt);
				if (rowData == null) {
					skipDirtyCounter.inc();
					LOG.warn("Message [topic: {}, brokerName: {}, queueId: {}, offset: {}] is invalid",
						messageQueue.getTopic(), messageQueue.getBrokerName(),
						messageQueue.getQueueId(), messageExt.getQueueOffset());
					continue;
				}
				lastTimestamp = System.currentTimeMillis();
				ctx.collect(rowData);
				// need ack every msg
				synchronized (RocketMQConsumer.this) {
					if (consumer != null) {
						RetryManager.retry(() -> consumer.ack(messageExt), retryStrategy);
					}
				}
				this.recordsNumMeterView.markEvent();
			}

			if (System.currentTimeMillis() - lastTimestamp > sourceIdleTimeMs) {
				lastTimestamp = Long.MAX_VALUE;
				ctx.markAsTemporarilyIdle();
			}

			if (messageExts.size() == 0) {
				Thread.sleep(DEFAULT_SLEEP_MILLISECONDS);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
		if (updateThread != null) {
			updateThread.interrupt();
			updateThread = null;
		}
		if (consumer != null) {
			synchronized (RocketMQConsumer.this) {
				if (consumer != null) {
					consumer.shutdown();
					consumer = null;
				}
			}
		}
	}

	private void assignMessageQueues(
			SupplierWithException<List<MessageQueuePb>, InterruptedException> supplier) throws InterruptedException {
		synchronized (RocketMQConsumer.this) {
			List<MessageQueuePb> currentMessageQueues = supplier.get();
			LOG.info("Fetch message queue size is {}", currentMessageQueues.size());
			Set<MessageQueue> newQueues =
				currentMessageQueues.stream().map(pb -> createMessageQueue(pb)).collect(Collectors.toSet());
			if (assignedMessageQueueSet == null || !assignedMessageQueueSet.equals(newQueues)) {
				LOG.info("Assign {} with {}.", assignedMessageQueueSet, newQueues);
				boolean isInitEmpty = assignedMessageQueuePbs != null && assignedMessageQueuePbs.isEmpty();
				assignedMessageQueuePbs = currentMessageQueues;
				assignedMessageQueueSet = newQueues;
				if (assignedMessageQueuePbs.isEmpty()) {
					LOG.warn("No queue assigned in this task.");
					return;
				}
				if (isInitEmpty) {
					this.notifyAll();
				}
				resetAllOffset();
				if (consumer != null) {
					RetryManager.retry(() -> consumer.assign(assignedMessageQueuePbs), retryStrategy);
				}
				for (MessageQueuePb messageQueuePb: assignedMessageQueuePbs) {
					topicAndQueuesGauge.addTopicAndQueue(
						messageQueuePb.getTopic(),
						String.format("%s:%d", messageQueuePb.getBrokerName(), messageQueuePb.getQueueId()));
				}
			}
		}
	}

	private List<MessageQueuePb> allocFixedMessageQueue() throws InterruptedException {
		List<QueryTopicQueuesResult> queryTopicQueuesResultList = new ArrayList<>();
		synchronized (RocketMQConsumer.class) {
			if (consumer != null) {
				RetryManager.retry(() -> queryTopicQueuesResultList.add(consumer.queryTopicQueues(topic)), retryStrategy);
			} else {
				return Collections.emptyList();
			}
		}
		QueryTopicQueuesResult queryTopicQueuesResult = queryTopicQueuesResultList.get(0);
		validateResponse(queryTopicQueuesResult.getErrorCode(), queryTopicQueuesResult.getErrorMsg());
		List<MessageQueuePb> messageQueuePbList = new ArrayList<>();
		for (MessageQueuePb queuePb: queryTopicQueuesResult.getMessageQueues()) {
			// Old alloc strategy.
			if (hashBelongToThisTask(createMessageQueue(queuePb))) {
				messageQueuePbList.add(queuePb);
			}
		}
		return messageQueuePbList;
	}

	private void validateResponse(int errorCode, String errMsg) {
		if (errorCode == ResponseCode.OK_VALUE) {
			return;
		}
		throw new FlinkRuntimeException(errMsg);
	}

	private MessageQueue createMessageQueue(MessageQueuePb queuePb) {
		return new MessageQueue(queuePb.getTopic(), queuePb.getBrokerName(), queuePb.getQueueId());
	}

	// keep same logic with flink-connector-rocketmq-legacy #AllocateMessageQueueStrategyParallelism#allocate()
	private boolean hashBelongToThisTask(MessageQueue messageQueue) {
		int assignedSubTaskId = RocketMQUtils.hashCodeOfMessageQueue(messageQueue, runtimeParallelism);
		return (assignedSubTaskId == subTaskId);
	}

	private void resetOffset(MessageQueuePb messageQueuePb) throws InterruptedException {
		MessageQueue messageQueue = createMessageQueue(messageQueuePb);
		String queueName = formatQueue(messageQueuePb);
		List<MessageQueuePb> queuePbList = Arrays.asList(messageQueuePb);
		Long offset = offsetTable.get(messageQueue);
		if (offset != null) {
			LOG.info("Queue {} use cache offset {}", queueName, offset);
			return;
		}

		if (consumer == null) {
			return;
		}

		ResetOffsetResult resetOffsetResult = null;
		if (isRestored) {
			offset = restoredOffsets.get(messageQueue);
			if (offset != null) {
				List<ResetOffsetResult> resetOffsetResultList = new ArrayList<>();
				Long finalOffset = offset;
				RetryManager.retry(
					() -> resetOffsetResultList.add(consumer.resetOffsetToSpecified(topic, group, queuePbList, finalOffset, false)),
					retryStrategy);
				resetOffsetResult = resetOffsetResultList.get(0);
				validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				Long newOffset = resetOffsetResult.getResetOffsetMap().get(messageQueuePb);
				offsetTable.put(messageQueue, offset);
				LOG.info("Queue {} use checkpoint offset {}, after reset offset {}",
					queueName, offset, newOffset);
				return;
			}
		}

		offset = RocketMQUtils.resetAndGetOffset(topic, group, messageQueuePb, props, consumer);
		offsetTable.put(messageQueue, offset);
	}

	private void resetAllOffset() throws InterruptedException {
		for (MessageQueuePb messageQueuePb: assignedMessageQueuePbs) {
			resetOffset(messageQueuePb);
		}
	}

	private Thread createUpdateThread() {
		Thread thread = new Thread() {
			@Override
			public void run() {
				RetryManager.Strategy fixRetryStrategy =
					RetryManager.createStrategy(RetryManager.StrategyType.FIXED_DELAY.name(),
						RocketMQOptions.CONSUMER_RETRY_TIMES_DEFAULT,
						RocketMQOptions.CONSUMER_RETRY_INIT_TIME_MS_DEFAULT);
				while (running && fixRetryStrategy.shouldRetry()) {
					try {
						Thread.sleep(discoveryIntervalMs);
						assignMessageQueues(() -> allocFixedMessageQueue());
						fixRetryStrategy.clear();
					} catch (InterruptedException e) {
						backgroundException = e;
						LOG.warn("Receive interrupted exception.");
					} catch (RMQClientException e) {
						backgroundException = e;
						LOG.warn("Receive RocketMQ client exception in RocketMQ partition discovery thread.", e);
					}
				}
			}
		};
		thread.setDaemon(true);
		thread.setName("RocketMQ_partition_discovery_thread: " + subTaskId);
		thread.setUncaughtExceptionHandler((t, e) -> {
			LOG.error("Receive uncaught exception in RocketMQ partition discovery thread.", e);
			backgroundException = e;
		});
		return thread;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	@Override
	public void close() throws Exception {
		cancel();
	}

	private List<MessageQueue> parseMessageQueueSet() {
		Map<String, List<MessageQueue>> queueMap =
			RocketMQUtils.parseCluster2QueueList(this.brokerQueueList);
		List<MessageQueue> messageQueues = queueMap.get(cluster);
		if (messageQueues == null) {
			if (!queueMap.isEmpty()) {
				String errorMsg = String.format("Job specific rocketmq queues, but [cluster: %s, topic: %s] not in %s",
					cluster, topic, String.join(",", queueMap.keySet()));
				LOG.error(errorMsg);
				throw new FlinkRuntimeException(errorMsg);
			}
			return Collections.emptyList();
		}

		List<MessageQueue> sortedQueues = messageQueues.stream().sorted().collect(Collectors.toList());
		List<MessageQueue> queueWithThisTasks = new ArrayList<>();
		for (int i = 0; i < sortedQueues.size(); i++) {
			if (i % getRuntimeContext().getNumberOfParallelSubtasks() == subTaskId) {
				queueWithThisTasks.add(sortedQueues.get(i));
			}
		}
		return queueWithThisTasks;
	}

	private String formatQueue(MessageQueuePb messageQueuePb) {
		return String.format("[broker %s, queue %s]", messageQueuePb.getBrokerName(), messageQueuePb.getQueueId());
	}

	private MessageQueuePb queue2queuePb(MessageQueue messageQueue) {
		return MessageQueuePb.newBuilder()
			.setTopic(messageQueue.getTopic())
			.setBrokerName(messageQueue.getBrokerName())
			.setQueueId(messageQueue.getQueueId()).build();
	}
}
