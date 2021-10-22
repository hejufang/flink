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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricsConstants;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.rocketmq.aggregate.SubTaskRunningState;
import org.apache.flink.streaming.connectors.rocketmq.aggregate.TaskStateAggFunction;
import org.apache.flink.streaming.connectors.rocketmq.serialization.RocketMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rocketmq.strategy.AllocateMessageQueueStrategyParallelism;
import org.apache.flink.streaming.connectors.rocketmq.strategy.FixedMessageQueueStrategy;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONNECTOR_TYPE_VALUE_ROCKETMQ;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_BATCH_SIZE;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_BATCH_SIZE_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_GROUP_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_OFFSET_EARLIEST;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_OFFSET_LATEST;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_OFFSET_RESET_TO;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_OFFSET_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_PULL_POOL_SIZE;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_PULL_POOL_SIZE_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_RETRY_INIT_TIME_MS_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_RETRY_TIMES;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_RETRY_TIMES_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_TAG;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_TAG_DEFAULT;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.CONSUMER_TOPIC;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.ROCKETMQ_NAMESRV_DOMAIN;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE_EARLIEST;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE_FROM_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE_LATEST;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.STARTUP_MODE_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.TOPIC_METRICS_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.buildAclRPCHook;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.buildConsumerConfigs;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQUtils.getInteger;
import static org.apache.flink.streaming.connectors.rocketmq.RocketMQUtils.getLong;

/**
 * The RocketMQSource is based on RocketMQ pull consumer mode, and provides exactly once reliability guarantees when
 * checkpoints are enabled. Otherwise, the source doesn't provide any reliability guarantees.
 */
public class RocketMQSource<OUT> extends RichParallelSourceFunction<OUT>
	implements CheckpointedFunction, CheckpointListener, ResultTypeQueryable<OUT>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);
	private static final String TASK_RUNNING_STATE = "task_running_state";
	private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
	private static final String NEW_OFFSETS_STATE_NAME = "rmq-topic-offset-states";
	private static final String FLINK_ROCKETMQ_METRICS = "flink_rocketmq_metrics";
	private static final String CONSUMER_RECORDS_METRICS_RATE = "consumerRecordsRate";
	private transient Counter skipDirty;
	private transient MeterView recordsNumMeterView;
	private transient MQPullConsumerScheduleService pullConsumerScheduleService;
	private transient AllocateMessageQueueStrategy parallelismStrategy;
	private DefaultMQPullConsumer consumer;
	private RocketMQDeserializationSchema<OUT> schema;
	private RunningChecker runningChecker;
	private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
	private transient ListState<Boolean> runningState;
	private Map<MessageQueue, Long> offsetTable;
	private Map<MessageQueue, Long> restoredOffsets;

	/**
	 * batch mode param.
	 */
	private boolean isTaskRunning = true;
	private boolean isSubTaskRunning = true;

	/**
	 * Data for pending but uncommitted offsets.
	 */
	private LinkedMap pendingOffsetsToCommit;
	private Properties props;
	private String topic;
	private String group;
	private String cluster;
	/**
	 * If forceAutoCommitEnabled=true, client will commit offsets automatically even if checkpoint is enabled.
	 */
	private boolean forceAutoCommitEnabled;
	private String brokerQueueList;
	/**
	 * Weather there is an successful checkpoint.
	 */
	private transient volatile boolean hasSuccessfulCheckpoint;
	private transient boolean enableCheckpoint;

	private transient int parallelism;
	private transient int retryTimes;
	private transient int subTaskId;
	/**
	 * Errors encountered in the async consumer are stored here.
	 */
	private transient volatile Throwable asyncError;
	private transient GlobalAggregateManager taskRunningAggregateManager;
	private transient TaskStateAggFunction taskStateAggFunction;
	private transient ProcessingTimeService processingTimeService;
	private transient RetryManager.Strategy retryStrategy;
	private transient Set<MessageQueue> assignMessageQueues;

	public RocketMQSource(RocketMQDeserializationSchema<OUT> schema, Properties props) {
		this.schema = schema;
		this.props = props;
		saveConfigurationToSystemProperties(props);
		runningChecker = new RunningChecker();
	}

	@Override
	public void open(Configuration parameters) {
		RocketMQUtils.setLog(props);
		LOG.debug("source open....");

		this.enableCheckpoint = ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled();

		if (pendingOffsetsToCommit == null) {
			pendingOffsetsToCommit = new LinkedMap();
		}
		if (!schema.isStreamingMode()) {
			taskStateAggFunction = new TaskStateAggFunction(parallelism);
			processingTimeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
			processingTimeService.registerTimer(processingTimeService
				.getCurrentProcessingTime() + TaskStateAggFunction.DEFAULT_AGG_INTERVAL, this);
		}

		//Wait for lite pull consumer
		pullConsumerScheduleService = new MQPullConsumerScheduleService(group);
		DefaultMQPullConsumer defaultMQPullConsumer =
			new DefaultMQPullConsumer(group, buildAclRPCHook(props));
		defaultMQPullConsumer.setCluster(cluster);
		defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
		pullConsumerScheduleService.setDefaultMQPullConsumer(defaultMQPullConsumer);
		consumer = pullConsumerScheduleService.getDefaultMQPullConsumer();
		consumer.setAllocateMessageQueueStrategy(parallelismStrategy);

		consumer.setInstanceName(getRuntimeContext().getIndexOfThisSubtask() + "_" + UUID.randomUUID());
		buildConsumerConfigs(props, consumer);
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup()
			.addGroup(TOPIC_METRICS_GROUP, this.topic)
			.addGroup(CONSUMER_GROUP_METRICS_GROUP, this.group)
			.addGroup(MetricsConstants.METRICS_CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ROCKETMQ);

		this.skipDirty = metricGroup.counter("skipDirty");
		this.recordsNumMeterView = metricGroup.meter(CONSUMER_RECORDS_METRICS_RATE, new MeterView(60));
	}

	@Override
	public void run(SourceContext context) throws Exception {
		LOG.debug("source run....");
		// The lock that guarantees that record emission and state updates are atomic,
		// from the view of taking a checkpoint.
		final Object lock = context.getCheckpointLock();

		int delayWhenMessageNotFound = getInteger(props, CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND,
			CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND_DEFAULT);

		String tag = props.getProperty(CONSUMER_TAG, CONSUMER_TAG_DEFAULT);

		int pullPoolSize = getInteger(props, CONSUMER_PULL_POOL_SIZE,
			CONSUMER_PULL_POOL_SIZE_DEFAULT);

		int pullBatchSize = getInteger(props, CONSUMER_BATCH_SIZE,
			CONSUMER_BATCH_SIZE_DEFAULT);

		pullConsumerScheduleService.setPullThreadNums(pullPoolSize);
		pullConsumerScheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {
			@Override
			public void doPullTask(MessageQueue mq, PullTaskContext pullTaskContext) {
				PullResult[] pullResults = new PullResult[1];
				try {
					long offset = getMessageQueueOffset(mq, consumer);
					if (offset < 0) {
						return;
					}

					RetryManager.retry(new RetryManager.RetryableRunner() {
						@Override
						public void run() throws IOException {
							try {
								pullResults[0] = consumer.pull(mq, tag, offset, pullBatchSize);
							} catch (Exception e) {
								throw new FlinkRuntimeException("Failed to pull data from " + mq.toString(), e);
							}
						}
					}, retryStrategy.copy());
					boolean found = false;
					PullResult pullResult = pullResults[0];
					switch (pullResult.getPullStatus()) {
						case FOUND:
							List<MessageExt> messages = pullResult.getMsgFoundList();
							for (MessageExt msg : messages) {
								OUT data = schema.deserialize(mq, msg);
								if (schema.isEndOfStream(assignMessageQueues, data)) {
									LOG.info("Subtask: {} received all assign message queue end message.", subTaskId);
									isSubTaskRunning = false;
									break;
								}
								// output and state update are atomic
								synchronized (lock) {
									if (data == null) {
										skipDirty.inc();
									} else {
										context.collectWithTimestamp(data, msg.getBornTimestamp());
										recordsNumMeterView.markEvent();
									}
									putMessageQueueOffset(mq, msg.getQueueOffset() + 1);
								}
							}
							found = true;
							break;
						case NO_MATCHED_MSG:
							LOG.debug("No matched message after offset {} for queue {}", offset, mq);
							break;
						case NO_NEW_MSG:
							break;
						case OFFSET_ILLEGAL:
							LOG.warn("Offset {} is illegal for queue {}", offset, mq);
							break;
						default:
							break;
					}

					synchronized (lock) {
						putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
					}

					if (found) {
						pullTaskContext.setPullNextDelayTimeMillis(0); // no delay when messages were found
					} else {
						pullTaskContext.setPullNextDelayTimeMillis(delayWhenMessageNotFound);
					}
				} catch (Exception e) {
					// We catch all error here and close the source.
					// Otherwise, MQPullConsumerScheduleService will catch all error
					// and we cannot get the error and fail the job.
					LOG.error("Failed to consume data from source.", e);
					asyncError = e;
				}
			}
		});

		if (isSubTaskRunning) {
			try {
				pullConsumerScheduleService.start();
				if (!schema.isStreamingMode()) {
					isSubTaskRunning = checkRunnable();
					if (!isSubTaskRunning) {
						pullConsumerScheduleService.shutdown();
					}
				}
			} catch (MQClientException e) {
				throw new RuntimeException(e);
			}
		}
		runningChecker.setRunning(isTaskRunning);
		awaitTermination();
	}

	private boolean checkRunnable() {
		try {
			return CollectionUtils.isNotEmpty(assignMessageQueues) || schema.isStreamingMode();
		} catch (Exception e) {
			throw new IllegalStateException(String.format("%d check balance mq failed.", subTaskId), e);
		}
	}

	private void awaitTermination() throws Exception {
		while (runningChecker.isRunning()) {
			checkErrors();
			Thread.sleep(50);
		}
	}

	private long getMessageQueueOffset(MessageQueue mq, DefaultMQPullConsumer consumer) throws MQClientException {
		Long offset = offsetTable.get(mq);

		if (offset == null) {
			// new message queue will use min offset to avoid lose data.
			if (!assignMessageQueues.contains(mq)) {
				LOG.info("Subtask {} detect new message queue: {} in running state, use min offset for this message queue.",
					subTaskId, mq);
				offset = consumer.minOffset(mq);
				assignMessageQueues.add(mq);
			} else {
				LOG.warn("Subtask {} detect missing message queue: {}, start get offset from config.", subTaskId, mq);
				offset = getStartModeOffset(consumer, mq);
			}
		}
		offsetTable.put(mq, offset);
		return offsetTable.get(mq);
	}

	private long getStartModeOffset(DefaultMQPullConsumer consumer, MessageQueue mq) throws MQClientException {
		long offset;
		String startupMode = props.getProperty(STARTUP_MODE, STARTUP_MODE_GROUP);
		switch (startupMode) {
			case STARTUP_MODE_GROUP:
				offset = consumer.fetchConsumeOffset(mq, false);
				if (offset < 0) {
					// We cannot get normal offset from RMQ.
					// Default setting is earliest, in case of data loss.
					String initialOffset = props.getProperty(CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_EARLIEST);
					switch (initialOffset) {
						case CONSUMER_OFFSET_EARLIEST:
							offset = consumer.minOffset(mq);
							break;
						case CONSUMER_OFFSET_LATEST:
							offset = consumer.maxOffset(mq);
							break;
						case CONSUMER_OFFSET_TIMESTAMP:
							offset = consumer.searchOffset(mq, getLong(props,
								CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis()));
							break;
						default:
							throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
					}
				}
				break;
			case STARTUP_MODE_EARLIEST:
				offset = consumer.minOffset(mq);
				break;
			case STARTUP_MODE_LATEST:
				offset = consumer.maxOffset(mq);
				break;
			case STARTUP_MODE_TIMESTAMP:
				offset = consumer.searchOffset(mq, getLong(props,
					STARTUP_MODE_FROM_TIMESTAMP, System.currentTimeMillis()));
				break;
			default:
				throw new IllegalArgumentException("Unknown value for startup-mode: " + startupMode);
		}
		return offset;
	}

	public void setForceAutoCommitEnabled(boolean forceAutoCommitEnabled) {
		this.forceAutoCommitEnabled = forceAutoCommitEnabled;
	}

	private void putMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
		offsetTable.put(mq, offset);
		boolean needCommitOffset =
			(!enableCheckpoint) || (forceAutoCommitEnabled && hasSuccessfulCheckpoint);
		if (needCommitOffset) {
			consumer.updateConsumeOffset(mq, offset);
		}
	}

	@Override
	public void cancel() {
		LOG.debug("cancel ...");
		runningChecker.setRunning(false);

		if (pullConsumerScheduleService != null) {
			pullConsumerScheduleService.shutdown();
		}
		if (offsetTable != null) {
			offsetTable.clear();
		}
		if (restoredOffsets != null) {
			restoredOffsets.clear();
		}
		if (pendingOffsetsToCommit != null) {
			pendingOffsetsToCommit.clear();
		}
	}

	private void checkErrors() throws Exception {
		Throwable t = asyncError;
		if (t != null) {
			// prevent double throwing
			asyncError = null;
			throw new Exception("Failed to consume data from RocketMQ: " + t.getMessage(), t);
		}
	}

	@Override
	public void close() throws Exception {
		LOG.debug("close ...");
		// pretty much the same logic as cancelling
		try {
			cancel();
			checkErrors();
		} finally {
			super.close();
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// called when a snapshot for a checkpoint is requested

		if (!runningChecker.isRunning()) {
			LOG.debug("snapshotState() called on closed source; returning null.");
			return;
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
		}

		unionOffsetStates.clear();

		HashMap<MessageQueue, Long> currentOffsets = new HashMap<>(offsetTable.size());

		for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
			unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
			currentOffsets.put(entry.getKey(), entry.getValue());
		}

		pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);

		if (!schema.isStreamingMode()) {
			runningState.clear();
			runningState.add(isSubTaskRunning);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
				offsetTable, context.getCheckpointId(), context.getCheckpointTimestamp());
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// called every time the user-defined function is initialized,
		// be that when the function is first initialized or be that
		// when the function is actually recovering from an earlier checkpoint.
		// Given this, initializeState() is not only the place where different types of state are initialized,
		// but also where state recovery logic is included.
		LOG.debug("initialize State ...");
		Preconditions.checkNotNull(props, "Consumer properties can not be empty");
		Preconditions.checkNotNull(schema, "RocketMQDeserializationSchema can not be null");
		this.topic = props.getProperty(CONSUMER_TOPIC);
		this.group = props.getProperty(CONSUMER_GROUP);
		this.cluster = props.getProperty(ROCKETMQ_NAMESRV_DOMAIN);

		Preconditions.checkNotNull(cluster, "Cluster can not be null");
		Preconditions.checkNotNull(topic, "Consumer topic can not be null");
		Preconditions.checkNotNull(group, "Consumer group can not be empty");
		Preconditions.checkArgument(!topic.isEmpty(), "Consumer topic can not be empty");
		Preconditions.checkArgument(!group.isEmpty(), "Consumer group can not be empty");

		this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

		parallelismStrategy = getAllocStrategy(parallelism, subTaskId, cluster);
		this.taskRunningAggregateManager = ((StreamingRuntimeContext) getRuntimeContext()).getGlobalAggregateManager();
		this.offsetTable = new ConcurrentHashMap<>();
		this.restoredOffsets = new ConcurrentHashMap<>();

		this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
			OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {

		})));

		// ************************* DTS State Compatibility *******************************
		// add new states into current version
		ListState<Tuple2<com.bytedance.rocketmq.clientv2.message.MessageQueue, Long>> newUnionOffsetStates = context.getOperatorStateStore()
				.getUnionListState(new ListStateDescriptor<>(NEW_OFFSETS_STATE_NAME, TypeInformation.of(
						new TypeHint<Tuple2<com.bytedance.rocketmq.clientv2.message.MessageQueue, Long>>() {
						})));

		for (Tuple2<com.bytedance.rocketmq.clientv2.message.MessageQueue, Long> newState : newUnionOffsetStates.get()) {
			com.bytedance.rocketmq.clientv2.message.MessageQueue legacyQueue = newState.f0;
			this.unionOffsetStates.add(Tuple2.of(
					new MessageQueue(legacyQueue.getTopic(), legacyQueue.getBrokerName(), legacyQueue.getQueueId()),
					newState.f1));
		}

		newUnionOffsetStates.clear();
		// ************************* DTS State Compatibility *******************************

		if (!schema.isStreamingMode()) {
			this.runningState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
				TASK_RUNNING_STATE, TypeInformation.of(new TypeHint<Boolean>() {
			})));
		}

		if (context.isRestored()) {
			hasSuccessfulCheckpoint = true;

			for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
				if (!restoredOffsets.containsKey(mqOffsets.f0) || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
					restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
				}
			}
			if (!schema.isStreamingMode()) {
				for (Boolean storedRunningState : runningState.get()) {
					isSubTaskRunning = storedRunningState && isSubTaskRunning;
				}
			}
			LOG.info("Subtask {} Setting restore state in the consumer. Using the following offsets: {}", subTaskId, restoredOffsets);
		} else {
			LOG.info("Subtask {} No restore state for the consumer.", subTaskId);
		}

		this.retryTimes = getInteger(props, CONSUMER_RETRY_TIMES, CONSUMER_RETRY_TIMES_DEFAULT);
		this.retryStrategy = RetryManager
			.createStrategy("EXPONENTIAL_BACKOFF", retryTimes, CONSUMER_RETRY_INIT_TIME_MS_DEFAULT);
		fetchAssignMessageQueue(context.isRestored());
	}

	/**
	 * Fetch current assign message queue list.
	 */
	private void fetchAssignMessageQueue(boolean restored) {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(buildAclRPCHook(props));
		consumer.setConsumerGroup(group);
		System.setProperty(ROCKETMQ_NAMESRV_DOMAIN, props.getProperty(ROCKETMQ_NAMESRV_DOMAIN));
		try {
			consumer.start();
			List<Set<MessageQueue>> queueSetList = new ArrayList<>();
			RetryManager.retry(new RetryManager.RetryableRunner() {
				@Override
				public void run() throws IOException {
					try {
						queueSetList.add(consumer.fetchSubscribeMessageQueues(topic));
					} catch (Exception e) {
						throw new FlinkRuntimeException(String.format("Failed to fetch topic %s offset", topic));
					}
				}
			}, retryStrategy.copy());
			assignMessageQueues = new HashSet<>(parallelismStrategy.allocate(consumer.getConsumerGroup(),
				null, new ArrayList<>(queueSetList.get(0)), null));

			Set<MessageQueue> stateMessageQueues = new HashSet<>(parallelismStrategy.allocate(consumer.getConsumerGroup(),
				null, new ArrayList<>(restoredOffsets.keySet()), null));

			if (CollectionUtils.isNotEmpty(stateMessageQueues)) {
				assignMessageQueues.addAll(stateMessageQueues);
			}

			LOG.info("Subtask {} assigned message queue: {}, size: {}.", subTaskId, assignMessageQueues
				, CollectionUtils.size(assignMessageQueues));

			if (restored && restoredOffsets != null) {
				for (MessageQueue messageQueue : assignMessageQueues) {
					if (!restoredOffsets.containsKey(messageQueue)) {
						restoredOffsets.put(messageQueue, consumer.minOffset(messageQueue));
					}
				}
				//Only put assigned message queue into offset table.
				for (MessageQueue messageQueue : assignMessageQueues) {
					offsetTable.put(messageQueue, restoredOffsets.get(messageQueue));
				}
			}
			if (!restored) {
				for (MessageQueue messageQueue : assignMessageQueues) {
					offsetTable.put(messageQueue, getMessageQueueOffset(messageQueue, consumer));
				}
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(String
				.format("Subtask %s failed when init offset table.", subTaskId), e);
		} finally {
			consumer.shutdown();
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return schema.getProducedType();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		// callback when checkpoint complete
		if (!runningChecker.isRunning()) {
			LOG.debug("notifyCheckpointComplete() called on closed source; returning null.");
			return;
		}

		hasSuccessfulCheckpoint = true;

		final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
		if (posInMap == -1) {
			LOG.warn("Received confirmation for unknown checkpoint id {}", checkpointId);
			return;
		}

		Map<MessageQueue, Long> offsets = (Map<MessageQueue, Long>) pendingOffsetsToCommit.remove(posInMap);

		// remove older checkpoints in map
		for (int i = 0; i < posInMap; i++) {
			pendingOffsetsToCommit.remove(0);
		}

		if (offsets == null || offsets.size() == 0) {
			LOG.debug("Checkpoint state was empty.");
			return;
		}

		for (Map.Entry<MessageQueue, Long> entry : offsets.entrySet()) {
			consumer.updateConsumeOffset(entry.getKey(), entry.getValue());
		}

	}

	/**
	 * Save rocketmq config to system properties, so we can use it when register the dashboard.
	 * See {@link org.apache.flink.monitor.Dashboard}.
	 */
	private void saveConfigurationToSystemProperties(Properties properties) {
		try {
			String cluster = properties.getProperty(ROCKETMQ_NAMESRV_DOMAIN);
			String topic = properties.getProperty(CONSUMER_TOPIC);
			String consumerGroup = properties.getProperty(CONSUMER_GROUP);
			String kafkaMetricsStr = System.getProperty(FLINK_ROCKETMQ_METRICS, "[]");
			JSONParser parser = new JSONParser();
			JSONArray jsonArray = (JSONArray) parser.parse(kafkaMetricsStr);
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("cluster", cluster);
			jsonObject.put("topic", topic);
			jsonObject.put("consumer_group", consumerGroup);
			jsonArray.add(jsonObject);
			System.setProperty(FLINK_ROCKETMQ_METRICS, jsonArray.toJSONString());
		} catch (Throwable t) {
			// We catch all Throwable as it is not critical path.
			LOG.warn("Parse rocketmq metrics failed.", t);
		}
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		LOG.info("SubTask {} isRunning: {}.", subTaskId, isSubTaskRunning);
		isTaskRunning = taskRunningAggregateManager.updateGlobalAggregate(TASK_RUNNING_STATE,
			new SubTaskRunningState(subTaskId, isSubTaskRunning),
			taskStateAggFunction);
		runningChecker.setRunning(isTaskRunning);
		if (isTaskRunning) {
			processingTimeService.registerTimer(processingTimeService.getCurrentProcessingTime()
				+ TaskStateAggFunction.DEFAULT_AGG_INTERVAL, this);
		}
	}

	public void setBrokerQueueList(String brokerQueueList) {
		this.brokerQueueList = brokerQueueList;
	}

	private AllocateMessageQueueStrategy getAllocStrategy(int parallelismNum, int subTaskId, String cluster) {
		Map<String, List<MessageQueue>> map = RocketMQUtils.parseCluster2QueueList(brokerQueueList);
		List<MessageQueue> queues = map.get(cluster);
		if (queues == null) {
			LOG.info("Cluster {} not specific queues.", cluster);
			return new AllocateMessageQueueStrategyParallelism(parallelismNum, subTaskId);
		} else {
			List<MessageQueue> sortedQueue = new ArrayList<>(queues);
			sortedQueue.sort(MessageQueue::compareTo);
			Set<MessageQueue> queuesInCurTask = new HashSet<>();
			for (int i = 0; i < sortedQueue.size(); i++) {
				if (i % parallelismNum == subTaskId) {
					queuesInCurTask.add(sortedQueue.get(i));
				}
			}
			return new FixedMessageQueueStrategy(queuesInCurTask, subTaskId);
		}
	}
}
