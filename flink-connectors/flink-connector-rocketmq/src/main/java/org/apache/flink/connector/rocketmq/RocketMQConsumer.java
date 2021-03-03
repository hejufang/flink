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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
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
import org.apache.flink.util.function.SupplierWithException;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.mqproxy.proto.ResponseCode;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.consumer.PollResult;
import com.bytedance.rocketmq.clientv2.consumer.QueryOffsetResult;
import com.bytedance.rocketmq.clientv2.consumer.QueryTopicQueuesResult;
import com.bytedance.rocketmq.clientv2.consumer.ResetOffsetResult;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONSUMER_OFFSET_EARLIEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONSUMER_OFFSET_LATEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONSUMER_OFFSET_TIMESTAMP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_LATEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP;
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

	private static final int FETCH_BATCH_SIZE = 100;
	private static final int DEFAULT_SLEEP_MILLISECONDS = 1;
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQConsumer.class);
	private static final String OFFSETS_STATE_NAME = "rmq-topic-offset-states";
	private static final String CONSUMER_RECORDS_METRICS_RATE = "consumerRecordsRate";
	public static final String ROCKET_MQ_CONSUMER_METRICS_GROUP = "RocketMQConsumer";
	private String cluster;
	private String topic;
	private String group;
	private String tag;
	private Map<String, String> props;
	private RocketMQDeserializationSchema<T> schema;
	private RocketMQOptions.AssignQueueStrategy assignQueueStrategy;
	private int parallelism;

	private transient MeterView recordsNumMeterView;
	private transient DefaultMQPullConsumer consumer;
	private transient List<MessageQueuePb> assignedMessageQueuePbs;
	private transient Set<MessageQueue> assignedMessageQueueSet;
	private transient Thread updateThread;
	private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
	private transient volatile boolean running;
	private transient boolean isRestored;
	private transient Map<MessageQueue, Long> offsetTable;
	private transient Map<MessageQueue, Long> restoredOffsets;
	private transient int subTaskId;
	private transient Counter skipDirtyCounter;

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
		this.assignQueueStrategy = config.getAssignQueueStrategy();
		this.parallelism = config.getParallelism();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.consumer = new DefaultMQPullConsumer(cluster, topic, group, getRocketMQProperties(props));
		this.consumer.setAutoCommit(true);
		this.consumer.setInstanceName(topic + "_" + getRuntimeContext().getIndexOfThisSubtask());
		if (this.parallelism > 0) {
			assert this.parallelism == getRuntimeContext().getNumberOfParallelSubtasks();
		} else {
			this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
		}

		this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
		this.offsetTable = new ConcurrentHashMap<>();
		this.restoredOffsets = new HashMap<>();

		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(ROCKET_MQ_CONSUMER_METRICS_GROUP)
			.addGroup(RocketMQOptions.TOPIC_METRICS_GROUP, this.topic)
			.addGroup(RocketMQOptions.CONSUMER_GROUP_METRICS_GROUP, this.group)
			.addGroup(MetricsConstants.METRICS_CONNECTOR_TYPE, RocketMQOptions.CONNECTOR_TYPE_VALUE_ROCKETMQ)
			.addGroup(MetricsConstants.METRICS_FLINK_VERSION, MetricsConstants.FLINK_VERSION_VALUE);

		this.recordsNumMeterView = metricGroup.meter(CONSUMER_RECORDS_METRICS_RATE, new MeterView(60));
		schema.open(() -> getRuntimeContext().getMetricGroup());
		this.skipDirtyCounter = getRuntimeContext().getMetricGroup().counter(FactoryUtil.SOURCE_SKIP_DIRTY);
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
		if (!running) {
			LOG.info("snapshotState() called on closed source");
		} else {
			unionOffsetStates.clear();
			for (Map.Entry<MessageQueue, Long> entry: offsetTable.entrySet()) {
				unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
			}
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
			OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {})
		));
		isRestored = context.isRestored();
		unionOffsetStates.get().forEach(
			x -> restoredOffsets.put(x.f0, x.f1)
		);
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		running = true;
		if (tag != null) {
			consumer.setSubExpr(tag);
		}
		consumer.start();

		if (assignQueueStrategy == RocketMQOptions.AssignQueueStrategy.FIXED) {
			assignMessageQueues(this::allocFixedMessageQueue);
		} else if (assignQueueStrategy == RocketMQOptions.AssignQueueStrategy.ROUND_ROBIN) {
			assignMessageQueues(this::allocRoundRobinMessageQueues);
		}

		for (MessageQueuePb messageQueuePb: assignedMessageQueuePbs) {
			resetOffset(messageQueuePb);
		}

		if (assignQueueStrategy == RocketMQOptions.AssignQueueStrategy.FIXED) {
			updateThread = createUpdateThread();
		}

		while (running) {
			PollResult pollResult;
			synchronized (consumer) {
				pollResult = consumer.poll(FETCH_BATCH_SIZE);
			}
			if (pollResult.getErrorCode() == ResponseCode.OK_VALUE) {
				for (MessageExt messageExt: pollResult.getMsgList()) {
					MessageQueue messageQueue = createMessageQueue(messageExt.getMessageQueue());
					T rowData = schema.deserialize(messageQueue, messageExt);
					if (rowData == null) {
						skipDirtyCounter.inc();
						continue;
					}
					ctx.collect(rowData);
					this.recordsNumMeterView.markEvent();
					offsetTable.put(messageQueue, messageExt.getMaxOffset());
				}
				if (pollResult.getMsgList().size() == 0) {
					Thread.sleep(DEFAULT_SLEEP_MILLISECONDS);
				}
			} else {
				LOG.warn("Receive error code is {}, error msg is {}.", pollResult.getErrorCode(), pollResult.getErrorMsg());
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
		if (updateThread != null) {
			updateThread.interrupt();
		}
		if (consumer != null) {
			consumer.shutdown();
		}
	}

	private void assignMessageQueues(
			SupplierWithException<List<MessageQueuePb>, InterruptedException> supplier) throws InterruptedException {
		List<MessageQueuePb> currentMessageQueues = supplier.get();
		if (tryReplaceOld(currentMessageQueues)) {
			synchronized (consumer) {
				consumer.assign(currentMessageQueues);
			}
		}
	}

	private boolean tryReplaceOld(List<MessageQueuePb> queuePbList) {
		Set<MessageQueue> newQueues =
			queuePbList.stream().map(pb -> createMessageQueue(pb)).collect(Collectors.toSet());
		if (assignedMessageQueueSet == null || !assignedMessageQueueSet.equals(newQueues)) {
			LOG.info("Assign {} with {}.", assignedMessageQueueSet, newQueues);
			assignedMessageQueuePbs = queuePbList;
			assignedMessageQueueSet = newQueues;
			return true;
		}

		return false;
	}

	private List<MessageQueuePb> allocRoundRobinMessageQueues() throws InterruptedException {
		QueryTopicQueuesResult queryTopicQueuesResult = consumer.queryTopicQueues(topic);
		validateResponse(queryTopicQueuesResult.getErrorCode(), queryTopicQueuesResult.getErrorMsg());
		List<MessageQueuePb> queuePbList = new ArrayList<>(queryTopicQueuesResult.getMessageQueues());
		// brokerName with queueId represent a min consumer unit in rocketMQ like kafka partition
		queuePbList.sort(
			Comparator.comparing(MessageQueuePb::getBrokerName).thenComparingInt(MessageQueuePb::getQueueId));
		List<MessageQueuePb> resultQueues = new ArrayList<>();
		for (int i = 0; i < queuePbList.size(); i++) {
			if ((i % parallelism) == subTaskId) {
				resultQueues.add(queuePbList.get(i));
			}
		}
		return resultQueues;
	}

	private List<MessageQueuePb> allocFixedMessageQueue() throws InterruptedException {
		QueryTopicQueuesResult queryTopicQueuesResult = consumer.queryTopicQueues(topic);
		validateResponse(queryTopicQueuesResult.getErrorCode(), queryTopicQueuesResult.getErrorMsg());
		List<MessageQueuePb> messageQueuePbList = new ArrayList<>();
		for (MessageQueuePb queuePb: queryTopicQueuesResult.getMessageQueues()) {
			// Old alloc strategy.
			if (((createMessageQueue(queuePb).hashCode() * 31) & 0x7FFFFFFF) % parallelism == subTaskId) {
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

	private void resetOffset(MessageQueuePb messageQueuePb) throws InterruptedException {
		MessageQueue messageQueue = createMessageQueue(messageQueuePb);
		List<MessageQueuePb> queuePbList = Arrays.asList(messageQueuePb);
		Long offset = offsetTable.get(messageQueue);
		if (offset != null) {
			return;
		}

		ResetOffsetResult resetOffsetResult;
		if (isRestored) {
			offset = restoredOffsets.get(messageQueue);
			if (offset != null) {
				resetOffsetResult = consumer.resetOffsetToSpecified(topic, group, queuePbList, offset, false);
				validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				return;
			}
		}

		String startupMode = props.getOrDefault(SCAN_STARTUP_MODE.key(), SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS);
		QueryOffsetResult queryOffsetResult;
		switch (startupMode) {
			case SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS:
				queryOffsetResult = consumer.queryCommitOffset(topic, queuePbList);
				validateResponse(queryOffsetResult.getErrorCode(), queryOffsetResult.getErrorMsg());
				if (getOnlyOffset(queryOffsetResult) < 0) {
					// We cannot get normal offset from RMQ.
					// Default setting is earliest, in case of data loss.
					String initialOffset = props.getOrDefault(RocketMQOptions.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_EARLIEST);
					switch (initialOffset) {
						case CONSUMER_OFFSET_EARLIEST:
							resetOffsetResult = consumer.resetOffsetToEarliest(topic, group, queuePbList, false);
							break;
						case CONSUMER_OFFSET_LATEST:
							resetOffsetResult = consumer.resetOffsetToLatest(topic, group, queuePbList, false);
							break;
						case CONSUMER_OFFSET_TIMESTAMP:
							long timestamp = RocketMQUtils.getLong(props,
								RocketMQOptions.CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis());
							resetOffsetResult = consumer.resetOffsetByTimestamp(topic, group, queuePbList, timestamp, false);
							break;
						default:
							throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
					}
					validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				}
				break;
			case SCAN_STARTUP_MODE_VALUE_EARLIEST:
				resetOffsetResult = consumer.resetOffsetToEarliest(topic, group, queuePbList, false);
				validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				break;
			case SCAN_STARTUP_MODE_VALUE_LATEST:
				resetOffsetResult = consumer.resetOffsetToLatest(topic, group, queuePbList, false);
				validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				break;
			case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
				long timestamp = RocketMQUtils.getLong(props,
					SCAN_STARTUP_MODE_VALUE_TIMESTAMP, System.currentTimeMillis());
				resetOffsetResult = consumer.resetOffsetByTimestamp(topic, group, queuePbList, timestamp, false);
				validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
				break;
			default:
				throw new IllegalArgumentException("Unknown value for startup-mode: " + startupMode);
		}
	}

	private long getOnlyOffset(QueryOffsetResult queryOffsetResult) {
		return queryOffsetResult.getOffsetMap().entrySet().iterator().next().getValue();
	}

	private Thread createUpdateThread() {
		Thread thread = new Thread() {
			@Override
			public void run() {
				while (running) {
					try {
						Thread.sleep(5 * 60 * 1000);
						assignMessageQueues(() -> allocFixedMessageQueue());
					} catch (InterruptedException e) {
						LOG.info("Receive interrupted exception.");
					}
				}
			}
		};
		thread.setDaemon(true);
		thread.setName("RocketMQ_partition_discovery_thread: " + subTaskId);
		return thread;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}
}
