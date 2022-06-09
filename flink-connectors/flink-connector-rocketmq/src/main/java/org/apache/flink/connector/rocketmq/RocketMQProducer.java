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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.selector.DeferLoopSelector;
import org.apache.flink.connector.rocketmq.selector.DeferMillisSelector;
import org.apache.flink.connector.rocketmq.selector.MsgDelayLevelSelector;
import org.apache.flink.connector.rocketmq.selector.TopicSelector;
import org.apache.flink.connector.rocketmq.serialization.KeyValueSerializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.functions.RowKindSinkFilter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RetryManager;

import com.bytedance.mqproxy.proto.MessageType;
import com.bytedance.rocketmq.clientv2.message.Message;
import com.bytedance.rocketmq.clientv2.producer.DefaultMQProducer;
import com.bytedance.rocketmq.clientv2.producer.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFER_MILLIS_MAX;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFER_MILLIS_MIN;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL00;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.getRocketMQProperties;

/**
 * RocketMQProducer.
 */
public class RocketMQProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction, SpecificParallelism {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQProducer.class);

	private KeyValueSerializationSchema<T> serializationSchema;
	private final String cluster;
	private final String group;
	private final int batchSize;
	private transient List<Message> messageList;
	private Map<String, String> props;
	private final TopicSelector<T> topicSelector;
	private int messageDelayLevel;
	private final DeferLoopSelector<T> deferLoopSelector;
	private final DeferMillisSelector<T> deferMillisSelector;
	private int parallelism;
	private final MsgDelayLevelSelector<T> msgDelayLevelSelector;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final long flushIntervalMs;
	private final RowKindSinkFilter<T> rowKindSinkFilter;
	private boolean batchFlushEnable;
	private final boolean hasPartitionKey;

	private transient DefaultMQProducer producer;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient RetryManager.Strategy retryStrategy;

	// TODO: support async write rocketmq
	public RocketMQProducer(
			KeyValueSerializationSchema<T> serializationSchema,
			Map<String, String> props,
			RocketMQConfig<T> rocketMQConfig) {
		this.serializationSchema = serializationSchema;
		this.props = props;
		this.cluster = rocketMQConfig.getCluster();
		this.group = rocketMQConfig.getGroup();
		this.messageDelayLevel = rocketMQConfig.getDelayLevel();
		this.batchSize = rocketMQConfig.getSendBatchSize();
		this.topicSelector = rocketMQConfig.getTopicSelector();
		this.msgDelayLevelSelector = rocketMQConfig.getMsgDelayLevelSelector();
		this.parallelism = rocketMQConfig.getParallelism();
		this.rateLimiter = rocketMQConfig.getRateLimiter();
		this.deferMillisSelector = rocketMQConfig.getDeferMillisSelector();
		this.deferLoopSelector = rocketMQConfig.getDeferLoopSelector();
		this.batchFlushEnable = rocketMQConfig.isBatchFlushEnable();
		this.flushIntervalMs = rocketMQConfig.getFlushIntervalMs();
		this.rowKindSinkFilter = rocketMQConfig.getRowKindSinkFilter();
		this.hasPartitionKey = rocketMQConfig.getSinkKeyByFields() != null;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		messageList = new ArrayList<>();
		this.retryStrategy = RetryManager.createStrategy(RetryManager.StrategyType.EXPONENTIAL_BACKOFF.name(),
			RocketMQOptions.CONSUMER_RETRY_TIMES_DEFAULT,
			RocketMQOptions.CONSUMER_RETRY_INIT_TIME_MS_DEFAULT);
		// TODO: use props construct producer.
		producer = new DefaultMQProducer(cluster, group, null, getRocketMQProperties(props));
		RetryManager.retry(() -> producer.start(), retryStrategy);
		serializationSchema.open(() -> getRuntimeContext().getMetricGroup());
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}

		if (msgDelayLevelSelector != null || messageDelayLevel > 0) {
			batchFlushEnable = false;
			LOG.warn("Batch flush is disable because you use delay level.");
		}

		if (batchFlushEnable) {
			this.scheduler = Executors.newScheduledThreadPool(
				1, new ExecutorThreadFactory("rocketmq-interval-flush-thread"));
			this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
				synchronized (messageList) {
					try {
						flushMessages();
					} catch (Exception e) {
						LOG.error("Flush failed ");
						flushException = e;
					}
				}
			}, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		synchronized (messageList) {
			flushMessages();
		}
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		if (rowKindSinkFilter != null && !rowKindSinkFilter.filter(value)) {
			return;
		}

		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}

		if (flushException != null) {
			throw flushException;
		}

		Message message = prepareMessage(value);
		if (batchFlushEnable) {
			synchronized (messageList) {
				messageList.add(message);
				if (messageList.size() >= batchSize) {
					flushMessages();
				}
			}
		} else {
			List<SendResult> sendResultList = new ArrayList<>();
			RetryManager.retry(() -> sendResultList.add(producer.send(message)), retryStrategy);
			SendResult sendResult = sendResultList.get(0);
			LOG.debug("Send message id: {}", sendResult.getMsgId());
		}
	}

	private void flushMessages() {
		if (messageList.size() > 0) {
			try {
				if (hasPartitionKey) {
					List<List<SendResult>> sendResultsList = new ArrayList<>();
					RetryManager.retry(() -> sendResultsList.add(producer.sendMessages(messageList, null)), retryStrategy);
					List<SendResult> sendResults = sendResultsList.get(0);
					if (LOG.isDebugEnabled()) {
						LOG.debug("Send Message Ids: {}",
							sendResults.stream().map(SendResult::getMsgId).collect(Collectors.joining(",")));
					}
				} else {
					List<SendResult> sendResultList = new ArrayList<>();
					RetryManager.retry(() -> sendResultList.add(producer.send(messageList)), retryStrategy);
					SendResult sendResult = sendResultList.get(0);
					LOG.debug("Send Message Ids: {}", sendResult.getMsgId());
				}
			} catch (Exception e) {
				LOG.error("Flush exception", e);
				throw new FlinkRuntimeException("Rocketmq flush exception", e);
			}
			messageList.clear();
		}
	}

	protected Message prepareMessage(T input) {
		String topic = topicSelector.getTopic(input);
		String tag = (tag = topicSelector.getTag(input)) != null ? tag : "";

		byte[] k = serializationSchema.serializeKey(input);
		String key = k != null ? new String(k, StandardCharsets.UTF_8) : "";
		byte[] value = serializationSchema.serializeValue(input);
		String partitionKey = serializationSchema.getPartitionKey(input);

		Preconditions.checkNotNull(topic, "the message topic is null");
		Preconditions.checkNotNull(value, "the message body is null");

		Message msg = new Message(topic, value);
		msg.setKeys(key);
		msg.setTags(tag);
		msg.setPartitionKey(partitionKey);

		int delayLevel = messageDelayLevel;
		if (msgDelayLevelSelector != null) {
			delayLevel = msgDelayLevelSelector.getDelayLevel(input);
		}
		if (delayLevel > MSG_DELAY_LEVEL00) {
			msg.setDelayLevel(delayLevel);
		}

		if (deferMillisSelector != null) {
			long millis = deferMillisSelector.getDeferMillis(input);
			if (millis < DEFER_MILLIS_MIN || millis > DEFER_MILLIS_MAX) {
				throw new FlinkRuntimeException(
					String.format("millis value %s not in range [%s, %s], origin row value is %s",
						millis, DEFER_MILLIS_MIN, DEFER_MILLIS_MAX, Arrays.toString(value)));
			}
			msg.setDeferMillis(millis);
			msg.setMessageType(MessageType.Defer);
			if (deferLoopSelector != null) {
				int loop = deferLoopSelector.getDeferLoop(input);
				if (loop < 0) {
					throw new FlinkRuntimeException(
						String.format("loop value %s not greater or equal 0, origin row value is %s",
							loop, Arrays.toString(value))
					);
				}
				msg.setDeferLoops(loop);
			}
		}

		return msg;
	}

	@Override
	public void close() throws Exception {
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduledFuture = null;
		}

		if (scheduler != null) {
			scheduler.shutdown();
			scheduler = null;
		}

		if (producer != null) {
			synchronized (messageList) {
				flushMessages();
			}
			producer.shutdown();
			producer = null;
		}
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}
}
