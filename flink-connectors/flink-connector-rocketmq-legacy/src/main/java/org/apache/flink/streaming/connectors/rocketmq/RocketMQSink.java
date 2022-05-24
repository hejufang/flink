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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.rocketmq.selector.MsgDelayLevelSelector;
import org.apache.flink.streaming.connectors.rocketmq.selector.TopicSelector;
import org.apache.flink.streaming.connectors.rocketmq.serialization.KeyValueSerializationSchema;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.BATCH_SIZE_DEFAULT;

/**
 * The RocketMQSink provides at-least-once reliability guarantees when
 * checkpoints are enabled and batchFlushOnCheckpoint(true) is set.
 * Otherwise, the sink reliability guarantees depends on rocketmq producer's retry policy.
 */
public class RocketMQSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final long serialVersionUID = 2L;

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);

	private transient DefaultMQProducer producer;
	private boolean async; // false by default

	private Properties props;
	private TopicSelector<IN> topicSelector;
	private KeyValueSerializationSchema<IN> serializationSchema;

	private boolean batchFlushOnCheckpoint; // false by default
	private int batchSize = BATCH_SIZE_DEFAULT;
	private List<Message> batchList;

	private int messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL_DEFAULT;
	private MsgDelayLevelSelector<IN> msgDelayLevelSelector;

	/** Errors encountered in the async producer are stored here. */
	private transient volatile Throwable asyncError;

	public RocketMQSink(
		boolean async,
		Properties props,
		TopicSelector<IN> topicSelector,
		KeyValueSerializationSchema<IN> serializationSchema,
		boolean batchFlushOnCheckpoint,
		int batchSize,
		int messageDeliveryDelayLevel) {
		this.async = async;
		this.props = props;
		this.topicSelector = topicSelector;
		this.serializationSchema = serializationSchema;
		this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
		this.batchSize = batchSize;
		this.messageDeliveryDelayLevel = messageDeliveryDelayLevel;
	}

	public RocketMQSink(KeyValueSerializationSchema<IN> schema, TopicSelector<IN> topicSelector, Properties props) {
		this.serializationSchema = schema;
		this.topicSelector = topicSelector;
		this.props = props;

		if (this.props != null) {
			this.messageDeliveryDelayLevel = RocketMQUtils.getInteger(this.props, RocketMQConfig.MSG_DELAY_LEVEL,
				RocketMQConfig.MSG_DELAY_LEVEL00);
			if (this.messageDeliveryDelayLevel < RocketMQConfig.MSG_DELAY_LEVEL00) {
				this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL00;
			} else if (this.messageDeliveryDelayLevel > RocketMQConfig.MSG_DELAY_LEVEL18) {
				this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL18;
			}
		}
	}

	public RocketMQSink(KeyValueSerializationSchema<IN> schema, TopicSelector<IN> topicSelector,
						MsgDelayLevelSelector<IN> msgDelayLevelSelector,
						Properties props, RocketMQOptions options) {
		this(schema, topicSelector, props);
		this.batchSize = options.getBatchSize();
		this.async = options.isAsync();
		this.batchFlushOnCheckpoint = options.isBatchFlushOnCheckpoint();
		this.msgDelayLevelSelector = msgDelayLevelSelector;
	}

	@Override
	public void open(Configuration parameters) {
		RocketMQUtils.setLog(props);
		Preconditions.checkArgument(!props.isEmpty(), "Producer properties can not be empty");
		Preconditions.checkNotNull(topicSelector, "TopicSelector can not be null");
		Preconditions.checkNotNull(serializationSchema, "KeyValueSerializationSchema can not be null");

		String cluster = props.getProperty(RocketMQConfig.ROCKETMQ_NAMESRV_DOMAIN);
		Preconditions.checkNotNull(cluster, "Cluster can not be null");

		producer = new DefaultMQProducer(RocketMQConfig.buildAclRPCHook(props));
		producer.setCluster(cluster);
		producer.setInstanceName(getRuntimeContext().getIndexOfThisSubtask() + "_" + UUID.randomUUID());
		RocketMQConfig.buildProducerConfigs(props, producer);

		batchList = new LinkedList<>();

		if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			batchFlushOnCheckpoint = false;
		}

		try {
			producer.start();
		} catch (MQClientException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void invoke(IN input, Context context) throws Exception {
		checkErrors();
		Message msg = prepareMessage(input);

		if (batchFlushOnCheckpoint) {
			batchList.add(msg);
			if (batchList.size() >= batchSize) {
				flushSync();
			}
			return;
		}

		if (async) {
			try {
				producer.send(msg, new SendCallback() {
					@Override
					public void onSuccess(SendResult sendResult) {
						LOG.debug("Async send message success! result: {}", sendResult);
					}

					@Override
					public void onException(Throwable throwable) {
						if (throwable != null) {
							LOG.error("Async send message failure!", throwable);
							asyncError = throwable;
						}
					}
				});
			} catch (Exception e) {
				asyncError = e;
				LOG.error("Async send message failure!", e);
			}
		} else {
			try {
				SendResult result = producer.send(msg);
				LOG.debug("Sync send message result: {}", result);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Sync send message failure!", e);
			}
		}
	}

	protected Message prepareMessage(IN input) {
		String topic = topicSelector.getTopic(input);
		String tag = (tag = topicSelector.getTag(input)) != null ? tag : "";

		byte[] k = serializationSchema.serializeKey(input);
		String key = k != null ? new String(k, StandardCharsets.UTF_8) : "";
		byte[] value = serializationSchema.serializeValue(input);

		Preconditions.checkNotNull(topic, "the message topic is null");
		Preconditions.checkNotNull(value, "the message body is null");

		Message msg = new Message(topic, tag, key, value);
		int delayLevel = this.messageDeliveryDelayLevel;
		if (msgDelayLevelSelector != null) {
			delayLevel = msgDelayLevelSelector.getDelayLevel(input);
		}
		if (delayLevel > RocketMQConfig.MSG_DELAY_LEVEL00) {
			msg.setDelayTimeLevel(delayLevel);
		}
		return msg;
	}

	public RocketMQSink<IN> withAsync(boolean async) {
		this.async = async;
		return this;
	}

	public RocketMQSink<IN> withBatchFlushOnCheckpoint(boolean batchFlushOnCheckpoint) {
		this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
		return this;
	}

	public RocketMQSink<IN> withBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	private void checkErrors() throws Exception {
		Throwable t = asyncError;
		if (t != null) {
			// prevent double throwing
			asyncError = null;
			throw new Exception("Failed to send data to RocketMQ: " + t.getMessage(), t);
		}
	}

	@Override
	public void close() throws Exception {
		if (producer != null) {
			try {
				flushSync();
				checkErrors();
			} finally {
				// make sure producer can be shutdown, thus current producerGroup will be unregistered
				producer.shutdown();
			}
		}
	}

	private void flushSync() throws Exception {
		if (batchFlushOnCheckpoint) {
			synchronized (batchList) {
				if (batchList.size() > 0) {
					producer.send(batchList);
					batchList.clear();
				}
			}
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		flushSync();
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// Nothing to do
	}
}
