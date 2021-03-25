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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.selector.MsgDelayLevelSelector;
import org.apache.flink.connector.rocketmq.selector.TopicSelector;
import org.apache.flink.connector.rocketmq.serialization.KeyValueSerializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import com.bytedance.rocketmq.clientv2.message.Message;
import com.bytedance.rocketmq.clientv2.producer.DefaultMQProducer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL00;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.getRocketMQProperties;

/**
 * RocketMQProducer.
 */
public class RocketMQProducer<T> extends RichSinkFunction<T> implements CheckpointedFunction, SpecificParallelism {
	private static final long serialVersionUID = 1L;

	private KeyValueSerializationSchema<T> serializationSchema;
	private final String cluster;
	private final String group;
	private final int batchSize;
	private transient List<Message> messageList;
	private Map<String, String> props;
	private final TopicSelector<T> topicSelector;
	private int messageDelayLevel;
	private int parallelism;
	private final MsgDelayLevelSelector<T> msgDelayLevelSelector;

	private transient DefaultMQProducer producer;

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
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		messageList = new ArrayList<>();
		// TODO: use props construct producer.
		producer = new DefaultMQProducer(cluster, group, null, getRocketMQProperties(props));
		producer.start();
		serializationSchema.open(() -> getRuntimeContext().getMetricGroup());
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		Message message = prepareMessage(value);
		messageList.add(message);
		if (messageList.size() >= batchSize) {
			producer.send(messageList);
			messageList.clear();
		}
	}

	private Message prepareMessage(T input) {
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
		return msg;
	}

	@Override
	public void close() throws Exception {
		if (producer != null) {
			producer.shutdown();
		}
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}
}
