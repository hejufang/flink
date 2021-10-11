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
import org.apache.flink.connector.rocketmq.RocketMQOptions.AssignQueueStrategy;
import org.apache.flink.connector.rocketmq.selector.DeferLoopSelector;
import org.apache.flink.connector.rocketmq.selector.DeferMillisSelector;
import org.apache.flink.connector.rocketmq.selector.MsgDelayLevelSelector;
import org.apache.flink.connector.rocketmq.selector.TopicSelector;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL_DEFAULT;

/**
 * RocketMQConfig.
 */
public class RocketMQConfig<T> {
	private MsgDelayLevelSelector<T> msgDelayLevelSelector;
	private TopicSelector<T> topicSelector;
	private String cluster;
	private String group;
	private String topic;
	private int delayLevel = MSG_DELAY_LEVEL_DEFAULT;
	private DeferMillisSelector<T> deferMillisSelector;
	private DeferLoopSelector<T> deferLoopSelector;
	private String tag;
	private int sendBatchSize;
	private AssignQueueStrategy assignQueueStrategy = AssignQueueStrategy.FIXED;
	private int[] keyByFields;
	private Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;
	private int parallelism = FactoryUtil.PARALLELISM.defaultValue();
	private String rocketMqBrokerQueueList;
	private FlinkConnectorRateLimiter rateLimiter;
	private long idleTimeOut = Long.MAX_VALUE;

	public MsgDelayLevelSelector<T> getMsgDelayLevelSelector() {
		return msgDelayLevelSelector;
	}

	public void setMsgDelayLevelSelector(MsgDelayLevelSelector<T> msgDelayLevelSelector) {
		this.msgDelayLevelSelector = msgDelayLevelSelector;
	}

	public TopicSelector<T> getTopicSelector() {
		return topicSelector;
	}

	public void setTopicSelector(TopicSelector<T> topicSelector) {
		this.topicSelector = topicSelector;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public int getDelayLevel() {
		return delayLevel;
	}

	public void setDelayLevel(int delayLevel) {
		if (delayLevel < RocketMQOptions.MSG_DELAY_LEVEL00 || delayLevel > RocketMQOptions.MSG_DELAY_LEVEL18) {
			throw new FlinkRuntimeException(String.format("Delay level %s is out of valid range [0, 18]", delayLevel));
		}
		this.delayLevel = delayLevel;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getSendBatchSize() {
		return sendBatchSize;
	}

	public void setSendBatchSize(int sendBatchSize) {
		this.sendBatchSize = sendBatchSize;
	}

	public AssignQueueStrategy getAssignQueueStrategy() {
		return assignQueueStrategy;
	}

	public void setAssignQueueStrategy(AssignQueueStrategy assignQueueStrategy) {
		this.assignQueueStrategy = assignQueueStrategy;
	}

	public int[] getKeyByFields() {
		return keyByFields;
	}

	public void setKeyByFields(int[] keyByFields) {
		this.keyByFields = keyByFields;
	}

	public Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> getMetadataMap() {
		return metadataMap;
	}

	public void setMetadataMap(Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
		this.metadataMap = metadataMap;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public String getRocketMqBrokerQueueList() {
		return rocketMqBrokerQueueList;
	}

	public void setRocketMqBrokerQueueList(String rocketMqBrokerQueueList) {
		this.rocketMqBrokerQueueList = rocketMqBrokerQueueList;
	}

	public void setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
		this.rateLimiter = rateLimiter;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public long getIdleTimeOut() {
		return idleTimeOut;
	}

	public void setIdleTimeOut(long idleTimeOut) {
		this.idleTimeOut = idleTimeOut;
	}

	public DeferMillisSelector<T> getDeferMillisSelector() {
		return deferMillisSelector;
	}

	public RocketMQConfig<T> setDeferMillisSelector(DeferMillisSelector<T> deferMillisSelector) {
		this.deferMillisSelector = deferMillisSelector;
		return this;
	}

	public DeferLoopSelector<T> getDeferLoopSelector() {
		return deferLoopSelector;
	}

	public RocketMQConfig<T> setDeferLoopSelector(DeferLoopSelector<T> deferLoopSelector) {
		this.deferLoopSelector = deferLoopSelector;
		return this;
	}
}
