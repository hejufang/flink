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

package org.apache.flink.rocketmq.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.Preconditions;

import com.bytedance.rocketmq.clientv2.message.MessageQueue;

import java.util.Objects;

/**
 * RocketMQSplitBase.
 */
public class RocketMQSplitBase implements SourceSplit, Comparable<RocketMQSplitBase> {
	private final String topic;
	private final String brokerName;
	private final int queueId;

	public RocketMQSplitBase(String topic, String brokerName, int queueId) {
		Preconditions.checkNotNull(topic);
		Preconditions.checkNotNull(brokerName);
		this.topic = topic;
		this.brokerName = brokerName;
		this.queueId = queueId;
	}

	public String getTopic() {
		return topic;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public int getQueueId() {
		return queueId;
	}

	public static RocketMQSplitBase createRocketMQSplitBase(MessageQueue messageQueue) {
		return new RocketMQSplitBase(
			messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId());
	}

	@Override
	public String splitId() {
		return getSplitId(topic, brokerName, queueId);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RocketMQSplitBase)) {
			return false;
		}
		RocketMQSplitBase that = (RocketMQSplitBase) o;
		return queueId == that.queueId && Objects.equals(topic, that.topic) &&
			Objects.equals(brokerName, that.brokerName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topic, brokerName, queueId);
	}

	public RocketMQSplitBase getRocketMQBaseSplit() {
		return new RocketMQSplitBase(topic, brokerName, queueId);
	}

	@Override
	public int compareTo(RocketMQSplitBase o) {
		int res = this.brokerName.compareTo(o.brokerName);
		if (res != 0) {
			return res;
		}

		res = this.topic.compareTo(o.topic);
		if (res != 0) {
			return res;
		}
		return this.queueId - o.queueId;
	}

	@Override
	public String toString() {
		return "RocketMQSplitBase{" +
			"topic='" + topic + '\'' +
			", brokerName='" + brokerName + '\'' +
			", queueId=" + queueId +
			'}';
	}

	public static String getSplitId(String topic, String broker, int queueId) {
		return "MessageQueue [topic=" + topic + ", brokerName=" + broker + ", queueId=" + queueId + "]";
	}
}
