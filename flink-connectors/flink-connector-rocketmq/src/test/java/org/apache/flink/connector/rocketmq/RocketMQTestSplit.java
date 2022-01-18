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

import java.util.Objects;

/**
 * RocketMQTestSplit.
 */
public class RocketMQTestSplit {
	/**
	 * RmqSplitType.
	 */
	public enum RmqSplitType {
		ENUMERATOR, SPLIT_READER, OLD_CONSUMER
	}

	public String cluster;
	public String topic;
	public String group;
	public int subTaskId;
	public RmqSplitType type;

	public RocketMQTestSplit(String cluster, String topic, String group, int subTaskId, RmqSplitType type) {
		this.cluster = cluster;
		this.topic = topic;
		this.group = group;
		this.subTaskId = subTaskId;
		this.type = type;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RocketMQTestSplit)) {
			return false;
		}
		RocketMQTestSplit that = (RocketMQTestSplit) o;
		return subTaskId == that.subTaskId && Objects.equals(cluster, that.cluster) &&
			Objects.equals(topic, that.topic) && Objects.equals(group, that.group) &&
			Objects.equals(type, that.type);
	}

	@Override
	public int hashCode() {
		return Objects.hash(cluster, topic, group, subTaskId, type);
	}

	@Override
	public String toString() {
		return "RocketMQTestSplit{" +
			"cluster='" + cluster + '\'' +
			", topic='" + topic + '\'' +
			", group='" + group + '\'' +
			", subTaskId=" + subTaskId +
			", type=" + type +
			'}';
	}
}
