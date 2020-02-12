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

package org.apache.flink.streaming.connectors.rocketmq.strategy;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Assign message queues by parallelim num.
 */
public class AllocateMessageQueueStrategyParallelism implements AllocateMessageQueueStrategy {
	private static final Logger LOG =
		LoggerFactory.getLogger(AllocateMessageQueueStrategyParallelism.class);

	/**
	 * Flink Job parallelism num.
	 */
	private final int parallelismNum;

	/**
	 * Flink sub task id.
	 */
	private final int subTaskId;

	public AllocateMessageQueueStrategyParallelism(int parallelismNum, int subTaskId) {
		this.parallelismNum = parallelismNum;
		this.subTaskId = subTaskId;
		LOG.info("initialize strategy in parallelism num: {}, task id: {}.", parallelismNum, subTaskId);
	}

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> assignMessageQueues = new ArrayList<>();
		for (MessageQueue messageQueue : mqAll) {
			int startIndex = ((getUniqKey(messageQueue).hashCode() * 31) & 0x7FFFFFFF) % parallelismNum;
			int assignedSubTaskId = (startIndex + messageQueue.getQueueId()) % parallelismNum;
			if (assignedSubTaskId == subTaskId) {
				assignMessageQueues.add(messageQueue);
			}
		}
		return assignMessageQueues;
	}

	private static String getUniqKey(MessageQueue queue) {
		return queue.getTopic() + "." + queue.getBrokerName();
	}

	@Override
	public String getName() {
		return "Parallelism";
	}
}
