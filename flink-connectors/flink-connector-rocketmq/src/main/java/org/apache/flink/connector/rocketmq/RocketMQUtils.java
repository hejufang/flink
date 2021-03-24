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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RocketMQUtils.
 */
public final class RocketMQUtils {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQUtils.class);

	public static int getInteger(Properties props, String key, int defaultValue) {
		return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
	}

	public static long getLong(Map<String, String> props, String key, long defaultValue) {
		return Long.parseLong(props.getOrDefault(key, String.valueOf(defaultValue)));
	}

	public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
		return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
	}

	public static Map<String, List<MessageQueue>> parseCluster2QueueList(String brokerQueueList) {
		if (brokerQueueList == null) {
			/*
			 * It will be get from system environment in AbstractYarnClusterDescriptor#startAppMaster,
			 * and set it to container system in org.apache.flink.yarn.Utils
			 */
			brokerQueueList = System.getenv(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY);
		}
		try {
			return parseMessageQueues(brokerQueueList);
		} catch (FlinkRuntimeException e) {
			throw new FlinkRuntimeException(String.format("Broker queue list is: %s", brokerQueueList));
		}
	}

	private static Map<String, List<MessageQueue>> parseMessageQueues(String brokerQueueString) {
		final Map<String, List<MessageQueue>> cluster2MessageQueues = new HashMap<>();
		if (brokerQueueString == null) {
			return cluster2MessageQueues;
		}

		String[] brokerQueueInfoList = brokerQueueString.split("#");
		for (String brokerQueueInfoString: brokerQueueInfoList) {
			/*
			 * format is:
			 * ${cluster}:${topic}:${broker_name}:{queue_range}#${cluster}:${topic}:${broker_name}:{queue_range}
			 */
			String[] brokerQueueInfo = brokerQueueInfoString.split(":");
			if (brokerQueueInfo.length == 4) {
				String cluster = brokerQueueInfo[0];
				List<MessageQueue> curQueueList =
					cluster2MessageQueues.computeIfAbsent(cluster, k -> new ArrayList<>());
				String topic = brokerQueueInfo[1];
				String brokerName = brokerQueueInfo[2];
				curQueueList.addAll(parseQueueList(topic, brokerName, brokerQueueInfo[3]));
			} else {
				throw new FlinkRuntimeException(String.format("Invalid queue info string %s", brokerQueueInfoString));
			}
		}

		return cluster2MessageQueues;
	}

	private static List<MessageQueue> parseQueueList(String topic, String brokerName, String queueList) {
		return parseQueueRange(queueList)
			.map(queueId -> new MessageQueue(topic, brokerName, queueId)).collect(Collectors.toList());
	}

	private static Stream<Integer> parseQueueRange(String queueList) {
		return Arrays.stream(queueList.split(","))
			.flatMap(
				queueIdString -> {
					List<Integer> queueIds = new ArrayList<>();
					if (queueIdString.contains("-")) {
						String[] range = queueIdString.split("-");
						if (range.length != 2) {
							throw new FlinkRuntimeException(String.format("invalid range string %s", queueIdString));
						}
						int start = Integer.parseInt(range[0]);
						int end = Integer.parseInt(range[1]);
						for (int i = start; i <= end; i++) {
							queueIds.add(i);
						}
					} else {
						queueIds.add(Integer.parseInt(queueIdString));
					}
					return queueIds.stream();
				}
			);
	}
}
