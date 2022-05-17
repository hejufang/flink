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
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.bytedance.mqproxy.proto.MessageQueuePb;
import com.bytedance.mqproxy.proto.ResponseCode;
import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import com.bytedance.rocketmq.clientv2.consumer.QueryOffsetResult;
import com.bytedance.rocketmq.clientv2.consumer.ResetOffsetResult;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.LEGACY_OFFSETS_STATE_NAME;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_CONSUMER_OFFSET_FROM_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_EARLIEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_LATEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_TIMESTAMP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_LATEST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;

/**
 * RocketMQUtils.
 */
public final class RocketMQUtils {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQUtils.class);
	private static final String FLINK_ROCKETMQ_METRICS = "flink_rocketmq_metrics";

	public static int getInteger(Properties props, String key, int defaultValue) {
		return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
	}

	public static long getLong(Map<String, String> props, String key, long defaultValue) {
		return Long.parseLong(props.getOrDefault(key, String.valueOf(defaultValue)));
	}

	public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
		return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
	}

	public static Set<RocketMQSplitBase> getClusterSplitBaseSet(String cluster, String brokerQueueList) {
		Map<String, List<MessageQueue>> map = parseCluster2QueueList(brokerQueueList);
		if (map.isEmpty()) {
			return Collections.emptySet();
		}
		List<MessageQueue> queues = map.get(cluster);
		if (queues == null) {
			throw new FlinkRuntimeException(String.format(
				"Cluster: %s not in specific cluster set %s", cluster, map.keySet()));
		}

		return queues.stream()
				.map(q -> new RocketMQSplitBase(q.getTopic(), q.getBrokerName(), q.getQueueId()))
				.collect(Collectors.toSet());
	}

	public static Map<String, List<MessageQueue>> parseCluster2QueueList(String brokerQueueList) {
		if (brokerQueueList == null) {
			/*
			 * It will be get from system environment in AbstractYarnClusterDescriptor#startAppMaster,
			 * and set it to container system in org.apache.flink.yarn.Utils
			 */
			brokerQueueList = System.getProperty(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY);
			LOG.info("Got broker queue list {}", brokerQueueList);
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

	public static void validateResponse(int errorCode, String errMsg) {
		if (errorCode == ResponseCode.OK_VALUE) {
			return;
		}
		throw new FlinkRuntimeException(errMsg);
	}

	public static Long resetAndGetOffset(
			String topic,
			String group,
			MessageQueuePb messageQueuePb,
			Map<String, String> props,
			DefaultMQPullConsumer consumer) throws InterruptedException {
		List<MessageQueuePb> queuePbList = Arrays.asList(messageQueuePb);
		ResetOffsetResult resetOffsetResult;
		String startupMode = props.getOrDefault(SCAN_STARTUP_MODE.key(), SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS);

		QueryOffsetResult queryOffsetResult;
		synchronized (consumer) {
			switch (startupMode) {
				case SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS:
					queryOffsetResult = consumer.queryCommitOffset(topic, queuePbList);
					validateResponse(queryOffsetResult.getErrorCode(), queryOffsetResult.getErrorMsg());
					if (getOnlyOffset(queryOffsetResult.getOffsetMap()) < 0) {
						// We cannot get normal offset from RMQ.
						// Default setting is earliest, in case of data loss.
						String initialOffset =
							props.getOrDefault(RocketMQOptions.SCAN_CONSUMER_OFFSET_RESET_TO.key(), SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_EARLIEST);
						switch (initialOffset) {
							case SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_EARLIEST:
								resetOffsetResult = consumer.resetOffsetToEarliest(topic, group, queuePbList, false);
								LOG.info("Group offset not find, reset {} offset to earliest offset: {}",
									formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
								break;
							case SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_LATEST:
								resetOffsetResult = consumer.resetOffsetToLatest(topic, group, queuePbList, false);
								LOG.info("Group offset not find, reset {} offset to latest offset: {}",
									formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
								break;
							case SCAN_CONSUMER_OFFSET_RESET_TO_VALUE_TIMESTAMP:
								long timestamp = RocketMQUtils.getLong(props,
									SCAN_CONSUMER_OFFSET_FROM_TIMESTAMP_MILLIS.key(), System.currentTimeMillis());
								resetOffsetResult = consumer.resetOffsetByTimestamp(topic, group, queuePbList, timestamp, false);
								LOG.info("Group offset not find, reset {} offset to timestamp offset: {}",
									formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
								break;
							default:
								throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
						}
						validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
					} else {
						LOG.info("Get {} group offset {}", formatQueue(messageQueuePb),
							getOnlyOffset(queryOffsetResult.getOffsetMap()));
						return queryOffsetResult.getOffsetMap().get(messageQueuePb);
					}
					break;
				case SCAN_STARTUP_MODE_VALUE_EARLIEST:
					resetOffsetResult = consumer.resetOffsetToEarliest(
						topic, group, queuePbList, false);
					validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
					LOG.info("Group offset not find, reset {} offset to earliest offset: {}",
						formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
					break;
				case SCAN_STARTUP_MODE_VALUE_LATEST:
					resetOffsetResult = consumer.resetOffsetToLatest(topic, group, queuePbList, false);
					validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
					LOG.info("Reset {} offset to latest offset: {}",
						formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
					break;
				case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
					long timestamp = RocketMQUtils.getLong(props,
						SCAN_STARTUP_TIMESTAMP_MILLIS.key(), System.currentTimeMillis());
					resetOffsetResult = consumer.resetOffsetByTimestamp(
						topic, group, queuePbList, timestamp, false);
					validateResponse(resetOffsetResult.getErrorCode(), resetOffsetResult.getErrorMsg());
					LOG.info("Reset {} offset to timestamp offset: {}",
						formatQueue(messageQueuePb), getOnlyOffset(resetOffsetResult.getResetOffsetMap()));
					break;
				default:
					throw new IllegalArgumentException("Unknown value for startup-mode: " + startupMode);
			}
		}

		return resetOffsetResult.getResetOffsetMap().get(messageQueuePb);
	}

	public static String formatQueue(MessageQueuePb messageQueuePb) {
		return String.format("Queue[topic: %s, broker: %s, queue: %s]",
			messageQueuePb.getTopic(), messageQueuePb.getBrokerName(), messageQueuePb.getQueueId());
	}

	// keep same logic with flink-connector-rocketmq-legacy #AllocateMessageQueueStrategyParallelism#allocate()
	public static int hashCodeOfMessageQueue(MessageQueue messageQueue, int runtimeParallelism) {
		int startIndex = ((messageQueue.toString().hashCode() * 31) & 0x7FFFFFFF) % runtimeParallelism;
		return  (startIndex + messageQueue.getQueueId()) % runtimeParallelism;
	}

	public static long getOnlyOffset(Map<MessageQueuePb, Long> offsetMap) {
		return offsetMap.entrySet().iterator().next().getValue();
	}

	/**
	 * Save rocketmq config to system properties, so we can use it when register the dashboard.
	 * See {@link org.apache.flink.monitor.Dashboard}.
	 */
	public static <T> void saveConfigurationToSystemProperties(RocketMQConfig<T> rocketMQConfig) {
		try {
			String cluster = rocketMQConfig.getCluster();
			String topic = rocketMQConfig.getTopic();
			String consumerGroup = rocketMQConfig.getGroup();
			String rocketMQMetricsStr = System.getProperty(FLINK_ROCKETMQ_METRICS, "[]");
			ObjectMapper objectMapper = new ObjectMapper();
			ArrayNode arrayNode = (ArrayNode) objectMapper.readTree(rocketMQMetricsStr);
			ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
			objectNode.put("cluster", cluster);
			objectNode.put("topic", topic);
			objectNode.put("consumer_group", consumerGroup);
			arrayNode.add(objectNode);
			System.setProperty(FLINK_ROCKETMQ_METRICS, arrayNode.toString());
		} catch (Throwable t) {
			// We catch all Throwable as it is not critical path.
			LOG.warn("Parse rocketmq metrics failed.", t);
		}
	}

	public static void validateAndSetBrokerQueueList(RocketMQConfig<?> rocketMQConfig) {
		if (rocketMQConfig.getRocketMqBrokerQueueList() == null) {
			rocketMQConfig.setRocketMqBrokerQueueList(
				System.getProperty(ConfigConstants.ROCKETMQ_BROKER_QUEUE_LIST_KEY));
		}
		validateBrokerQueueList(rocketMQConfig);
	}

	public static void validateBrokerQueueList(RocketMQConfig<?> rocketMQConfig) {
		Map<String, List<MessageQueue>> messageQueueMap =
			RocketMQUtils.parseCluster2QueueList(rocketMQConfig.getRocketMqBrokerQueueList());
		LOG.info("Validate broker size {}", messageQueueMap.size());
		if (!messageQueueMap.isEmpty() && messageQueueMap.get(rocketMQConfig.getCluster()) == null) {
			throw new FlinkRuntimeException(
				String.format("Cluster %s not found in broker queue config", rocketMQConfig.getCluster()));
		}
	}

	public static List<Tuple2<MessageQueue, Long>> getDtsState(OperatorStateStore stateStore) throws Exception {
		List<Tuple2<MessageQueue, Long>> tuple2List = new ArrayList<>();

		// ************************* DTS State Compatibility *******************************
		// add legacy states into current version
		ListState<Tuple2<org.apache.rocketmq.common.message.MessageQueue, Long>> legacyUnionOffsetStates = stateStore
			.getUnionListState(new ListStateDescriptor<>(LEGACY_OFFSETS_STATE_NAME, TypeInformation.of(
				new TypeHint<Tuple2<org.apache.rocketmq.common.message.MessageQueue, Long>>() {
				})));

		for (Tuple2<org.apache.rocketmq.common.message.MessageQueue, Long> legacyState : legacyUnionOffsetStates.get()) {
			org.apache.rocketmq.common.message.MessageQueue legacyQueue = legacyState.f0;
			tuple2List.add(Tuple2.of(
				new MessageQueue(legacyQueue.getTopic(), legacyQueue.getBrokerName(), legacyQueue.getQueueId()),
				legacyState.f1));
		}

		legacyUnionOffsetStates.clear();
		// ************************* DTS State Compatibility *******************************
		return tuple2List;
	}
}
