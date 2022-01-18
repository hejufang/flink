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

import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.rocketmq.RocketMQTestSplit.RmqSplitType.ENUMERATOR;
import static org.apache.flink.connector.rocketmq.RocketMQTestSplit.RmqSplitType.OLD_CONSUMER;
import static org.apache.flink.connector.rocketmq.RocketMQTestSplit.RmqSplitType.SPLIT_READER;

/**
 * TestMQConsumerFactory.
 */
public class TestMQConsumerStaticFactory implements RocketMQConsumerFactory {

	private static Map<RocketMQTestSplit, DefaultMQPullConsumer> splitConsumerMap = new HashMap<>();

	@Override
	public DefaultMQPullConsumer createRocketMqConsumer(
			String cluster,
			String topic,
			String group,
			String instanceId,
			Properties properties) {
		synchronized (TestMQConsumerStaticFactory.class) {
			RocketMQTestSplit split = getRocketMQSplit(cluster, topic, group, instanceId);
			DefaultMQPullConsumer consumer = splitConsumerMap.get(split);
			Assert.assertNotNull(consumer);
			return consumer;
		}
	}

	public static void registerConsumer(RocketMQTestSplit split, DefaultMQPullConsumer consumer) {
		synchronized (TestMQConsumerStaticFactory.class) {
			splitConsumerMap.put(split, consumer);
		}
	}

	private RocketMQTestSplit getRocketMQSplit(
			String cluster,
			String topic,
			String group,
			String instanceName) {
		RocketMQTestSplit.RmqSplitType type;
		int subTaskId;
		if (instanceName.contains("Enumerator")) {
			type = ENUMERATOR;
			subTaskId = -1;
		} else if (instanceName.contains("Split")) {
			type = SPLIT_READER;
			subTaskId = Integer.valueOf(instanceName.split(":")[3].substring(7));
		} else {
			type = OLD_CONSUMER;
			String[] instanceSplits = instanceName.split("_");
			subTaskId = Integer.valueOf(instanceName.split("_")[instanceSplits.length - 2]);
		}
		return new RocketMQTestSplit(cluster, topic, group, subTaskId, type);
	}

	public static void clear() {
		synchronized (TestMQConsumerStaticFactory.class) {
			splitConsumerMap = new HashMap<>();
		}
	}
}
