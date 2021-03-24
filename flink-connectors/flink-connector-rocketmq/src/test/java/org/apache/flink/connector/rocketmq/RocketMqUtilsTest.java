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

import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RocketMqUtilsTest.
 */
public class RocketMqUtilsTest {
	@Test
	public void testParseCluster2QueueList() {
		String cluster = "cluster";
		String topic = "topic";
		String broker1 = "broker1";
		Map<String, List<MessageQueue>>  cluster2QueueList =
			RocketMQUtils.parseCluster2QueueList(String.format("%s:%s:%s:%s", cluster, topic, broker1, "0"));
		Set<MessageQueue> expectSet = createQueues(topic, broker1, 0, 0);
		Assert.assertEquals(new HashSet<>(cluster2QueueList.get(cluster)), expectSet);

		String broker2 = "broker2";
		String config1 = String.format("%s:%s:%s:%s", cluster, topic, broker1, "0");
		String config2 = String.format("%s:%s:%s:%s", cluster, topic, broker2, "0-1");
		cluster2QueueList =
			RocketMQUtils.parseCluster2QueueList(String.format("%s#%s", config1, config2));
		Set<MessageQueue> set1 = createQueues(topic, broker1, 0, 0);
		Set<MessageQueue> set2 = createQueues(topic, broker2, 0, 1);
		set1.addAll(set2);
		Assert.assertEquals(new HashSet<>(cluster2QueueList.get(cluster)), set1);
	}

	private Set<MessageQueue> createQueues(String topic, String broker, int start, int end) {
		Set<MessageQueue> set = new HashSet<>();
		for (int i = start; i <= end; i++) {
			set.add(new MessageQueue(topic, broker, i));
		}
		return set;
	}
}
