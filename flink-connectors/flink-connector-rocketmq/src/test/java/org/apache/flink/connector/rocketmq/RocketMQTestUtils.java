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

import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;

import java.util.ArrayList;
import java.util.List;

/**
 * RocketMQTestConstant.
 */
public class RocketMQTestUtils {
	public static final String TEST_CLUSTER_A = "clusterDefault";
	public static final String TEST_CONSUMER_GROUP_A = "consumerDefault";
	public static final String TEST_TOPIC_NAME_A = "topicDefault";
	public static final String[] TEST_BROKER_LIST_SIZE_2 =
		{"test_dc1_0", "test_dc2_0"};
	protected static final String[] TEST_BROKER_LIST_SIZE_4 =
		{"test_dc1_0", "test_dc1_1", "test_dc2_0", "test_dc2_1"};

	public static List<RocketMQSplitBase> createSplitListSize4(String topic) {
		return buildSplitList(topic, TEST_BROKER_LIST_SIZE_2, 2);
	}

	public static List<RocketMQSplitBase> buildSplitList(String topic, String[] brokerList, int endId) {
		List<RocketMQSplitBase> splitBaseList = new ArrayList<>();
		for (int i = 0; i < endId; i++) {
			for (String broker: brokerList) {
				splitBaseList.add(new RocketMQSplitBase(topic, broker, i));
			}
		}
		return splitBaseList;
	}
}
