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

import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.rocketmq.clientv2.consumer.DefaultMQPullConsumer;

import java.io.Serializable;
import java.util.Properties;

/**
 * RocketMQConsumerFactory.
 */
public interface RocketMQConsumerFactory extends Serializable {
	DefaultMQPullConsumer createRocketMqConsumer(
		String cluster,
		String topic,
		String group,
		String instanceId,
		Properties properties);

	static RocketMQConsumerFactory getFactoryByClassName(String factoryClassName) {
		try {
			return ((Class<RocketMQConsumerFactory>) Class.forName(factoryClassName)).newInstance();
		} catch (Exception e) {
			throw new FlinkRuntimeException("Invalid factory class name" + factoryClassName, e);
		}
	}
}
