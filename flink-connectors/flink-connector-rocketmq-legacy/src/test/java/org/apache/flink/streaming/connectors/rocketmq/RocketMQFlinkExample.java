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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.rocketmq.selector.DefaultTopicSelector;
import org.apache.flink.streaming.connectors.rocketmq.serialization.SimpleKeyValueDeserializationSchema;
import org.apache.flink.streaming.connectors.rocketmq.serialization.SimpleKeyValueSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RocketMQSourceExample.
 */
public class RocketMQFlinkExample {
	public static void main(String[] args) {
		final String rocketMQCluster = "dts.service.lq";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(3000);

		Properties consumerProps = new Properties();
		consumerProps.setProperty(RocketMQConfig.ROCKETMQ_NAMESRV_DOMAIN, rocketMQCluster);
		consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002");
		consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "to_hive_topic_2");

		Properties producerProps = new Properties();
		producerProps.setProperty(RocketMQConfig.ROCKETMQ_NAMESRV_DOMAIN, rocketMQCluster);
		int msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05;
		producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel));
		// TimeDelayLevel is not supported for batching
		boolean batchFlag = msgDelayLevel <= 0;

		env.addSource(new RocketMQSource<>(new SimpleKeyValueDeserializationSchema("key", "value"), consumerProps))
			.name("rocketmq-source")
			.setParallelism(1)
			.process(new ProcessFunction<Map, Map>() {
				@Override
				public void processElement(Map in, Context ctx, Collector<Map> out) throws Exception {
					HashMap result = new HashMap();
					result.put("key", in.get("key"));
					result.put("value", in.get("value"));
					out.collect(result);
				}
			})
			.name("upper-processor")
			.setParallelism(1)
			.addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema("key", "value"),
				new DefaultTopicSelector("dts_test_topic"), producerProps).withBatchFlushOnCheckpoint(batchFlag))
			.name("rocketmq-sink")
			.setParallelism(1);

		try {
			env.execute("rocketmq-flink-example");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
