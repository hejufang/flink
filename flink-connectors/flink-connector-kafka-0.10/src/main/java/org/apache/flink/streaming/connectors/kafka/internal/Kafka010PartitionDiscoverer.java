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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerFactory;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka 0.10 high-level consumer API.
 */
@Internal
public class Kafka010PartitionDiscoverer extends AbstractPartitionDiscoverer {

	private final Properties kafkaProperties;
	private final KafkaConsumerFactory kafkaConsumerFactory;

	private KafkaConsumer<?, ?> kafkaConsumer;

	public Kafka010PartitionDiscoverer(
		KafkaTopicsDescriptor topicsDescriptor,
		int indexOfThisSubtask,
		int numParallelSubtasks,
		Properties kafkaProperties,
		KafkaConsumerFactory kafkaConsumerFactory) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);
		this.kafkaProperties = checkNotNull(kafkaProperties);
		this.kafkaConsumerFactory = kafkaConsumerFactory;
	}

	@Override
	protected void initializeConnections() {
		try {
			this.kafkaConsumer = (KafkaConsumer<?, ?>) kafkaConsumerFactory.getConsumer(kafkaProperties);
		} catch (KafkaException e) {
			String propertiesMessage = kafkaProperties.entrySet().stream()
				.map(entry -> String.format("[key=%s,value=%s]", entry.getKey(), entry.getValue()))
				.collect(Collectors.joining(", "));
			throw new FlinkRuntimeException("Init KafkaConsumer failed. The properties : " + propertiesMessage, e);
		}
	}

	@Override
	protected List<String> getAllTopics() throws WakeupException {
		try {
			return new ArrayList<>(kafkaConsumer.listTopics().keySet());
		} catch (org.apache.kafka.common.errors.WakeupException e) {
			// rethrow our own wakeup exception
			throw new WakeupException();
		}
	}

	@Override
	protected List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) throws WakeupException, RuntimeException {
		List<KafkaTopicPartition> partitions = new LinkedList<>();

		try {
			for (String topic : topics) {
				// Currently, partitionsFor may return partial available partitions for some reason (e.g. network error)
				// set getOnlyAvailablePartitions == false to retrieve all partitions for given topic
				final List<PartitionInfo> kafkaPartitions = kafkaConsumer.partitionsFor(topic, false);

				if (kafkaPartitions == null) {
					throw new RuntimeException(String.format("Could not fetch partitions for %s. Make sure that the topic exists.", topic));
				}

				for (PartitionInfo partitionInfo : kafkaPartitions) {
					partitions.add(new KafkaTopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}
			}
		} catch (org.apache.kafka.common.errors.WakeupException e) {
			// rethrow our own wakeup exception
			throw new WakeupException();
		}

		return partitions;
	}

	@Override
	protected void wakeupConnections() {
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.wakeup();
		}
	}

	@Override
	protected void closeConnections() throws Exception {
		if (this.kafkaConsumer != null) {
			this.kafkaConsumer.close();

			// de-reference the consumer to avoid closing multiple times
			this.kafkaConsumer = null;
		}
	}
}
