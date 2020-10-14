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

package org.apache.flink.streaming.connectors.kafka.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Provides ByteDance Kafka010 Utils.
 */
public class Kafka010Utils {
	/**
	 * Creates a new  Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializer 			The de-/serializer used to convert
	 *                              between Kafka's byte messages and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			String topic,
			String consumerGroup,
			DeserializationSchema<T> deserializer) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			deserializer);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			List<String> topics,
			String consumerGroup,
			DeserializationSchema<T> deserializer) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		return new FlinkKafkaConsumer010<>(topics, deserializer, properties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param originProperties      The properties of kafka client
	 * @param deserializer 			The de-/serializer used to convert
	 *                              between Kafka's byte messages and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			String topic,
			String consumerGroup,
			Properties originProperties,
			DeserializationSchema<T> deserializer) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			originProperties,
			deserializer);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param originProperties      The origin properties.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			List<String> topics,
			String consumerGroup,
			Properties originProperties,
			DeserializationSchema<T> deserializer) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		properties.putAll(originProperties);
		return new FlinkKafkaConsumer010<>(topics, deserializer, properties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			String topic,
			String consumerGroup,
			KafkaDeserializationSchema<T> deserializer) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			deserializer);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			List<String> topics,
			String consumerGroup,
			KafkaDeserializationSchema<T> deserializer) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		return new FlinkKafkaConsumer010<>(topics, deserializer, properties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param originProperties      The origin properties.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			String topic,
			String consumerGroup,
			Properties originProperties,
			KafkaDeserializationSchema<T> deserializer) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			originProperties,
			deserializer);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>
	 *      This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param originProperties      The origin properties.
	 * @param deserializer 			The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
			String kafkaClusterName,
			List<String> topics,
			String consumerGroup,
			Properties originProperties,
			KafkaDeserializationSchema<T> deserializer) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		properties.putAll(originProperties);
		return new FlinkKafkaConsumer010<>(topics, deserializer, properties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema User defined key-less serialization schema.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			SerializationSchema<T> serializationSchema) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param originProperties    The origin properties.
	 * @param serializationSchema User defined (keyless) serialization schema.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			Properties originProperties,
			SerializationSchema<T> serializationSchema) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		properties.putAll(originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema A (key-less) serializable serialization schema for
	 *                            turning user objects into a kafka-consumable byte[].
	 * @param customPartitioner   A serializable partitioner for assigning messages to
	 *                            Kafka partitions (when passing null, we'll use Kafka's partitioner).
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			SerializationSchema<T> serializationSchema,
			FlinkKafkaPartitioner<T> customPartitioner) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		return new FlinkKafkaProducer010<>(
			topic,
			serializationSchema,
			properties,
			customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema A (key-less) serializable serialization schema for
	 *                            turning user objects into a kafka-consumable byte[].
	 * @param originProperties    The origin properties.
	 * @param customPartitioner   A serializable partitioner for assigning messages to
	 *                            Kafka partitions (when passing null, we'll use Kafka's partitioner).
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			SerializationSchema<T> serializationSchema,
			Properties originProperties,
			FlinkKafkaPartitioner<T> customPartitioner) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		properties.putAll(originProperties);
		return new FlinkKafkaProducer010<>(
			topic,
			serializationSchema,
			properties,
			customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema User defined serialization schema supporting key/value messages.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			KeyedSerializationSchema<T> serializationSchema) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param originProperties    The origin properties.
	 * @param serializationSchema User defined serialization schema supporting key/value messages.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			Properties originProperties,
			KeyedSerializationSchema<T> serializationSchema) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		properties.putAll(originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties);
	}

	/**
	 * Creates Kafka producer.
	 *  <p>This method does not allow writing timestamps to Kafka</p>
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema User defined serialization schema supporting key/value messages.
	 * @param customPartitioner   A serializable partitioner for
	 *                            assigning messages to Kafka partitions.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			KeyedSerializationSchema<T> serializationSchema,
			FlinkKafkaPartitioner<T> customPartitioner) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		return new FlinkKafkaProducer010<>(
			topic,
			serializationSchema,
			properties,
			customPartitioner);
	}

	/**
	 * Creates Kafka producer.
	 *  <p>This method does not allow writing timestamps to Kafka</p>
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param originProperties    The origin properties.
	 * @param serializationSchema User defined serialization schema supporting key/value messages.
	 * @param customPartitioner   A serializable partitioner for
	 *                            assigning messages to Kafka partitions.
	 */
	public static <T> FlinkKafkaProducer010<T> customFlinkKafkaProducer010(
			String kafkaClusterName,
			String topic,
			Properties originProperties,
			KeyedSerializationSchema<T> serializationSchema,
			FlinkKafkaPartitioner<T> customPartitioner) {
		Properties properties = createKafkaProducerConfig(kafkaClusterName);
		properties.putAll(originProperties);
		return new FlinkKafkaProducer010<>(
			topic,
			serializationSchema,
			properties,
			customPartitioner);
	}

	/**
	 * Creates a kafka consumer configuration for a given kafka cluster.
	 *
	 * @param kafkaClusterName The name of kafka cluster.
	 * @param consumerGroup    The consumer group that the consumer belongs to.
	 * @param topics           The topics that the consumer will consume.
	 *
	 */
	private static Properties createKafkaConsumerConfig(
			String kafkaClusterName,
			String consumerGroup,
			List<String> topics) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.CLUSTER_NAME_CONFIG, kafkaClusterName);
		String topicList = "".join(",", topics);
		properties.put(ConsumerConfig.TOPIC_NAME_CONFIG, topicList);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
		return properties;
	}

	/**
	 * Creates a kafka producer configuration for a given kafka cluster.
	 *
	 * @param kafkaClusterName The name of kafka cluster.
	 */
	private static Properties createKafkaProducerConfig(String kafkaClusterName) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.CLUSTER_NAME_CONFIG, kafkaClusterName);
		return properties;
	}
}
