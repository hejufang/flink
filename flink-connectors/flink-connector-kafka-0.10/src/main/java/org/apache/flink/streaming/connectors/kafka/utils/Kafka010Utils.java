/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Provides Kafka010 Utils.
 */
public class Kafka010Utils {
	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializationSchema The de-/serializer used to convert
	 *                              between Kafka's byte messages and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		String topic,
		String consumerGroup, DeserializationSchema<T> deserializationSchema) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			deserializationSchema);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName, List<String> topics,
		String consumerGroup,
		DeserializationSchema<T> deserializationSchema) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		return new FlinkKafkaConsumer010<>(topics, deserializationSchema, properties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param originProperties      The properties of kafka client
	 * @param deserializationSchema The de-/serializer used to convert
	 *                              between Kafka's byte messages and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		String topic,
		String consumerGroup,
		Properties originProperties,
		DeserializationSchema<T> deserializationSchema) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			originProperties,
			deserializationSchema);
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
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName, List<String> topics,
		String consumerGroup,
		Properties originProperties,
		DeserializationSchema<T> deserializationSchema) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaConsumer010<>(topics, deserializationSchema, allProperties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		String topic,
		String consumerGroup,
		KeyedDeserializationSchema<T> deserializationSchema) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			deserializationSchema);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing multiple topics to the consumer.
	 *  </p>
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topics                The Kafka topics to read from.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		List<String> topics,
		String consumerGroup,
		KeyedDeserializationSchema<T> deserializationSchema) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		return new FlinkKafkaConsumer010<>(topics, deserializationSchema, properties);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param kafkaClusterName      The name of kafka cluster.
	 * @param topic                 The name of the topic that should be consumed.
	 * @param originProperties      The origin properties.
	 * @param consumerGroup         The consumer group that the consumer belongs to.
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		String topic,
		String consumerGroup,
		Properties originProperties,
		KeyedDeserializationSchema<T> deserializationSchema) {
		return customFlinkKafkaConsumer010(
			kafkaClusterName,
			Collections.singletonList(topic),
			consumerGroup,
			originProperties,
			deserializationSchema);
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
	 * @param deserializationSchema The de-/serializer used to convert between Kafka's byte messages
	 *                              and Flink's objects.
	 */
	public static <T> FlinkKafkaConsumer010<T> customFlinkKafkaConsumer010(
		String kafkaClusterName,
		List<String> topics,
		String consumerGroup,
		Properties originProperties,
		KeyedDeserializationSchema<T> deserializationSchema) {
		Properties properties = createKafkaConsumerConfig(kafkaClusterName, consumerGroup, topics);
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaConsumer010<>(topics, deserializationSchema, allProperties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema User defined (keyless) serialization schema.
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
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, allProperties);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema A (keyless) serializable serialization schema for
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
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties, customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param kafkaClusterName    The name of kafka cluster.
	 * @param topic               The topic to write data to.
	 * @param serializationSchema A (keyless) serializable serialization schema for
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
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, allProperties, customPartitioner);
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
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, allProperties);
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
		return new FlinkKafkaProducer010<>(topic, serializationSchema, properties, customPartitioner);
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
		Properties allProperties =  joinProperties(properties, originProperties);
		return new FlinkKafkaProducer010<>(topic, serializationSchema, allProperties, customPartitioner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param topic             The name of the topic that should be consumed.
	 * @param valueDeserializer The de-/serializer used to convert between Kafka's byte messages and
	 *                          Flink's objects.
	 * @param props             The properties used to configure the Kafka consumer client,
	 *                          and the ZooKeeper client.
	 */
	public static <T> FlinkKafkaConsumer010<T> flinkKafkaConsumer010(
		String topic,
		DeserializationSchema<T> valueDeserializer,
		Properties props) {
		return new FlinkKafkaConsumer010<>(topic, valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *  <p>This method allows passing a KeyedDeserializationSchema for reading key/value
	 * pairs, offsets, and topic names from Kafka.</p>
	 *
	 * @param topic        The name of the topic that should be consumed.
	 * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and
	 *                     Flink's objects.
	 * @param props        The properties used to configure the Kafka consumer client, and
	 *                     the ZooKeeper client.
	 */
	public static <T> FlinkKafkaConsumer010<T> flinkKafkaConsumer010(
		String topic,
		KeyedDeserializationSchema<T> deserializer,
		Properties props) {
		return new FlinkKafkaConsumer010<>(topic, deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 *  <p>This method allows passing multiple topics to the consumer.</p>
	 *
	 * @param topics       The Kafka topics to read from.
	 * @param deserializer The de-/serializer used to convert between Kafka's byte messages and
	 *                     Flink's objects.
	 * @param props        The properties that are used to configure both the fetcher and
	 *                     the offset handler.
	 */
	public static <T> FlinkKafkaConsumer010<T> flinkKafkaConsumer010(
		List<String> topics,
		DeserializationSchema<T> deserializer,
		Properties props) {
		return new FlinkKafkaConsumer010<>(topics, deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 *  <p>This method allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics       The Kafka topics to read from.
	 * @param deserializer The keyed de-/serializer used to convert between Kafka's byte messages and
	 *                     Flink's objects.
	 * @param props        The properties that are used to configure both the fetcher and
	 *                     the offset handler.
	 */
	public static <T> FlinkKafkaConsumer010<T> flinkKafkaConsumer010(
		List<String> topics,
		KeyedDeserializationSchema<T> deserializer,
		Properties props) {
		return new FlinkKafkaConsumer010<>(topics, deserializer, props);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList          Comma separated addresses of the brokers
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined (keyless) serialization schema.
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String brokerList,
		String topicId,
		SerializationSchema<IN>
			serializationSchema) {
		return new FlinkKafkaProducer010<>(brokerList, topicId, serializationSchema);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined (keyless) serialization schema.
	 * @param producerConfig      Properties with the producer configuration.
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		SerializationSchema<IN> serializationSchema,
		Properties producerConfig) {
		return new FlinkKafkaProducer010<>(topicId, serializationSchema, producerConfig);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId             The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user
	 *                            objects into a kafka-consumable byte[]
	 * @param producerConfig      Configuration properties for the KafkaProducer.
	 *                            'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner   A serializable partitioner for assigning messages to
	 *                            Kafka partitions (when passing null, we'll use Kafka's partitioner)
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		SerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		FlinkKafkaPartitioner<IN> customPartitioner) {
		return new FlinkKafkaProducer010<>(
			topicId,
			serializationSchema,
			producerConfig,
			customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList          Comma separated addresses of the brokers
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String brokerList,
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema) {
		return new FlinkKafkaProducer010<>(brokerList, topicId, serializationSchema);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 * @param producerConfig      Properties with the producer configuration.
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig) {
		return new FlinkKafkaProducer010<>(topicId, serializationSchema, producerConfig);
	}

	/**
	 * Creates Kafka producer
	 *
	 *  <p>This method does not allow writing timestamps to Kafka.
	 */
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		FlinkKafkaPartitioner<IN> customPartitioner) {
		return new FlinkKafkaProducer010<>(
			topicId,
			serializationSchema,
			producerConfig,
			customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId             The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user
	 *                            objects into a kafka-consumable byte[]
	 * @param producerConfig      Configuration properties for the KafkaProducer. 'bootstrap.servers.'
	 *                           is the only required argument.
	 * @param customPartitioner   A serializable partitioner for assigning messages to Kafka
	 *                            partitions (when passing null, we'll use Kafka's partitioner)
	 * @deprecated This is a deprecated since it does not correctly handle partitioning when
	 * producing to multiple topics. Use
	 * {@link FlinkKafkaProducer010#FlinkKafkaProducer010(String, SerializationSchema,
	 * Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		SerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		KafkaPartitioner<IN> customPartitioner) {
		return new FlinkKafkaProducer010<>(
			topicId,
			serializationSchema,
			producerConfig,
			customPartitioner);
	}

	/**
	 * Creates Kafka producer.
	 *
	 * <p>This method does not allow writing timestamps to Kafka, it follow approach (a) (see above)
	 *
	 * @deprecated This is a deprecated method that does not correctly handle partitioning when
	 * producing to multiple topics. Use
	 * {@link FlinkKafkaProducer010#FlinkKafkaProducer010(String, SerializationSchema,
	 * Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public static <IN> FlinkKafkaProducer010<IN> flinkKafkaProducer010(
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		KafkaPartitioner<IN> customPartitioner) {
		return new FlinkKafkaProducer010<>(
			topicId,
			serializationSchema,
			producerConfig,
			customPartitioner);
	}

	/**
	 * Creates a kafka consumer configuration for a given kafka cluster.
	 *
	 * @param kafkaClusterName The name of kafka cluster.
	 * @param consumerGroup    The consumer group that the consumer belongs to.
	 */
	private static Properties createKafkaConsumerConfig(String kafkaClusterName,
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

	/**
	 * Merge multi properties to one properties.
	 * @param properties
	 * @return
	 */
	private static Properties joinProperties(Properties... properties) {
		Properties joinedProperties = new Properties();
		for (Properties prop: properties) {
			if (prop != null && !prop.isEmpty()) {
				joinedProperties.putAll(prop);
			}
		}
		return joinedProperties;
	}
}
