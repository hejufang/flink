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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.BytedKafkaConfig;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010PartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka 0.10.x. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions.
 *
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once".
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 *
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka / ZooKeeper are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 *
 * <p>Please refer to Kafka's documentation for the available configuration properties:
 * http://kafka.apache.org/documentation.html#newconsumerconfigs</p>
 */
@PublicEvolving
public class FlinkKafkaConsumer010<T> extends FlinkKafkaConsumerBase<T> {

	private static final long serialVersionUID = 2324564345203409112L;

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer010.class);

	private static final Map<String, Object> DEFAULT_CONSUMER_CONFIG =
		ImmutableMap.of(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000,
						CommonClientConfigs.START_TIMEOUT_MS, 30000,
						CommonClientConfigs.ENABLE_ZTI_TOKEN, true,
						ConsumerConfig.IGNORE_DC_CHECK_CONFIG, true);

	/**  Configuration key to change the polling timeout. **/
	public static final String KEY_POLL_TIMEOUT = "flink.poll-timeout";

	/** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
	 * available. If 0, returns immediately with any records that are available now. */
	public static final long DEFAULT_POLL_TIMEOUT = 100L;

	// ------------------------------------------------------------------------

	/** User-supplied properties for Kafka. **/
	protected final Properties properties;

	/** From Kafka's Javadoc: The time, in milliseconds, spent waiting in poll if data is not
	 * available. If 0, returns immediately with any records that are available now */
	protected final long pollTimeout;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer010(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(Collections.singletonList(topic), valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * <p>This constructor allows passing a {@see KafkaDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer010(String topic, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(Collections.singletonList(topic), deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * <p>This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer010(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
		this(topics, new KafkaDeserializationSchemaWrapper<>(deserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer010(List<String> topics, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(topics, null, deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer010#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	@PublicEvolving
	public FlinkKafkaConsumer010(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(subscriptionPattern, new KafkaDeserializationSchemaWrapper<>(valueDeserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer010#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * <p>This constructor allows passing a {@see KafkaDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	@PublicEvolving
	public FlinkKafkaConsumer010(Pattern subscriptionPattern, KafkaDeserializationSchema<T> deserializer, Properties props) {
		this(null, subscriptionPattern, deserializer, props);
	}

	private FlinkKafkaConsumer010(
			@Nullable List<String> topics,
			@Nullable Pattern subscriptionPattern,
			KafkaDeserializationSchema<T> deserializer,
			Properties props) {

		super(
				topics,
				subscriptionPattern,
				deserializer,
				getLong(
					checkNotNull(props, "props"),
					KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_INTERVAL_DEFAULT),
				!getBoolean(props, KEY_DISABLE_METRICS, false), props);

		this.properties = props;
		addDefaultConfig(this.properties);

		// configure the polling timeout
		try {
			if (properties.containsKey(KEY_POLL_TIMEOUT)) {
				this.pollTimeout = Long.parseLong(properties.getProperty(KEY_POLL_TIMEOUT));
			} else {
				this.pollTimeout = DEFAULT_POLL_TIMEOUT;
			}
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Cannot parse poll timeout for '" + KEY_POLL_TIMEOUT + '\'', e);
		}

		int threshold = 10000;
		if (props.containsKey("threshold")) {
			threshold = Integer.parseInt(props.getProperty("threshold"));
		}
		String kafkaClusterName = props.getProperty("cluster");
		String groupId = props.getProperty("group.id");
		if (kafkaClusterName != null && !"".equals(kafkaClusterName)) {
			String kafkaMetricsStr = System.getProperty("flink_kafka_metrics", "[]");
			JSONParser parser = new JSONParser();
			try {
				JSONArray jsonArray = (JSONArray) parser.parse(kafkaMetricsStr);
				for (String topic: topics) {
					JSONObject jsonObject = new JSONObject();
					jsonObject.put("cluster", kafkaClusterName);
					jsonObject.put("topic", topic);
					jsonObject.put("consumer", groupId);
					jsonObject.put("threshold", threshold);
					jsonArray.add(jsonObject);
				}
				System.setProperty("flink_kafka_metrics", jsonArray.toJSONString());
			} catch (ParseException e) {
				LOG.error("Parse kafka metrics failed", e);
			}
		}
	}

	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
			StreamingRuntimeContext runtimeContext,
			OffsetCommitMode offsetCommitMode,
			MetricGroup consumerMetricGroup,
			boolean useMetrics,
			BytedKafkaConfig kafkaConfig) throws Exception {

		// make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
		// this overwrites whatever setting the user configured in the properties
		adjustAutoCommitConfig(properties, offsetCommitMode);

		// If a rateLimiter is set, then call rateLimiter.open() with the runtime context.
		if (rateLimiter != null) {
			rateLimiter.open(runtimeContext);
		}

		return new Kafka010Fetcher<>(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				watermarkStrategy,
				runtimeContext.getProcessingTimeService(),
				runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
				runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getTaskNameWithSubtasks(),
				deserializer,
				properties,
				pollTimeout,
				runtimeContext.getMetricGroup(),
				consumerMetricGroup,
				useMetrics,
				rateLimiter,
				kafkaConfig);
	}

	@Override
	protected AbstractPartitionDiscoverer createPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks) {
		properties.put(ConsumerConfig.TOPIC_NAME_CONFIG, topicsDescriptor.getFixedTopics());
		return new Kafka010PartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties, kafkaConsumerFactory);
	}

	@Override
	protected boolean getIsAutoCommitEnabled() {
		return getBoolean(properties, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true) &&
			PropertiesUtil.getLong(properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000) > 0;
	}

	@Override
	protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
			Collection<KafkaTopicPartition> partitions,
			long timestamp) {

		Map<TopicPartition, Long> partitionOffsetsRequest = new HashMap<>(partitions.size());
		for (KafkaTopicPartition partition : partitions) {
			partitionOffsetsRequest.put(
				new TopicPartition(partition.getTopic(), partition.getPartition()),
				timestamp);
		}

		final Map<KafkaTopicPartition, Long> result = new HashMap<>(partitions.size());

		// use a short-lived consumer to fetch the offsets;
		// this is ok because this is a one-time operation that happens only on startup
		try (KafkaConsumer<?, ?> consumer = new KafkaConsumer(properties)) {
			for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
				consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

				result.put(
					new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
					(partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
			}
		}

		return result;
	}

	private void addDefaultConfig(Properties props) {
		setDeserializer(props);
		DEFAULT_CONSUMER_CONFIG.forEach((key, value) -> {
			if (!properties.containsKey(key)) {
				LOG.info("Add default configuration: {} = {}", key, value);
				properties.put(key, value);
			}
		});
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Makes sure that the ByteArrayDeserializer is registered in the Kafka properties.
	 *
	 * @param props The Kafka properties to register the serializer in.
	 */
	private static void setDeserializer(Properties props) {
		final String deSerName = ByteArrayDeserializer.class.getName();

		Object keyDeSer = props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
		Object valDeSer = props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

		if (keyDeSer != null && !keyDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured key DeSerializer ({})", ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
		}
		if (valDeSer != null && !valDeSer.equals(deSerName)) {
			LOG.warn("Ignoring configured value DeSerializer ({})", ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
		}

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deSerName);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deSerName);
	}
}
