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
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010PartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.FeatureStoreSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.types.Row;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

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
public class FlinkKafkaConsumer010<T> extends FlinkKafkaConsumer09<T> {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer010.class);
	private static final long serialVersionUID = 2324564345203409112L;

	private final long manualCommitInterval;
	private Map<Integer, Metadata> fieldToMetadataMap;
	private DeserializationSchema<T> deserializationSchemaWithoutMetadata;

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
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x.
	 *
	 * <p>This constructor allow consumer to build a output row with kafka message metadata.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The deserializer used to resolve the real schema which includes kafka message metadata.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 * @param fieldToMetadataMap
	 *           Mapping of schema field indices and {{@link Metadata}}.
	 * @param deserializationSchemaWithoutMetadata
	 *           The deserializer used to convert between Kafka's byte messages and Flink's objects.
	 */
	public FlinkKafkaConsumer010(String topic, DeserializationSchema<T> valueDeserializer, Properties props, Map<Integer, Metadata> fieldToMetadataMap, DeserializationSchema<T> deserializationSchemaWithoutMetadata) {
		this(Collections.singletonList(topic), valueDeserializer, props);
		this.fieldToMetadataMap = fieldToMetadataMap;
		this.deserializationSchemaWithoutMetadata = deserializationSchemaWithoutMetadata;
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
		this(topics, (KafkaDeserializationSchemaWrapper<T>) getDeserializationSchema(deserializer, props), props);
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
		super(topics, deserializer, props);
		String kafkaClusterName = props.getProperty("cluster");
		int threshold = 10000;
		if (props.containsKey("threshold")) {
			threshold = Integer.parseInt(props.getProperty("threshold"));
		}
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

		manualCommitInterval = PropertiesUtil.getLong(props,
				KEY_MANUAL_COMMIT_OFFSETS_INTERVAL_MILLIS, -1);
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
		super(subscriptionPattern, deserializer, props);
		manualCommitInterval = -1;
		fieldToMetadataMap = null;
	}

	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			OffsetCommitMode offsetCommitMode,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {

		// make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS;
		// this overwrites whatever setting the user configured in the properties
		adjustAutoCommitConfig(properties, offsetCommitMode);

		FlinkConnectorRateLimiter rateLimiter = super.getRateLimiter();
		// If a rateLimiter is set, then call rateLimiter.open() with the runtime context.
		if (rateLimiter != null) {
			rateLimiter.open(runtimeContext);
		}

		KafkaDeserializationSchema<T> realDeserializer = deserializationSchemaWithoutMetadata == null ? deserializer :
			new KafkaDeserializationSchemaWrapper<>(deserializationSchemaWithoutMetadata);

		return new Kafka010Fetcher<>(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				runtimeContext.getProcessingTimeService(),
				runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
				runtimeContext.getUserCodeClassLoader(),
				runtimeContext.getTaskNameWithSubtasks(),
				realDeserializer,
				properties,
				pollTimeout,
				runtimeContext.getMetricGroup(),
				consumerMetricGroup,
				useMetrics,
				rateLimiter,
				rateLimitingUnit,
				sampleInterval,
				sampleNum,
				manualCommitInterval,
				fieldToMetadataMap
			);
	}

	@Override
	protected AbstractPartitionDiscoverer createPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks) {

		return new Kafka010PartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, properties);
	}

	// ------------------------------------------------------------------------
	//  Timestamp-based startup
	// ------------------------------------------------------------------------

	@Override
	public FlinkKafkaConsumerBase<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
		// the purpose of this override is just to publicly expose the method for Kafka 0.10+;
		// the base class doesn't publicly expose it since not all Kafka versions support the functionality
		return super.setStartFromTimestamp(startupOffsetsTimestamp);
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
		try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(properties)) {
			for (Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset :
				consumer.offsetsForTimes(partitionOffsetsRequest).entrySet()) {

				result.put(
					new KafkaTopicPartition(partitionToOffset.getKey().topic(), partitionToOffset.getKey().partition()),
					(partitionToOffset.getValue() == null) ? null : partitionToOffset.getValue().offset());
			}
		}

		return result;
	}

	@Override
	protected Map<String, Object> getDefaultConfig() {
		return ImmutableMap.of(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000,
			CommonClientConfigs.START_TIMEOUT_MS, 30000,
			CommonClientConfigs.ENABLE_ZTI_TOKEN, true);
	}

	private static KafkaDeserializationSchemaWrapper getDeserializationSchema(DeserializationSchema valueDeserializer, Properties props) {
		if (props.containsKey(ConsumerConfig.PARQUET_SELECT_COLUMNS_CONIFG)) {
			return new FeatureStoreSchemaWrapper((DeserializationSchema<Row>) valueDeserializer);
		} else {
			return new KafkaDeserializationSchemaWrapper(valueDeserializer);
		}
	}
}
