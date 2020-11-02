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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * A version-agnostic Kafka {@link ScanTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaDynamicSourceBase implements ScanTableSource {

	// --------------------------------------------------------------------------------------------
	// Common attributes
	// --------------------------------------------------------------------------------------------
	protected final DataType outputDataType;

	// --------------------------------------------------------------------------------------------
	// Scan format attributes
	// --------------------------------------------------------------------------------------------

	/** Scan format for decoding records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topic to consume. */
	protected final String topic;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	/** Properties for the flink source. */
	protected final Properties otherProperties;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	protected final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	protected final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** The start timestamp to locate partition offsets; only relevant when startup mode is {@link StartupMode#TIMESTAMP}.*/
	protected final long startupTimestampMillis;

	/** The default value when startup timestamp is not used.*/
	private static final long DEFAULT_STARTUP_TIMESTAMP_MILLIS = 0L;

	protected KafkaDynamicSourceBase(
		DataType outputDataType,
		String topic,
		Properties properties,
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
		StartupMode startupMode,
		Map<KafkaTopicPartition, Long> specificStartupOffsets,
		long startupTimestampMillis) {
		this(outputDataType, topic, properties, decodingFormat, startupMode, specificStartupOffsets, startupTimestampMillis, null);
	}

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param outputDataType         Source produced data type
	 * @param topic                  Kafka topic to consume.
	 * @param properties             Properties for the Kafka consumer.
	 * @param decodingFormat         Decoding format for decoding records from Kafka.
	 * @param startupMode            Startup mode for the contained consumer.
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 * @param startupTimestampMillis Startup timestamp for offsets; only relevant when startup
	 *                               mode is {@link StartupMode#TIMESTAMP}.
	 */
	protected KafkaDynamicSourceBase(
			DataType outputDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			Properties otherProperties) {
		this.outputDataType = Preconditions.checkNotNull(
				outputDataType, "Produced data type must not be null.");
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.decodingFormat = Preconditions.checkNotNull(
			decodingFormat, "Decoding format must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.startupTimestampMillis = startupTimestampMillis;
		this.otherProperties = otherProperties;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return this.decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema =
				this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.outputDataType);
		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<RowData> kafkaConsumer =
				getKafkaConsumer(topic, properties, deserializationSchema);
		kafkaConsumer.setWhiteTopicPartitionList(otherProperties.getProperty(ConfigConstants.PARTITION_LIST_KEY));
		long rateLimitingNum =
			Long.valueOf(properties.getProperty(KafkaOptions.SCAN_RATE_LIMITING_NUM.key(), "-1"));
		if (rateLimitingNum > 0) {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitingNum);
			kafkaConsumer.setRateLimiter(rateLimiter);
		}

		String rateLimitingUnitStr = properties.getProperty(KafkaOptions.SCAN_RATE_LIMITING_UNIT.key());
		if (rateLimitingUnitStr != null &&
				RateLimitingUnit.valueList().contains(rateLimitingUnitStr)) {
			kafkaConsumer.setRateLimitingUnit(RateLimitingUnit.valueOf(rateLimitingUnitStr));
		}

		// Set sampling strategy.
		long sampleInterval = Long.parseLong(properties.getProperty(KafkaOptions.SCAN_SOURCE_SAMPLE_INTERVAL.key(), "0"));
		long sampleNum = Long.parseLong(properties.getProperty(KafkaOptions.SCAN_SOURCE_SAMPLE_NUM.key(), "1"));
		kafkaConsumer.setSampleNum(sampleNum);
		kafkaConsumer.setSampleInterval(sampleInterval);

		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaDynamicSourceBase that = (KafkaDynamicSourceBase) o;
		return Objects.equals(outputDataType, that.outputDataType) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets) &&
			startupTimestampMillis == that.startupTimestampMillis;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			outputDataType,
			topic,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis);
	}

	// --------------------------------------------------------------------------------------------
	// Abstract methods for subclasses
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema);

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<RowData> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema) {
		FlinkKafkaConsumerBase<RowData> kafkaConsumer =
				createKafkaConsumer(topic, properties, deserializationSchema);
		switch (startupMode) {
			case EARLIEST:
				kafkaConsumer.setStartFromEarliest();
				break;
			case LATEST:
				kafkaConsumer.setStartFromLatest();
				break;
			case GROUP_OFFSETS:
				kafkaConsumer.setStartFromGroupOffsets();
				break;
			case SPECIFIC_OFFSETS:
				kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
				break;
			case TIMESTAMP:
				kafkaConsumer.setStartFromTimestamp(startupTimestampMillis);
				break;
			}
		kafkaConsumer.setCommitOffsetsOnCheckpoints(properties.getProperty("group.id") != null);
		return kafkaConsumer;
	}
}
