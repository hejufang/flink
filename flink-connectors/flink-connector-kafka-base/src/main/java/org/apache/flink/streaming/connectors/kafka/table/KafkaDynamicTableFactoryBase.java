/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.KafkaSourceConfig;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_CLUSTER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_PARTITION_RANGE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RATE_LIMITING_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RATE_LIMITING_UNIT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RELATIVE_OFFSET;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_START_IGNORE_STATE_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_LOG_FAILURE_ONLY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableOptions;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARTITIONER_FIELD;

/**
 * Factory for creating configured instances of
 * {@link KafkaDynamicSourceBase} and {@link KafkaDynamicSinkBase}.
 */
public abstract class KafkaDynamicTableFactoryBase implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(TOPIC);
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		validateTableOptions(tableOptions);

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		final KafkaOptions.StartupOptions startupOptions = getStartupOptions(tableOptions, topic);
		return createKafkaTableSource(
				producedDataType,
				topic,
				getKafkaProperties(context.getCatalogTable().getOptions()),
				decodingFormat,
				startupOptions.startupMode,
				startupOptions.specificOffsets,
				startupOptions.startupTimestampMillis,
				getKafkaSourceConfig(context.getCatalogTable().getSchema(), tableOptions));
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		String topic = tableOptions.get(TOPIC);
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class,
				FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		// Validate the option values.
		validateTableOptions(tableOptions);

		DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		return createKafkaTableSink(
				consumedDataType,
				topic,
				getKafkaProperties(context.getCatalogTable().getOptions()),
				getFlinkKafkaPartitioner(tableOptions, context.getClassLoader(), context.getCatalogTable().getSchema()),
				encodingFormat,
				getSinkOtherProperties(context.getCatalogTable().getOptions()));
	}

	/**
	 * Constructs the version-specific Kafka table source.
	 *
	 * @param producedDataType       Source produced data type
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 */
	protected abstract KafkaDynamicSourceBase createKafkaTableSource(
			DataType producedDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			KafkaSourceConfig kafkaSourceConfig);

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param consumedDataType Sink consumed data type
	 * @param topic            Kafka topic to consume
	 * @param properties       Properties for the Kafka consumer
	 * @param partitioner      Partitioner to select Kafka partition for each item
	 * @param encodingFormat   Encoding format for encoding records to Kafka
	 */
	protected abstract KafkaDynamicSinkBase createKafkaTableSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			Properties otherProperties);

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TOPIC);
		options.add(FactoryUtil.FORMAT);
		options.add(PROPS_CLUSTER);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PROPS_GROUP_ID);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SCAN_PARTITION_RANGE);
		options.add(SCAN_RATE_LIMITING_NUM);
		options.add(SCAN_RATE_LIMITING_UNIT);
		options.add(SCAN_SOURCE_SAMPLE_NUM);
		options.add(SCAN_SOURCE_SAMPLE_INTERVAL);
		options.add(SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION);
		options.add(SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL);
		options.add(SCAN_RELATIVE_OFFSET);
		options.add(SCAN_START_IGNORE_STATE_OFFSETS);

		options.add(SINK_LOG_FAILURE_ONLY);
		options.add(SINK_PARTITIONER);
		options.add(SINK_PARTITIONER_FIELD);
		options.add(FactoryUtil.SOURCE_METADATA_COLUMNS);
		options.add(FactoryUtil.PARALLELISM);
		return options;
	}

	private KafkaSourceConfig getKafkaSourceConfig(TableSchema tableSchema, ReadableConfig readableConfig) {
		KafkaSourceConfig sourceConfig = new KafkaSourceConfig();

		String cluster = readableConfig.get(PROPS_CLUSTER);
		String topic = readableConfig.get(TOPIC);
		readableConfig.getOptional(SCAN_PARTITION_RANGE).ifPresent(
			range ->
				sourceConfig.setPartitionTopicList(cluster + "||" + topic + "||" + range)
		);
		readableConfig.getOptional(SCAN_RATE_LIMITING_NUM).ifPresent(sourceConfig::setScanSampleNum);
		readableConfig.getOptional(SCAN_RATE_LIMITING_UNIT).ifPresent(sourceConfig::setRateLimitingUnit);
		readableConfig.getOptional(SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION).ifPresent(sourceConfig::setKafkaResetNewPartition);
		readableConfig.getOptional(SCAN_SOURCE_SAMPLE_INTERVAL).ifPresent(sourceConfig::setScanSampleInterval);
		readableConfig.getOptional(SCAN_SOURCE_SAMPLE_NUM).ifPresent(sourceConfig::setScanSampleNum);
		readableConfig.getOptional(FactoryUtil.SOURCE_METADATA_COLUMNS).ifPresent(
			metaDataInfo -> validateAndSetMetaInfo(metaDataInfo, tableSchema, sourceConfig)
		);
		readableConfig.getOptional(SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL).ifPresent(
			d -> sourceConfig.setManualCommitInterval(d.toMillis())
		);
		readableConfig.getOptional(SCAN_RELATIVE_OFFSET).ifPresent(sourceConfig::setRelativeOffset);
		readableConfig.getOptional(FactoryUtil.PARALLELISM).ifPresent(sourceConfig::setParallelism);
		readableConfig.getOptional(SCAN_START_IGNORE_STATE_OFFSETS).ifPresent(sourceConfig::setStartIgnoreStateOffsets);

		return sourceConfig;
	}

	private Properties getSinkOtherProperties(Map<String, String> properties) {
		Properties otherProperties = new Properties();

		String logFailuresOnly = properties.getOrDefault(SINK_LOG_FAILURE_ONLY.key(), "false");
		otherProperties.put(SINK_LOG_FAILURE_ONLY.key(), logFailuresOnly);
		if (properties.containsKey(FactoryUtil.PARALLELISM.key())) {
			otherProperties.put(FactoryUtil.PARALLELISM.key(), properties.get(FactoryUtil.PARALLELISM.key()));
		}
		return otherProperties;
	}

	private void validateAndSetMetaInfo(String metaInfo, TableSchema tableSchema, KafkaSourceConfig sourceConfig) {
		DynamicSourceMetadataFactory factory = new DynamicSourceMetadataFactory() {
			@Override
			protected DynamicSourceMetadata findMetadata(String name) {
				return Metadata.findByName(name);
			}

			@Override
			protected String getMetadataValues() {
				return Metadata.getValuesString();
			}
		};
		sourceConfig.setMetadataMap(factory.parseWithSchema(metaInfo, tableSchema));
		sourceConfig.setWithoutMetaDataType(factory.getWithoutMetaDataTypes(tableSchema, sourceConfig.getMetadataMap().keySet()));
	}
}
