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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.KafkaSinkConfig;
import org.apache.flink.streaming.connectors.kafka.config.KafkaSourceConfig;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerFactory;
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
import org.apache.flink.table.functions.ChangeNonPrimaryFieldsNullNormalizer;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_CLUSTER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_CONSUMER_FACTORY_CLASS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_ENABLE_PROJECTION_PUSHDOWN;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_FORCE_MANUAL_COMMIT_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_PARTITION_RANGE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RELATIVE_OFFSET;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_START_IGNORE_STATE_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_IN_FLIGHT_BATCH_SIZE_FACTOR;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_IN_FLIGHT_MAX_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_LOG_FAILURE_ONLY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER_CLASS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PRODUCER_FACTORY_CLASS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableOptions;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_DELETE_NORMALIZE;
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
		KafkaSourceConfig kafkaSourceConfig = getKafkaSourceConfig(
			context.getCatalogTable().getSchema(),
			tableOptions,
			(RowType) producedDataType.getLogicalType(),
			(Configuration) context.getConfiguration());
		final KafkaOptions.StartupOptions startupOptions = getStartupOptions(tableOptions, topic);
		return createKafkaTableSource(
				producedDataType,
				topic,
				getKafkaProperties(context.getCatalogTable().getOptions()),
				decodingFormat,
				startupOptions.startupMode,
				startupOptions.specificOffsets,
				startupOptions.startupTimestampMillis,
				kafkaSourceConfig);
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

		KafkaSinkConfig sinkConfig = getSinkConfig(tableOptions, context.getCatalogTable().getSchema());

		DataType consumedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		return createKafkaTableSink(
				consumedDataType,
				topic,
				getKafkaProperties(context.getCatalogTable().getOptions()),
				getFlinkKafkaPartitioner(tableOptions, context.getClassLoader(), context.getCatalogTable().getSchema()),
				encodingFormat,
				getSinkOtherProperties(context.getCatalogTable().getOptions()),
				sinkConfig);
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
			Properties otherProperties,
			KafkaSinkConfig sinkConfig);

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
		options.add(RATE_LIMIT_NUM);

		options.add(PROPS_GROUP_ID);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SCAN_PARTITION_RANGE);
		options.add(SCAN_SOURCE_SAMPLE_NUM);
		options.add(SCAN_CONSUMER_FACTORY_CLASS);
		options.add(SCAN_SOURCE_SAMPLE_INTERVAL);
		options.add(SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION);
		options.add(SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL);
		options.add(SCAN_FORCE_MANUAL_COMMIT_OFFSETS);
		options.add(SCAN_RELATIVE_OFFSET);
		options.add(SCAN_START_IGNORE_STATE_OFFSETS);
		options.add(SCAN_ENABLE_PROJECTION_PUSHDOWN);

		options.add(SINK_LOG_FAILURE_ONLY);
		options.add(SINK_PARTITIONER);
		options.add(SINK_PARTITIONER_FIELD);
		options.add(SINK_PARTITIONER_CLASS);
		options.add(SINK_IN_FLIGHT_BATCH_SIZE_FACTOR);
		options.add(SINK_IN_FLIGHT_MAX_NUM);
		options.add(SINK_DELETE_NORMALIZE);
		options.add(FactoryUtil.SOURCE_METADATA_COLUMNS);
		options.add(FactoryUtil.PARALLELISM);
		options.add(FactoryUtil.SOURCE_KEY_BY_FIELD);
		return options;
	}

	private KafkaSourceConfig getKafkaSourceConfig(
			TableSchema tableSchema,
			ReadableConfig readableConfig,
			RowType rowType,
			Configuration configuration) {
		KafkaSourceConfig sourceConfig = new KafkaSourceConfig();

		String cluster = readableConfig.get(PROPS_CLUSTER);
		String topic = readableConfig.get(TOPIC);
		readableConfig.getOptional(SCAN_PARTITION_RANGE).ifPresent(
			range ->
				sourceConfig.setPartitionTopicList(cluster + "||" + topic + "||" + range)
		);
		readableConfig.getOptional(RATE_LIMIT_NUM).ifPresent(sourceConfig::setRateLimitNumber);
		readableConfig.getOptional(SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION).ifPresent(sourceConfig::setKafkaResetNewPartition);
		readableConfig.getOptional(SCAN_SOURCE_SAMPLE_INTERVAL).ifPresent(sourceConfig::setScanSampleInterval);
		readableConfig.getOptional(SCAN_SOURCE_SAMPLE_NUM).ifPresent(sourceConfig::setScanSampleNum);
		readableConfig.getOptional(SCAN_CONSUMER_FACTORY_CLASS)
			.map(KafkaConsumerFactory::getFactoryByClassName)
			.ifPresent(sourceConfig::setKafkaConsumerFactory);
		readableConfig.getOptional(FactoryUtil.SOURCE_METADATA_COLUMNS).ifPresent(
			metaDataInfo -> validateAndSetMetaInfo(metaDataInfo, tableSchema, sourceConfig)
		);
		readableConfig.getOptional(SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL).ifPresent(
			d -> sourceConfig.setManualCommitInterval(d.toMillis())
		);
		readableConfig.getOptional(SCAN_RELATIVE_OFFSET).ifPresent(sourceConfig::setRelativeOffset);
		readableConfig.getOptional(FactoryUtil.PARALLELISM).ifPresent(sourceConfig::setParallelism);
		readableConfig.getOptional(SCAN_START_IGNORE_STATE_OFFSETS).ifPresent(sourceConfig::setStartIgnoreStateOffsets);
		readableConfig.getOptional(SCAN_FORCE_MANUAL_COMMIT_OFFSETS).ifPresent(sourceConfig::setForceManuallyCommitOffsets);
		if (readableConfig.getOptional(SINK_PARTITIONER_FIELD).isPresent()) {
			throw new FlinkRuntimeException("Source don't support partition-fields.");
		}

		readableConfig.getOptional(FactoryUtil.SOURCE_KEY_BY_FIELD).ifPresent(
			keybyFields -> {
				int[] fields = tableSchema.getIndexListFromFieldNames(keybyFields);
				sourceConfig.setKeySelector(
					KeySelectorUtil.getRowDataSelector(fields, new RowDataTypeInfo(rowType), configuration));
			}
		);
		readableConfig.getOptional(SCAN_ENABLE_PROJECTION_PUSHDOWN).
			ifPresent(sourceConfig::setProjectionPushDownIsApplicable);
		return sourceConfig;
	}

	private KafkaSinkConfig getSinkConfig(ReadableConfig readableConfig, TableSchema tableSchema) {
		KafkaSinkConfig.Builder builder = KafkaSinkConfig.builder();
		switch (readableConfig.get(SINK_DELETE_NORMALIZE)) {
			case NULL_FOR_NON_PRIMARY_FIELDS:
				builder.withDeleteNormalizer(ChangeNonPrimaryFieldsNullNormalizer.of(tableSchema));
				break;
			default:
				// do not set, leave it as null
		}
		return builder.build();
	}

	private Properties getSinkOtherProperties(Map<String, String> properties) {
		Properties otherProperties = new Properties();

		String logFailuresOnly = properties.getOrDefault(SINK_LOG_FAILURE_ONLY.key(), "false");
		otherProperties.put(SINK_LOG_FAILURE_ONLY.key(), logFailuresOnly);
		String inFlightIndex = properties.getOrDefault(SINK_IN_FLIGHT_BATCH_SIZE_FACTOR.key(), "0");
		otherProperties.put(SINK_IN_FLIGHT_BATCH_SIZE_FACTOR.key(), inFlightIndex);
		String maxInFlightNum = properties.getOrDefault(SINK_IN_FLIGHT_MAX_NUM.key(), "0");
		otherProperties.put(SINK_IN_FLIGHT_MAX_NUM.key(), maxInFlightNum);
		if (properties.containsKey(FactoryUtil.PARALLELISM.key())) {
			otherProperties.put(FactoryUtil.PARALLELISM.key(), properties.get(FactoryUtil.PARALLELISM.key()));
		}
		if (properties.containsKey(RATE_LIMIT_NUM.key())) {
			otherProperties.put(RATE_LIMIT_NUM.key(), properties.get(RATE_LIMIT_NUM.key()));
		}
		if (properties.containsKey(SINK_PRODUCER_FACTORY_CLASS.key())) {
			otherProperties.put(SINK_PRODUCER_FACTORY_CLASS.key(), properties.get(SINK_PRODUCER_FACTORY_CLASS.key()));
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
		final TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(tableSchema);
		sourceConfig.setMetadataMap(factory.parseWithSchema(metaInfo, physicalSchema));
		sourceConfig.setWithoutMetaDataType(factory.getWithoutMetaDataTypes(physicalSchema, sourceConfig.getMetadataMap().keySet()));
	}
}
