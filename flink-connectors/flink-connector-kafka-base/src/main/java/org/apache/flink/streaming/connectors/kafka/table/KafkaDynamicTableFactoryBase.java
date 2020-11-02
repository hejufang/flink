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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
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

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_GROUP_ID;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_PARTITION_RANGE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RATE_LIMITING_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RATE_LIMITING_UNIT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_INTERVAL;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_SOURCE_SAMPLE_NUM;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_LOG_FAILURE_ONLY;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.SINK_PARTITIONER_FIELD;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getFlinkKafkaPartitioner;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getKafkaProperties;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.getStartupOptions;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.validateTableOptions;

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
				getSourceOtherProperties(context.getCatalogTable().getOptions()));
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
			Properties otherProperties);

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
		options.add(SINK_LOG_FAILURE_ONLY);
		options.add(SINK_PARTITIONER);
		options.add(SINK_PARTITIONER_FIELD);
		return options;
	}

	private Properties getSourceOtherProperties(Map<String, String> properties) {
		Properties otherProperties = new Properties();

		// kafka partition range.
		String partitionRange = properties.get(SCAN_PARTITION_RANGE.key());
		String cluster = properties.get(PROPERTIES_PREFIX + ".cluster");
		String topic = properties.get(TOPIC.key());
		String partitionRangeConf = cluster + "||" + topic + "||" + partitionRange;
		otherProperties.put(ConfigConstants.PARTITION_LIST_KEY, partitionRangeConf);

		// rate limit num
		String limitingNum = properties.getOrDefault(SCAN_RATE_LIMITING_NUM.key(), null);
		if (limitingNum != null) {
			otherProperties.put(SCAN_RATE_LIMITING_NUM.key(), limitingNum);
		}

		String limitUnit = properties.getOrDefault(SCAN_RATE_LIMITING_UNIT.key(), null);
		if (limitUnit != null) {
			otherProperties.put(SCAN_RATE_LIMITING_UNIT.key(), limitUnit);
		}

		return otherProperties;
	}

	private Properties getSinkOtherProperties(Map<String, String> properties) {
		Properties otherProperties = new Properties();

		String logFailuresOnly = properties.getOrDefault(SINK_LOG_FAILURE_ONLY.key(), "false");
		otherProperties.put(SINK_LOG_FAILURE_ONLY.key(), logFailuresOnly);
		return otherProperties;
	}
}
