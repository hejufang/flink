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

package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQMetadata;
import org.apache.flink.connector.rocketmq.RocketMQOptions;
import org.apache.flink.connector.rocketmq.selector.DefaultTopicSelector;
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
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.CLUSTER;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFAULT_TOPIC_SELECTOR;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.GROUP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL00;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL18;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.PROPERTIES_PREFIX;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_ASSIGN_QUEUE_STRATEGY;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DELAY_LEVEL_FIELD;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_TOPIC_SELECTOR;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.TAG;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.TOPIC;
import static org.apache.flink.table.factories.FactoryUtil.SOURCE_METADATA_COLUMNS;

/**
 * RocketMQDynamicTableFactory.
 */
public class RocketMQDynamicTableFactory implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {

		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		DataType sourceDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
			DeserializationFormatFactory.class,
			FactoryUtil.FORMAT);
		// Validate the option data type.
		helper.validateExcept(PROPERTIES_PREFIX);

		RocketMQOptions.validateTableOptions(helper.getOptions());
		RocketMQConfig<RowData> rocketMQConfig = createMQConfig(context.getCatalogTable().getSchema(), helper.getOptions(), false);
		return new RocketMQDynamicSource(sourceDataType, context.getCatalogTable().getOptions(), decodingFormat, rocketMQConfig);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		DataType sinkDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FactoryUtil.FORMAT);

		helper.validateExcept(PROPERTIES_PREFIX);
		RocketMQOptions.validateTableOptions(helper.getOptions());
		RocketMQConfig<RowData> rocketMQConfig = createMQConfig(context.getCatalogTable().getSchema(), helper.getOptions(), true);
		return new RocketMQDynamicSink(sinkDataType, context.getCatalogTable().getOptions(), encodingFormat, rocketMQConfig);
	}

	@Override
	public String factoryIdentifier() {
		return "rocketmq";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(GROUP);
		options.add(CLUSTER);
		options.add(FactoryUtil.FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(TAG);
		options.add(TOPIC);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SCAN_ASSIGN_QUEUE_STRATEGY);
		options.add(SINK_BATCH_SIZE);
		options.add(SINK_DELAY_LEVEL_FIELD);
		options.add(SINK_TOPIC_SELECTOR);
		options.add(FactoryUtil.SINK_PARTITIONER_FIELD);
		options.add(SOURCE_METADATA_COLUMNS);
		return options;
	}

	private RocketMQConfig<RowData> createMQConfig(TableSchema tableSchema, ReadableConfig config, boolean isSink) {
		RocketMQConfig<RowData> rocketMQConfig = new RocketMQConfig<>();

		rocketMQConfig.setGroup(config.get(GROUP));
		rocketMQConfig.setCluster(config.get(CLUSTER));

		if (isSink) {
			Optional<String> delayFieldOption = config.getOptional(SINK_DELAY_LEVEL_FIELD);
			if (delayFieldOption.isPresent()) {
				final int delayFieldIndex = tableSchema.getIndexListFromFieldNames(delayFieldOption.get())[0];
				rocketMQConfig.setMsgDelayLevelSelector(
					rowData ->
						Math.min(MSG_DELAY_LEVEL18, Math.max(rowData.getInt(delayFieldIndex), MSG_DELAY_LEVEL00)));
			}

			String topicSelectorType = config.get(SINK_TOPIC_SELECTOR);
			switch (topicSelectorType) {
				case DEFAULT_TOPIC_SELECTOR:
					String topicName =
						config.getOptional(TOPIC).orElseThrow(
							() -> new FlinkRuntimeException(String.format("%s must be set when use %s.",
								TOPIC.key(), DEFAULT_TOPIC_SELECTOR)));
					String tagName =
						config.getOptional(TAG).orElse("");
					rocketMQConfig.setTopicSelector(new DefaultTopicSelector<>(topicName, tagName));
					break;
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported topic selector: %s, supported selector: %s",
							topicSelectorType, Collections.singleton(DEFAULT_TOPIC_SELECTOR)));
			}

			config.getOptional(FactoryUtil.SINK_PARTITIONER_FIELD).ifPresent(
				keyByFields ->
					rocketMQConfig.setKeyByFields(tableSchema.getIndexListFromFieldNames(keyByFields))
			);
			config.getOptional(SOURCE_METADATA_COLUMNS).ifPresent(
				metadataInfo ->
					validateAndSetMetadata(metadataInfo, rocketMQConfig, tableSchema)
			);
		} else {
			rocketMQConfig.setTopic(config.getOptional(TOPIC).orElseThrow(
				() -> new FlinkRuntimeException(
					String.format("You must set `%s` when use RocketMQ consumer.", TOPIC.key()))));
			rocketMQConfig.setTag(config.get(TAG));
			rocketMQConfig.setSendBatchSize(config.get(SINK_BATCH_SIZE));
			rocketMQConfig.setAssignQueueStrategy(config.get(SCAN_ASSIGN_QUEUE_STRATEGY));
		}

		return rocketMQConfig;
	}

	private void validateAndSetMetadata(String metaInfo, RocketMQConfig<RowData> config, TableSchema schema) {
		DynamicSourceMetadataFactory dynamicSourceMetadataFactory = new DynamicSourceMetadataFactory() {
			@Override
			protected DynamicSourceMetadata findMetadata(String name) {
				return RocketMQMetadata.findByName(name);
			}

			@Override
			protected String getMetadataValues() {
				return RocketMQMetadata.getValuesString();
			}
		};
		Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap =
			dynamicSourceMetadataFactory.parseWithSchema(metaInfo, schema);
		config.setMetadataMap(metadataMap);
	}
}
