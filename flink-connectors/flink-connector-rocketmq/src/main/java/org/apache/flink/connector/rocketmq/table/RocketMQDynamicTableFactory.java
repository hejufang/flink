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

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQMetadata;
import org.apache.flink.connector.rocketmq.RocketMQOptions;
import org.apache.flink.connector.rocketmq.RocketMQUtils;
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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.BINLOG_TARGET_TABLE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.CLUSTER;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFAULT_TOPIC_SELECTOR;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFER_MILLIS_MAX;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.DEFER_MILLIS_MIN;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.GROUP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL00;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.MSG_DELAY_LEVEL18;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.PROPERTIES_PREFIX;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_ASSIGN_QUEUE_STRATEGY;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_BROKER_QUEUE_LIST;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_FORCE_AUTO_COMMIT;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_ASYNC_MODE_ENABLED;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_BATCH_FLUSH_ENABLE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_BATCH_SIZE;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DEFER_LOOP;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DEFER_LOOP_FIELD;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DEFER_MILLIS;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DEFER_MILLIS_FIELD;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_DELAY_LEVEL_FIELD;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_MESSAGE_DELAY_LEVEL;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SINK_TOPIC_SELECTOR;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.TAG;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.TOPIC;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SCAN_SOURCE_IDLE_TIMEOUT;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SOURCE_METADATA_COLUMNS;

/**
 * RocketMQDynamicTableFactory.
 */
public class RocketMQDynamicTableFactory implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQDynamicTableFactory.class);

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
		RocketMQConfig<RowData> rocketMQConfig =
			createRocketmqSourceConfig(
				context.getCatalogTable().getSchema(),
				helper.getOptions(),
				(RowType) sourceDataType.getLogicalType(),
				(Configuration) context.getConfiguration());
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
		options.add(CLUSTER);
		options.add(FactoryUtil.FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(GROUP);
		options.add(TAG);
		options.add(TOPIC);
		options.add(SCAN_STARTUP_MODE);
		options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
		options.add(SCAN_ASSIGN_QUEUE_STRATEGY);
		options.add(SCAN_FORCE_AUTO_COMMIT);
		options.add(SINK_BATCH_SIZE);
		options.add(SINK_DELAY_LEVEL_FIELD);
		options.add(SINK_DEFER_MILLIS);
		options.add(SINK_DEFER_MILLIS_FIELD);
		options.add(SINK_DEFER_LOOP);
		options.add(SINK_DEFER_LOOP_FIELD);
		options.add(SINK_MESSAGE_DELAY_LEVEL);
		options.add(SINK_TOPIC_SELECTOR);
		options.add(FactoryUtil.SINK_PARTITIONER_FIELD);
		options.add(SOURCE_METADATA_COLUMNS);
		options.add(SCAN_BROKER_QUEUE_LIST);
		options.add(SINK_BATCH_FLUSH_ENABLE);
		options.add(SINK_ASYNC_MODE_ENABLED);
		options.add(FactoryUtil.PARALLELISM);
		options.add(FactoryUtil.RATE_LIMIT_NUM);
		options.add(FactoryUtil.SOURCE_KEY_BY_FIELD);
		options.add(FactoryUtil.SCAN_SOURCE_IDLE_TIMEOUT);
		options.add(FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL);
		return options;
	}

	private RocketMQConfig<RowData> createRocketmqSourceConfig(
			TableSchema tableSchema,
			ReadableConfig config,
			RowType rowType,
			Configuration configuration) {
		RocketMQConfig<RowData> rocketMQConfig = createMQConfig(tableSchema, config, false);

		config.getOptional(FactoryUtil.SOURCE_KEY_BY_FIELD).ifPresent(
			keyByFields -> {
				int[] fields = tableSchema.getIndexListFromFieldNames(keyByFields);
				rocketMQConfig.setKeySelector(
					KeySelectorUtil.getRowDataSelector(fields, new RowDataTypeInfo(rowType), configuration));
			}
		);
		return rocketMQConfig;
	}

	private RocketMQConfig<RowData> createMQConfig(TableSchema tableSchema, ReadableConfig config, boolean isSink) {
		RocketMQConfig<RowData> rocketMQConfig = new RocketMQConfig<>();

		rocketMQConfig.setGroup(config.get(GROUP));
		rocketMQConfig.setCluster(config.get(CLUSTER));
		rocketMQConfig.setParallelism(config.get(FactoryUtil.PARALLELISM));
		config.getOptional(RATE_LIMIT_NUM).ifPresent(rate -> {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rate);
			rocketMQConfig.setRateLimiter(rateLimiter);
		});
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
					rocketMQConfig.setSinkKeyByFields(tableSchema.getIndexListFromFieldNames(keyByFields))
			);

			validateConflictConf(config, SINK_DEFER_MILLIS, SINK_DEFER_MILLIS_FIELD);
			config.getOptional(SINK_DEFER_MILLIS).ifPresent(
				millis -> {
					if (millis < DEFER_MILLIS_MIN || millis > DEFER_MILLIS_MAX) {
						throw new FlinkRuntimeException(
							String.format("%s not in range [%s, %s]",
								SINK_DEFER_MILLIS.key(), DEFER_MILLIS_MIN, DEFER_MILLIS_MAX)
						);
					}
					rocketMQConfig.setDeferMillisSelector(msg -> millis);
				}
			);

			config.getOptional(SINK_DEFER_MILLIS_FIELD).ifPresent(
				field -> {
					int[] fieldIndexList = tableSchema.getIndexListFromFieldNames(field);
					if (!(tableSchema.getFieldDataType(field).get().getLogicalType() instanceof BigIntType)) {
						throw new FlinkRuntimeException(
							String.format("Field `%s` data type must be bigint.", field));
					}
					rocketMQConfig.setDeferMillisSelector(msg -> msg.getLong(fieldIndexList[0]));
				}
			);

			validateConflictConf(config, SINK_DEFER_LOOP, SINK_DEFER_LOOP_FIELD);
			config.getOptional(SINK_DEFER_LOOP).ifPresent(
				loop -> {
					if (loop > 0) {
						rocketMQConfig.setDeferLoopSelector(msg -> loop);
					} else {
						throw new FlinkRuntimeException(
							String.format("Config `%s` value must greater than 0", loop)
						);
					}
				}
			);

			config.getOptional(SINK_DEFER_LOOP_FIELD).ifPresent(
				loopField -> {
					int[] loopIndexList = tableSchema.getIndexListFromFieldNames(loopField);
					if (!(tableSchema.getFieldDataType(loopField).get().getLogicalType() instanceof IntType)) {
						throw new FlinkRuntimeException(
							String.format("Field `%s` data type must be int.", loopField));
					}
					rocketMQConfig.setDeferLoopSelector(msg -> msg.getInt(loopIndexList[0]));
				}
			);

			if (rocketMQConfig.getDeferLoopSelector() != null && rocketMQConfig.getDeferMillisSelector() == null) {
				throw new FlinkRuntimeException(
					String.format("`%s or %s` must be set, when you use `%s or %s`", SINK_DEFER_MILLIS.key(),
						SINK_DEFER_MILLIS_FIELD.key(), SINK_DEFER_LOOP.key(), SINK_DEFER_LOOP_FIELD.key())
				);
			}

			config.getOptional(SINK_MESSAGE_DELAY_LEVEL).ifPresent(rocketMQConfig::setDelayLevel);

			if (StringUtils.isNullOrWhitespaceOnly(rocketMQConfig.getGroup())) {
				rocketMQConfig.setGroup(UUID.randomUUID().toString());
			}

			config.getOptional(SINK_BATCH_FLUSH_ENABLE).ifPresent(rocketMQConfig::setBatchFlushEnable);
			config.getOptional(SINK_BUFFER_FLUSH_INTERVAL).ifPresent(
				interval -> rocketMQConfig.setFlushIntervalMs(interval.toMillis())
			);

			config.getOptional(SINK_ASYNC_MODE_ENABLED).ifPresent(
				x ->
					LOG.warn("Param {} not supported in current version.", SINK_ASYNC_MODE_ENABLED.key())
			);
			rocketMQConfig.setSendBatchSize(config.get(SINK_BATCH_SIZE));
		} else {
			rocketMQConfig.setTopic(config.getOptional(TOPIC).orElseThrow(
				() -> new FlinkRuntimeException(
					String.format("You must set `%s` when use RocketMQ consumer.", TOPIC.key()))));
			rocketMQConfig.setTag(config.get(TAG));
			rocketMQConfig.setAssignQueueStrategy(config.get(SCAN_ASSIGN_QUEUE_STRATEGY));
			if (rocketMQConfig.getTag() == null && "binlog".equalsIgnoreCase(config.get(FORMAT))) {
				rocketMQConfig.setTag(config.get(BINLOG_TARGET_TABLE));
			}
			config.getOptional(SCAN_BROKER_QUEUE_LIST).ifPresent(rocketMQConfig::setRocketMqBrokerQueueList);
			validateBrokerQueueList(rocketMQConfig);
			if (StringUtils.isNullOrWhitespaceOnly(rocketMQConfig.getGroup())) {
				throw new FlinkRuntimeException("You have to specific group when use rocketmq consumer.");
			}
			if (config.getOptional(FactoryUtil.SINK_PARTITIONER_FIELD).isPresent()) {
				throw new FlinkRuntimeException("Source don't support partition-fields.");
			}

			config.getOptional(SOURCE_METADATA_COLUMNS).ifPresent(
				metadataInfo ->
					validateAndSetMetadata(metadataInfo, rocketMQConfig, tableSchema));
			config.getOptional(SCAN_SOURCE_IDLE_TIMEOUT).ifPresent(
				idle ->
					rocketMQConfig.setIdleTimeOut(config.get(SCAN_SOURCE_IDLE_TIMEOUT).toMillis())
			);
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

	private void validateBrokerQueueList(RocketMQConfig<RowData> rocketMQConfig) {
		Map<String, List<MessageQueue>> messageQueueMap =
			RocketMQUtils.parseCluster2QueueList(rocketMQConfig.getRocketMqBrokerQueueList());
		if (!messageQueueMap.isEmpty() && messageQueueMap.get(rocketMQConfig.getCluster()) == null) {
			throw new FlinkRuntimeException(
				String.format("Cluster %s not found in broker queue config", rocketMQConfig.getCluster()));
		}
	}

	private void validateConflictConf(
			ReadableConfig readableConfig,
			ConfigOption<?> conf1,
			ConfigOption<?> conf2) {
		if (readableConfig.getOptional(conf1).isPresent() && readableConfig.getOptional(conf2).isPresent()) {
			throw new FlinkRuntimeException(
				String.format("Config `%s` and `%s` are conflict.", conf1.key(), conf2.key())
			);
		}
	}
}
