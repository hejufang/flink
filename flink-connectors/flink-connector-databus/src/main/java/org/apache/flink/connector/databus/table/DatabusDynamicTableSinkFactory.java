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

package org.apache.flink.connector.databus.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.databus.options.DatabusConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.RetryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.databus.table.DatabusOptions.CHANNEL;
import static org.apache.flink.connector.databus.table.DatabusOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.databus.table.DatabusOptions.SINK_MAX_BUFFER_BYTES;
import static org.apache.flink.connector.databus.table.DatabusOptions.SINK_NEED_RESPONSE;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RETRY_INIT_DELAY_MS;
import static org.apache.flink.table.factories.FactoryUtil.RETRY_MAX_TIMES;
import static org.apache.flink.table.factories.FactoryUtil.RETRY_STRATEGY;

/**
 * Databus dynamic table sink factory.
 */
public class DatabusDynamicTableSinkFactory implements DynamicTableSinkFactory {
	private static final Logger LOG = LoggerFactory.getLogger(DatabusDynamicTableSinkFactory.class);
	private static final String IDENTIFIER = "databus";

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final ReadableConfig config = helper.getOptions();

		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
			helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT)
				.orElse(null);
		helper.validate();

		DatabusConfig databusConfig = getDatabusConfig(config);
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		return new DatabusDynamicTableSink(encodingFormat, databusConfig, physicalSchema);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	private static DatabusConfig getDatabusConfig(ReadableConfig config) {
		RetryManager.Strategy retryStrategy = getRetryStrategy(config);

		return DatabusConfig.builder()
			.setChannel(config.get(CHANNEL))
			.setBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS))
			.setMaxBufferSize(config.get(SINK_MAX_BUFFER_BYTES))
			.setNeedResponse(config.get(SINK_NEED_RESPONSE))
			.setRetryStrategy(retryStrategy)
			.setParallelism(config.get(PARALLELISM))
			.build();
	}

	private static RetryManager.Strategy getRetryStrategy (ReadableConfig config) {
		Optional<String> optionalStrategyName = config.getOptional(RETRY_STRATEGY);

		if (optionalStrategyName.isPresent()) {
			int retryTimes = config.get(RETRY_MAX_TIMES);
			int initDelay = config.get(RETRY_INIT_DELAY_MS);
			try {
				return RetryManager.createStrategy(optionalStrategyName.get(), retryTimes, initDelay);
			} catch (IllegalArgumentException e) {
				LOG.warn("Failed to create retry strategy with params, ignore it. " +
						"strategy name: {}, retry max times: {}, init delay ms: {}.",
					optionalStrategyName.get(), retryTimes, initDelay, e);
			}
		}
		return null;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FORMAT);
		options.add(CHANNEL);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		options.add(SINK_MAX_BUFFER_BYTES);
		options.add(SINK_NEED_RESPONSE);
		options.add(RETRY_STRATEGY);
		options.add(RETRY_MAX_TIMES);
		options.add(RETRY_INIT_DELAY_MS);
		return options;
	}
}
