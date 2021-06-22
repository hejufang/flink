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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.KafkaSourceConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/**
 * Factory for creating configured instances of {@link Kafka010DynamicSource}.
 */
public class Kafka010DynamicTableFactory extends KafkaDynamicTableFactoryBase {

	public static final String IDENTIFIER = "kafka-0.10";

	@Override
	protected KafkaDynamicSourceBase createKafkaTableSource(
			DataType producedDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			KafkaSourceConfig kafkaSourceConfig) {
		return new Kafka010DynamicSource(
			producedDataType,
			topic,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis,
			kafkaSourceConfig);
	}

	@Override
	protected KafkaDynamicSinkBase createKafkaTableSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			Properties otherProperties) {

		return new Kafka010DynamicSink(
			consumedDataType,
			topic,
			properties,
			partitioner,
			encodingFormat,
			otherProperties);
	}

	@Override
	protected DecodingFormat<DeserializationSchema<RowData>> getKafkaDecodingFormat(
			FactoryUtil.TableFactoryHelper helper,
			Properties kafkaProperties) {
		if (helper.getOptions().getOptional(FORMAT).isPresent()) {
			return helper.discoverDecodingFormat(
				DeserializationFormatFactory.class, FactoryUtil.FORMAT);
		} else {
			if (kafkaProperties.containsKey(ConsumerConfig.PARQUET_SELECT_COLUMNS_CONIFG)) {
				return new DecodingFormat<DeserializationSchema<RowData>>() {
					@Override
					public DeserializationSchema<RowData> createRuntimeDecoder(
							DynamicTableSource.Context context,
							DataType producedDataType) {
						return null;
					}

					@Override
					public ChangelogMode getChangelogMode() {
						return ChangelogMode.insertOnly();
					}
				};
			}
			throw new FlinkRuntimeException(
				String.format("Must contains `format` property if you not use `%s`.",
					ConsumerConfig.PARQUET_SELECT_COLUMNS_CONIFG));
		}
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}
}
