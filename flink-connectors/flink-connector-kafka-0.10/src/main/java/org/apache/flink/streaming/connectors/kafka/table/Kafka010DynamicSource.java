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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.KafkaSourceConfig;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWithMetadataWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.10.
 */
@Internal
public class Kafka010DynamicSource extends KafkaDynamicSourceBase {

	/**
	 * Creates a Kafka 0.10 {@link StreamTableSource}.
	 *
	 * @param outputDataType         Source output data type
	 * @param topic                  Kafka topic to consume
	 * @param properties             Properties for the Kafka consumer
	 * @param decodingFormat         Decoding format for decoding records from Kafka
	 * @param startupMode            Startup mode for the contained consumer
	 * @param specificStartupOffsets Specific startup offsets; only relevant when startup
	 *                               mode is {@link StartupMode#SPECIFIC_OFFSETS}
	 * @param startupTimestampMillis Startup timestamp for offsets; only relevant when startup
	 *                               mode is {@link StartupMode#TIMESTAMP}
	 */
	public Kafka010DynamicSource(
			DataType outputDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis,
			KafkaSourceConfig kafkaSourceConfig) {
		super(
			outputDataType,
			topic,
			properties,
			decodingFormat,
			startupMode,
			specificStartupOffsets,
			startupTimestampMillis,
			kafkaSourceConfig);
	}

	@Override
	protected FlinkKafkaConsumerBase<RowData> createKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<RowData> deserializationSchema) {
		FlinkKafkaConsumerBase<RowData> consumerBase;
		if (kafkaSourceConfig.getWithoutMetaDataType() == null) {
			consumerBase = new FlinkKafkaConsumer010<>(topic, deserializationSchema, properties);
		} else {
			TypeInformation<RowData> typeInformation = (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(outputDataType);
			final Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap = kafkaSourceConfig.getMetadataMap();
			KafkaDeserializationSchema<RowData> kafkaDeserializationSchema =
				new KafkaDeserializationSchemaRowDataWithMetadata(
						deserializationSchema,
						typeInformation,
						metadataMap);
			consumerBase = new FlinkKafkaConsumer010<>(topic, kafkaDeserializationSchema, properties);
		}
		if (kafkaSourceConfig.getKafkaResetNewPartition() != null) {
			if (kafkaSourceConfig.getKafkaResetNewPartition()) {
				consumerBase.resetToEarliestForNewPartition();
			} else {
				consumerBase.disableResetToEarliestForNewPartition();
			}
		}

		Optional
			.ofNullable(kafkaSourceConfig.getKafkaConsumerFactory())
			.ifPresent(consumerBase::setKafkaConsumerFactory);

		if (kafkaSourceConfig.getParallelism() != null) {
			consumerBase.setParallelism(kafkaSourceConfig.getParallelism());
		}
		return consumerBase;
	}

	private static class KafkaDeserializationSchemaRowDataWithMetadata extends KafkaDeserializationSchemaWithMetadataWrapper<RowData> {
		public KafkaDeserializationSchemaRowDataWithMetadata(
				DeserializationSchema<RowData> deserializationSchema,
				TypeInformation<RowData> typeInformation,
				Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
			super(deserializationSchema, typeInformation, metadataMap);
		}

		@Override
		public RowData addMetadata(RowData element, ConsumerRecord<byte[], byte[]> record) {
			GenericRowData oldRowData = (GenericRowData) element;
			GenericRowData rowData = new GenericRowData(this.producerType.getArity());
			for (int i = 0, j = 0; i < rowData.getArity(); i++) {
				Metadata metadata = (Metadata) this.metadataMap.get(i);
				if (metadata != null) {
					rowData.setField(i, getMetadata(record, metadata));
				} else {
					rowData.setField(i, oldRowData.getField(j++));
				}
			}
			return rowData;
		}

		private Object getMetadata(ConsumerRecord<byte[], byte[]> record, Metadata metadata) {
			switch (metadata) {
				case OFFSET:
					return record.offset();
				case TIMESTAMP:
					return record.timestamp();
				case PARTITION:
					return (long) record.partition();
				default:
					throw new FlinkRuntimeException("Unsupported metadata.");
			}
		}
	}

	@Override
	public DynamicTableSource copy() {
		return new Kafka010DynamicSource(
				this.outputDataType,
				this.topic,
				this.properties,
				this.decodingFormat,
				this.startupMode,
				this.specificStartupOffsets,
				this.startupTimestampMillis,
				this.kafkaSourceConfig);
	}

	@Override
	public String asSummaryString() {
		return "Kafka-0.10";
	}
}
