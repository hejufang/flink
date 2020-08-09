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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.PropertyUtils;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.METADATA_FIELDS_MAPPING;
import static org.apache.flink.table.descriptors.KafkaValidator.METADATA_FIELD_INDEX_MAPPING;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * Factory for creating configured instances of {@link Kafka010TableSource}.
 */
public class Kafka010TableSourceSinkFactory extends KafkaTableSourceSinkFactoryBase {

	@Override
	protected String kafkaVersion() {
		return KafkaValidator.CONNECTOR_VERSION_VALUE_010;
	}

	@Override
	protected boolean supportsKafkaTimestamps() {
		return true;
	}

	@Override
	protected KafkaTableSourceBase createKafkaTableSource(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Map<String, String> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			Long relativeOffset,
			Long timestamp,
			Map<String, String> configuration) {
		return new Kafka010TableSource(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			Optional.of(fieldMapping),
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets,
			relativeOffset,
			timestamp,
			configuration);
	}

	protected KafkaTableSourceBase createKafkaTableSource(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Map<String, String> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			Long relativeOffset,
			Long timestamp,
			Map<String, String> configuration,
			DeserializationSchema<Row> deserializationSchemaWithoutMetadata) {
		return new Kafka010TableSource(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			Optional.of(fieldMapping),
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets,
			relativeOffset,
			timestamp,
			configuration,
			deserializationSchemaWithoutMetadata);
	}

	@Override
	protected KafkaTableSinkBase createKafkaTableSink(
			TableSchema schema,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<Row>> partitioner,
			SerializationSchema<Row> serializationSchema,
			Map<String, String> configuration) {
		return new Kafka010TableSink(
			schema,
			topic,
			properties,
			partitioner,
			serializationSchema,
			configuration);
	}

	/**
	 * Construct properties map by removing metadata related properties and adding a
	 * {@link KafkaValidator#METADATA_FIELD_INDEX_MAPPING} string that represents
	 * mapping of schema field indices and {{@link Metadata}}.
	 */
	protected Map<String, String> genPropsWithoutMetadata(Map<String, String> originalProperties, TableSchema tableSchema) {
		if (originalProperties.containsKey(METADATA_FIELDS_MAPPING)) {
			Map<String, String> properties = new HashMap<>(originalProperties);
			List<String> fieldIndexToMetadataStrList = new ArrayList<>();
			List<Integer> fieldIndexList = new ArrayList<>();
			String[] extractItems = originalProperties.get(METADATA_FIELDS_MAPPING).split(",");
			for (String extractItem: extractItems) {
				String[] kvList = extractItem.trim().split("=");
				String metadataName = kvList[0];
				String fieldName = kvList[1];
				tableSchema.getFieldNameIndex(fieldName)
					.ifPresent(index -> {
						fieldIndexToMetadataStrList.add(index + "=" + metadataName);
						fieldIndexList.add(index);
					});
			}
			if (fieldIndexToMetadataStrList.size() > 0) {
				properties.put(METADATA_FIELD_INDEX_MAPPING, String.join(",", fieldIndexToMetadataStrList));
				PropertyUtils.removeFieldProperties(properties, fieldIndexList);
				return properties;
			}
		}
		return originalProperties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);

		final DeserializationSchema<Row> deserializationSchema = TableConnectorUtils.getDeserializationSchema(
			properties, this.getClass().getClassLoader());
		// Generate a deserializationSchema without kafka metadata.
		Map<String, String> newProperties = genPropsWithoutMetadata(properties, descriptorProperties.getTableSchema(SCHEMA));
		DeserializationSchema<Row> deserializationSchemaWithoutMetadata = null;
		if (newProperties.containsKey(METADATA_FIELD_INDEX_MAPPING)) {
			deserializationSchemaWithoutMetadata = TableConnectorUtils.getDeserializationSchema(
				newProperties, this.getClass().getClassLoader());
			// Putting this property in it is for delivering the mapping to consumer.
			descriptorProperties.putString(METADATA_FIELD_INDEX_MAPPING,
				newProperties.get(METADATA_FIELD_INDEX_MAPPING));
		}
		final StartupOptions startupOptions = getStartupOptions(descriptorProperties, topic);
		Map<String, String> configurations = getOtherConfigurations(descriptorProperties);

		return createKafkaTableSource(
			descriptorProperties.getTableSchema(SCHEMA),
			SchemaValidator.deriveProctimeAttribute(descriptorProperties),
			SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
			SchemaValidator.deriveFieldMapping(
				descriptorProperties,
				Optional.of(deserializationSchema.getProducedType())
			),
			topic,
			getKafkaProperties(descriptorProperties),
			deserializationSchema,
			startupOptions.startupMode,
			startupOptions.specificOffsets,
			startupOptions.relativeOffset,
			startupOptions.timestamp,
			configurations,
			deserializationSchemaWithoutMetadata
			);
	}
}
