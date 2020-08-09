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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_KEYBY_FIELDS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RATE_LIMITING_NUM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RATE_LIMITING_UNIT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_PARTITION_RANGE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_SAMPLE_INTERVAL;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_SAMPLE_NUM;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
@Internal
public abstract class KafkaTableSourceBase implements
		StreamTableSource<Row>,
		DefinedProctimeAttribute,
		DefinedRowtimeAttributes,
		DefinedFieldMapping {

	// common table source attributes

	/** The schema of the table. */
	private final TableSchema schema;

	/** Field name of the processing time attribute, null if no processing time field is defined. */
	private final Optional<String> proctimeAttribute;

	/** Descriptor for a rowtime attribute. */
	private final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/** Mapping for the fields of the table schema to fields of the physical returned type. */
	private final Optional<Map<String, String>> fieldMapping;

	// Kafka-specific attributes

	/** The Kafka topic to consume. */
	private final String topic;

	/** Properties for the Kafka consumer. */
	private final Properties properties;

	/** Other configurations for kafka table source, such as keyby fields, parallelism and so on. */
	private final Map<String, String> configurations;

	/** Deserialization schema for decoding records from Kafka. */
	private final DeserializationSchema<Row> deserializationSchema;

	/** The startup mode for the contained consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
	private final StartupMode startupMode;

	/** Specific startup offsets; only relevant when startup mode is {@link StartupMode#SPECIFIC_OFFSETS}. */
	private final Map<KafkaTopicPartition, Long> specificStartupOffsets;

	/** Relative offset; only relavent when startup mode is in
	 * {{@link StartupMode#EARLIEST}, {@link StartupMode#GROUP_OFFSETS}, {@link StartupMode#LATEST}}. */
	private final Long relativeOffset;

	/** Specific timestamp for kafka consumer to consume from. */
	private final Long timestamp;

	/** Deserialization schema without metadata for decoding records from Kafka. */
	private final DeserializationSchema<Row> deserializationSchemaWithoutMetadata;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param schema                      Schema of the produced table.
	 * @param proctimeAttribute           Field name of the processing time attribute.
	 * @param rowtimeAttributeDescriptors Descriptor for a rowtime attribute
	 * @param fieldMapping                Mapping for the fields of the table schema to
	 *                                    fields of the physical returned type.
	 * @param topic                       Kafka topic to consume.
	 * @param properties                  Properties for the Kafka consumer.
	 * @param deserializationSchema       Deserialization schema for decoding records from Kafka.
	 * @param startupMode                 Startup mode for the contained consumer.
	 * @param specificStartupOffsets      Specific startup offsets; only relevant when startup
	 *                                    mode is {@link StartupMode#SPECIFIC_OFFSETS}.
	 * @param relativeOffset              Relative offset.
	 * @param timestamp                   Timestamp for consumer to start from.
	 */
	protected KafkaTableSourceBase(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Optional<Map<String, String>> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			Long relativeOffset,
			Long timestamp) {
			this(schema,
				proctimeAttribute,
				rowtimeAttributeDescriptors,
				fieldMapping,
				topic,
				properties,
				deserializationSchema,
				startupMode,
				specificStartupOffsets,
				relativeOffset,
				timestamp,
				new HashMap<>(),
				null);
	}

	protected KafkaTableSourceBase(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Optional<Map<String, String>> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			Long relativeOffset,
			Long timestamp,
			Map<String, String> configurations) {
		this(schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			fieldMapping,
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets,
			relativeOffset,
			timestamp,
			configurations,
			null);
	}

	protected KafkaTableSourceBase(
			TableSchema schema,
			Optional<String> proctimeAttribute,
			List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
			Optional<Map<String, String>> fieldMapping,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			Long relativeOffset,
			Long timestamp,
			Map<String, String> configurations,
			DeserializationSchema<Row> deserializationSchemaWithoutMetadata) {
		this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
		this.proctimeAttribute = validateProctimeAttribute(proctimeAttribute);
		this.rowtimeAttributeDescriptors = validateRowtimeAttributeDescriptors(rowtimeAttributeDescriptors);
		this.fieldMapping = fieldMapping;
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.deserializationSchema = Preconditions.checkNotNull(
			deserializationSchema, "Deserialization schema must not be null.");
		this.startupMode = Preconditions.checkNotNull(startupMode, "Startup mode must not be null.");
		this.specificStartupOffsets = Preconditions.checkNotNull(
			specificStartupOffsets, "Specific offsets must not be null.");
		this.relativeOffset = relativeOffset;
		this.timestamp = timestamp;
		this.configurations = configurations;
		this.deserializationSchemaWithoutMetadata = deserializationSchemaWithoutMetadata;
	}

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param schema                Schema of the produced table.
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema for decoding records from Kafka.
	 */
	protected KafkaTableSourceBase(
			TableSchema schema,
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema) {
		this(
			schema,
			Optional.empty(),
			Collections.emptyList(),
			Optional.empty(),
			topic, properties,
			deserializationSchema,
			StartupMode.GROUP_OFFSETS,
			Collections.emptyMap(),
			null,
			null);
	}

	/**
	 * NOTE: This method is for internal use only for defining a TableSource.
	 *       Do not use it in Table API programs.
	 */
	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

		DeserializationSchema<Row> deserializationSchema = getDeserializationSchema();

		// Version-specific Kafka consumer
		FlinkKafkaConsumerBase<Row> kafkaConsumer = getKafkaConsumer(topic, properties, deserializationSchema);

		// Set partition ranger to consume
		String partitionRange = configurations.get(CONNECTOR_SOURCE_PARTITION_RANGE);
		if (partitionRange != null) {
			kafkaConsumer.setWhiteTopicPartitionList(properties.getProperty("cluster") + "||" + topic + "||" + partitionRange);
		}

		// Set kafka rate limiting strategy.
		long rateLimitingNum =
			Long.valueOf(configurations.getOrDefault(CONNECTOR_RATE_LIMITING_NUM, "-1"));
		String rateLimitingUnitStr = configurations.get(CONNECTOR_RATE_LIMITING_UNIT);
		if (rateLimitingNum > 0) {
			FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
			rateLimiter.setRate(rateLimitingNum);
			kafkaConsumer.setRateLimiter(rateLimiter);
		}

		if (rateLimitingUnitStr != null
			&& RateLimitingUnit.valueList().contains(rateLimitingUnitStr)) {
			kafkaConsumer.setRateLimitingUnit(RateLimitingUnit.valueOf(rateLimitingUnitStr));
		}

		// Set sampling strategy.
		long sampleInterval = Long.parseLong(configurations.getOrDefault(CONNECTOR_SOURCE_SAMPLE_INTERVAL, "0"));
		long sampleNum = Long.parseLong(configurations.getOrDefault(CONNECTOR_SOURCE_SAMPLE_NUM, "1"));
		kafkaConsumer.setSampleNum(sampleNum);
		kafkaConsumer.setSampleInterval(sampleInterval);

		// Set Kafka Source Parallelism
		int parallelism = Integer.valueOf(configurations.getOrDefault(CONNECTOR_PARALLELISM, "-1"));

		DataStream<Row> dataStream;
		if (parallelism > 0) {
			dataStream = env.addSource(kafkaConsumer).name(explainSource()).setParallelism(parallelism);
		} else {
			dataStream = env.addSource(kafkaConsumer).name(explainSource());
		}

		// Transfer the stream to a KeyedStream if necessary.
		String keybyFields = configurations.get(CONNECTOR_KEYBY_FIELDS);
		if (keybyFields != null && !keybyFields.isEmpty()) {
			String[] fields = Arrays.stream(keybyFields.split(","))
				.map(String::trim).toArray(String[]::new);
			RowTypeInfo resultTypeInformation = (RowTypeInfo) getReturnType();
			String[] rowFieldNames = resultTypeInformation.getFieldNames();
			for (String fieldName : fields) {
				int fieldIndex = resultTypeInformation.getFieldIndex(fieldName);
				if (fieldIndex < 0) {
					throw new FlinkRuntimeException(String.format("Keyby filed '%s' not found in " +
						"table source, all row fields are: %s", fieldName, Arrays.asList(rowFieldNames)));
				}
			}
			dataStream = dataStream.keyBy(fields);
		}
		return dataStream;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return deserializationSchema.getProducedType();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute.orElse(null);
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping.orElse(null);
	}

	@Override
	public String explainSource() {
		return TableConnectorUtils.generateRuntimeName(this.getClass(), schema.getFieldNames());
	}

	/**
	 * Returns the properties for the Kafka consumer.
	 *
	 * @return properties for the Kafka consumer.
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * Returns the deserialization schema.
	 *
	 * @return The deserialization schema
	 */
	public DeserializationSchema<Row> getDeserializationSchema(){
		return deserializationSchema;
	}

	protected DeserializationSchema<Row> getDeserializationSchemaWithoutMetadata() {
		return deserializationSchemaWithoutMetadata;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaTableSourceBase that = (KafkaTableSourceBase) o;
		return Objects.equals(schema, that.schema) &&
			Objects.equals(proctimeAttribute, that.proctimeAttribute) &&
			Objects.equals(rowtimeAttributeDescriptors, that.rowtimeAttributeDescriptors) &&
			Objects.equals(fieldMapping, that.fieldMapping) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(deserializationSchema, that.deserializationSchema) &&
			startupMode == that.startupMode &&
			Objects.equals(specificStartupOffsets, that.specificStartupOffsets);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			schema,
			proctimeAttribute,
			rowtimeAttributeDescriptors,
			fieldMapping,
			topic,
			properties,
			deserializationSchema,
			startupMode,
			specificStartupOffsets);
	}

	/**
	 * Returns a version-specific Kafka consumer with the start position configured.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected FlinkKafkaConsumerBase<Row> getKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema) {
		FlinkKafkaConsumerBase<Row> kafkaConsumer =
				createKafkaConsumer(topic, properties, deserializationSchema);
		if (configurations.containsKey(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION)) {
			boolean value = Boolean.parseBoolean(configurations.get(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION));
			if (value) {
				kafkaConsumer.resetToEarliestForNewPartition();
			} else {
				kafkaConsumer.disableResetToEarliestForNewPartition();
			}
		}
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
				kafkaConsumer.setStartFromTimestamp(timestamp);
		}
		if (relativeOffset != null) {
			kafkaConsumer.setRelativeOffset(relativeOffset);
		}
		return kafkaConsumer;
	}

	//////// VALIDATION FOR PARAMETERS

	/**
	 * Validates a field of the schema to be the processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	private Optional<String> validateProctimeAttribute(Optional<String> proctimeAttribute) {
		return proctimeAttribute.map((attribute) -> {
			// validate that field exists and is of correct type
			Optional<TypeInformation<?>> tpe = schema.getFieldType(attribute);
			if (!tpe.isPresent()) {
				throw new ValidationException("Processing time attribute '" + attribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Processing time attribute '" + attribute + "' is not of type SQL_TIMESTAMP.");
			}
			return attribute;
		});
	}

	/**
	 * Validates a list of fields to be rowtime attributes.
	 *
	 * @param rowtimeAttributeDescriptors The descriptors of the rowtime attributes.
	 */
	private List<RowtimeAttributeDescriptor> validateRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptors, "List of rowtime attributes must not be null.");
		// validate that all declared fields exist and are of correct type
		for (RowtimeAttributeDescriptor desc : rowtimeAttributeDescriptors) {
			String rowtimeAttribute = desc.getAttributeName();
			Optional<TypeInformation<?>> tpe = schema.getFieldType(rowtimeAttribute);
			if (!tpe.isPresent()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not present in TableSchema.");
			} else if (tpe.get() != Types.SQL_TIMESTAMP()) {
				throw new ValidationException("Rowtime attribute '" + rowtimeAttribute + "' is not of type SQL_TIMESTAMP.");
			}
		}
		return rowtimeAttributeDescriptors;
	}

	/**
	 * Refer to the field_index_mapping string, generate a map to represent it.
	 * @return Mapping of schema field indices and {@link Metadata}.
	 */
	protected Map<Integer, Metadata> genFieldToMetadataMap() {
		if (!configurations.containsKey(KafkaValidator.METADATA_FIELD_INDEX_MAPPING)) {
			return null;
		} else {
			return Arrays.stream(configurations.get(KafkaValidator.METADATA_FIELD_INDEX_MAPPING).split(","))
				.map(kvStr -> kvStr.split("="))
				.collect(Collectors.toMap(kvList -> Integer.valueOf(kvList[0]),
					kvList -> Metadata.findByName(kvList[1])));
		}
	}

	//////// ABSTRACT METHODS FOR SUBCLASSES

	/**
	 * Creates a version-specific Kafka consumer.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @return The version-specific Kafka consumer
	 */
	protected abstract FlinkKafkaConsumerBase<Row> createKafkaConsumer(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema);

}
