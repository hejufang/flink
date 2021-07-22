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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;
import org.apache.flink.streaming.connectors.kafka.config.Metadata;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;

import org.apache.kafka.clients.producer.internals.BatchRandomPartitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.table.descriptors.DescriptorProperties.noValidation;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;

/**
 * The validator for {@link Kafka}.
 */
@Internal
public class KafkaValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_KAFKA = "kafka";
	public static final String CONNECTOR_VERSION_VALUE_08 = "0.8";
	public static final String CONNECTOR_VERSION_VALUE_09 = "0.9";
	public static final String CONNECTOR_VERSION_VALUE_010 = "0.10";
	public static final String CONNECTOR_VERSION_VALUE_011 = "0.11";
	public static final String CONNECTOR_VERSION_VALUE_UNIVERSAL = "universal";
	public static final String CONNECTOR_TOPIC = "connector.topic";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_TEAM = "connector.team";
	public static final String CONNECTOR_PSM = "connector.psm";
	public static final String CONNECTOR_OWNER = "connector.owner";
	public static final String CONNECTOR_GROUP_ID = "connector.group.id";
	public static final String CONNECTOR_STARTUP_MODE = "connector.startup-mode";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_LATEST = "latest-offset";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
	public static final String CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_TIMESTAMP = "specific-timestamp";
	public static final String CONNECTOR_SPECIFIC_OFFSETS = "connector.specific-offsets";
	public static final String CONNECTOR_SPECIFIC_OFFSETS_PARTITION = "partition";
	public static final String CONNECTOR_SPECIFIC_OFFSETS_OFFSET = "offset";
	public static final String CONNECTOR_SPECIFIC_TIMESTAMP = "connector.specific-timestamp";
	public static final String CONNECTOR_RELATIVE_OFFSET = "connector.relative-offset";
	public static final String CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION =
		"connector.reset-to-earliest-for-new-partition";
	public static final String CONNECTOR_KAFKA_PROPERTIES = "connector.kafka.properties";
	public static final String CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS =
		"connector.kafka.properties.partitioner.class";
	public static final String CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS_DEFAULT =
		BatchRandomPartitioner.class.getName();
	public static final String CONNECTOR_PROPERTIES = "connector.properties";
	public static final String CONNECTOR_PROPERTIES_KEY = "key";
	public static final String CONNECTOR_PROPERTIES_VALUE = "value";
	public static final String CONNECTOR_SINK_SEMANTIC = "connector.sink-semantic";
	public static final String CONNECTOR_SINK_PARTITIONER = "connector.sink-partitioner";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_FIXED = "fixed";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_ROW_FIELDS_HASH = "row-fields-hash";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM = "custom";
	public static final String CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY = "custom-class-with-keyby";
	public static final String CONNECTOR_SINK_PARTITIONER_CLASS = "connector.sink-partitioner-class";
	public static final String CONNECTOR_SINK_IGNORE_TRANSACTION_TIMEOUT =
		"connector.sink-ignore-transaction-timeout-error";

	// Rate limiting configurations
	public static final String CONNECTOR_RATE_LIMITING_NUM = "connector.rate-limiting-num";
	public static final String CONNECTOR_RATE_LIMITING_UNIT = "connector.rate-limiting-unit";

	// Partition range to consume
	public static final String CONNECTOR_SOURCE_PARTITION_RANGE = "connector.source-partition-range";

	// Source sampling
	public static final String CONNECTOR_SOURCE_SAMPLE_INTERVAL = "connector.source-sample-interval";
	public static final String CONNECTOR_SOURCE_SAMPLE_NUM = "connector.source-sample-num";

	// Disable currentOffsetsRate metrics
	public static final String DISABLE_CURRENT_OFFSET_RATE_METRICS = "disableCurrentOffsetsRateMetrics";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA, false);

		properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_CLUSTER, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_TEAM, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_PSM, false, 1, Integer.MAX_VALUE);
		properties.validateString(CONNECTOR_OWNER, false, 1, Integer.MAX_VALUE);
		properties.validateBoolean(CONNECTOR_SINK_IGNORE_TRANSACTION_TIMEOUT, true);

		validateRateLimiting(properties);

		validateStartupMode(properties);

		validateKafkaProperties(properties);

		validateSinkPartitioner(properties);

		validatePartitionRange(properties);

		validateSampling(properties);

		validateMetadataExtract(properties);
	}

	private void validatePartitionRange(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_SOURCE_PARTITION_RANGE, true, 1);
	}

	private void validateRateLimiting (DescriptorProperties properties) {
		properties.validateLong(CONNECTOR_RATE_LIMITING_NUM, true, 1);

		properties.validateEnumValues(CONNECTOR_RATE_LIMITING_UNIT, true, RateLimitingUnit.valueList());
	}

	private void validateStartupMode(DescriptorProperties properties) {
		final Map<String, Consumer<String>> specificOffsetValidators = new HashMap<>();
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
			(key) -> properties.validateInt(
				key,
				false,
				0,
				Integer.MAX_VALUE));
		specificOffsetValidators.put(
			CONNECTOR_SPECIFIC_OFFSETS_OFFSET,
			(key) -> properties.validateLong(
				key,
				false,
				0,
				Long.MAX_VALUE));

		final Map<String, Consumer<String>> startupModeValidation = new HashMap<>();
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS, noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_EARLIEST, noValidation());
		startupModeValidation.put(CONNECTOR_STARTUP_MODE_VALUE_LATEST, noValidation());
		startupModeValidation.put(
			CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_TIMESTAMP,
			key -> properties.validateLong(CONNECTOR_SPECIFIC_TIMESTAMP, false));
		startupModeValidation.put(
			CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
			key -> properties.validateFixedIndexedProperties(CONNECTOR_SPECIFIC_OFFSETS, false, specificOffsetValidators));
		properties.validateEnum(CONNECTOR_STARTUP_MODE, true, startupModeValidation);
		properties.validateLong(CONNECTOR_RELATIVE_OFFSET, true);
		properties.validateLong(CONNECTOR_SPECIFIC_TIMESTAMP, true);
		properties.validateBoolean(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION, true);
		properties.validateBoolean(DISABLE_CURRENT_OFFSET_RATE_METRICS, true);
	}

	private void validateKafkaProperties(DescriptorProperties properties) {
		final Map<String, Consumer<String>> propertyValidators = new HashMap<>();
		propertyValidators.put(
			CONNECTOR_PROPERTIES_KEY,
			key -> properties.validateString(key, false, 1));
		propertyValidators.put(
			CONNECTOR_PROPERTIES_VALUE,
			key -> properties.validateString(key, false, 0));
		properties.validateFixedIndexedProperties(CONNECTOR_PROPERTIES, true, propertyValidators);
	}

	private void validateSinkPartitioner(DescriptorProperties properties) {
		final Map<String, Consumer<String>> sinkPartitionerValidators = new HashMap<>();
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_FIXED, noValidation());
		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN, noValidation());
		sinkPartitionerValidators.put(
			CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM,
			key -> properties.validateString(CONNECTOR_SINK_PARTITIONER_CLASS, false, 1));

		sinkPartitionerValidators.put(
			CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY,
			key -> {
				properties.validateString(CONNECTOR_SINK_PARTITIONER_CLASS, false, 1);
				properties.validateString(CONNECTOR_KEYBY_FIELDS, false, 1);
			});

		sinkPartitionerValidators.put(CONNECTOR_SINK_PARTITIONER_VALUE_ROW_FIELDS_HASH,
			key -> properties.validateString(CONNECTOR_KEYBY_FIELDS, false, 1));
		properties.validateEnum(CONNECTOR_SINK_PARTITIONER, true, sinkPartitionerValidators);
	}

	private void validateSampling(DescriptorProperties properties) {
		properties.validateLong(CONNECTOR_SOURCE_SAMPLE_INTERVAL, true, 0);
		properties.validateLong(CONNECTOR_SOURCE_SAMPLE_NUM, true, 1);
	}

	private void validateMetadataExtract(DescriptorProperties properties) {
		properties.getOptionalString(METADATA_FIELDS_MAPPING).ifPresent(
			metadataExtractStr -> {
				TableSchema tableSchema = properties.getTableSchema(SCHEMA);
				String[] extractItems = metadataExtractStr.split(",");
				for (String extractItem: extractItems) {
					String[] kvList = extractItem.trim().split("=");
					if (kvList.length != 2) {
						throw new ValidationException("Could not split the property str: '" + extractItem
							+ "' correctly, please use '=' as delimiter, like 'timestamp=ts'.");
					}
					if (!Metadata.contains(kvList[0])) {
						throw new ValidationException("Could not support this kind of metadata: '" + kvList[0]
							+ "', we only support " + Metadata.getCollectionStr() + ".");
					}
					tableSchema.getFieldDataType(kvList[1])
						.ifPresent(dataType -> {
							if (!dataType.getConversionClass().equals(Metadata.findByName(kvList[0]).getDataClass())) {
								throw new ValidationException("The metadata field '" + kvList[0]
									+ "' should be " + Metadata.findByName(kvList[0]).getDataClass());
							}
						});
				}
			}
		);
	}

	// utilities

	public static String normalizeStartupMode(StartupMode startupMode) {
		switch (startupMode) {
			case EARLIEST:
				return CONNECTOR_STARTUP_MODE_VALUE_EARLIEST;
			case LATEST:
				return CONNECTOR_STARTUP_MODE_VALUE_LATEST;
			case GROUP_OFFSETS:
				return CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS;
			case SPECIFIC_OFFSETS:
				return CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS;
			case TIMESTAMP:
				return CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_TIMESTAMP;
		}
		throw new IllegalArgumentException("Invalid startup mode.");
	}
}
