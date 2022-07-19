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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerFactory;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaProducerFactory;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkRowDataFieldHashPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.SINK_PARTITIONER_FIELD;

/** Option utils for Kafka table source sink. */
public class KafkaOptions {
	private KafkaOptions() {}

	// --------------------------------------------------------------------------------------------
	// Kafka specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> TOPIC = ConfigOptions
			.key("topic")
			.stringType()
			.noDefaultValue()
			.withDescription("Required topic name from which the table is read");

	public static final ConfigOption<String> PROPS_GROUP_ID = ConfigOptions
			.key("properties.group.id")
			.stringType()
			.noDefaultValue()
			.withDescription("Required consumer group in Kafka consumer, no need for Kafka producer");

	public static final ConfigOption<String> PROPS_CLUSTER = ConfigOptions
			.key("properties.cluster")
			.stringType()
			.noDefaultValue()
			.withDescription("Required consumer group in Kafka consumer, no need for Kafka producer");

	// --------------------------------------------------------------------------------------------
	// Scan specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions
			.key("scan.startup.mode")
			.stringType()
			.defaultValue("group-offsets")
			.withDescription("Optional startup mode for Kafka consumer, valid enumerations are "
					+ "\"earliest-offset\", \"latest-offset\", \"group-offsets\", \"timestamp\"\n"
					+ "or \"specific-offsets\"");

	public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS = ConfigOptions
			.key("scan.startup.specific-offsets")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional offsets used in case of \"specific-offsets\" startup mode");

	public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS = ConfigOptions
			.key("scan.startup.timestamp-millis")
			.longType()
			.noDefaultValue()
			.withDescription("Optional timestamp used in case of \"timestamp\" startup mode");

	public static final ConfigOption<String> SCAN_PARTITION_RANGE = ConfigOptions
			.key("scan.source-partition-range")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional kafka source partition range");

	public static final ConfigOption<Long> SCAN_SOURCE_SAMPLE_INTERVAL = ConfigOptions
			.key("scan.source-sample-interval")
			.longType()
			.noDefaultValue()
			.withDescription("Optional source sample interval.");

	public static final ConfigOption<String> SCAN_CONSUMER_FACTORY_CLASS = ConfigOptions
		.key("scan.consumer-factory-class")
		.stringType()
		.defaultValue(KafkaConsumerFactory.DefaultKafkaConsumerFactory.class.getName())
		.withDescription("Factory class used to build kafka consumer");

	public static final ConfigOption<Long> SCAN_SOURCE_SAMPLE_NUM = ConfigOptions
			.key("scan.source-sample-num")
			.longType()
			.noDefaultValue()
			.withDescription("Optional source sample num.");

	public static final ConfigOption<Long> SCAN_RELATIVE_OFFSET = ConfigOptions
			.key("scan.relative-offset")
			.longType()
			.noDefaultValue()
			.withDescription("Optional. Kafka relative offset.");

	public static final ConfigOption<Boolean> SCAN_RESET_TO_EARLIEST_FOR_NEW_PARTITION = ConfigOptions
			.key("scan.reset-to-earliest-for-new-partition")
			.booleanType()
			.noDefaultValue()
			.withDescription("Optional reset to earliest for new partition.");

	public static final ConfigOption<Duration> SCAN_MANUALLY_COMMIT_OFFSET_INTERVAL = ConfigOptions
		.key("scan.manually-commit-offsets-interval")
		.durationType()
		.noDefaultValue()
		.withDescription("Optional commit offset interval.");

	public static final ConfigOption<Boolean> SCAN_START_IGNORE_STATE_OFFSETS = ConfigOptions
		.key("scan.start.ignore-state-offsets")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional ignore kafka offsets in state during start and rely on scan.startup.mode to init kafka offsets");

	public static final ConfigOption<Boolean> SCAN_FORCE_MANUAL_COMMIT_OFFSETS = ConfigOptions
		.key("scan.force.manually-commit-offsets")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional ignore whether the checkpoint is successful, and force commit offset");

	public static final ConfigOption<Boolean> SCAN_ENABLE_PROJECTION_PUSHDOWN = ConfigOptions
		.key("scan.enable-projection-pushdown")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional decide whether table scan is applicable to pushdown projection.");

	// --------------------------------------------------------------------------------------------
	// Sink specific options
	// --------------------------------------------------------------------------------------------

	public static final ConfigOption<String> SINK_PARTITIONER_CLASS = ConfigOptions
			.key("sink.partition-class")
			.stringType()
			.noDefaultValue()
			.withDeprecatedKeys("flink_1_9.sink-partitioner-class")
			.withDescription("Optional. specify custom partitioner class.");

	public static final ConfigOption<String> SINK_PARTITIONER = ConfigOptions
			.key("sink.partitioner")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional output partitioning from Flink's partitions\n"
					+ "into Kafka's partitions valid enumerations are\n"
					+ "\"fixed\": (each Flink partition ends up in at most one Kafka partition),\n"
					+ "\"round-robin\": (a Flink partition is distributed to Kafka partitions round-robin),\n"
					+ "\"row-fields-hash\": (a Flink partition is partitioned by specific field)\n"
					+ "\"custom class name\": (use a custom FlinkKafkaPartitioner subclass)");

	public static final ConfigOption<String> SINK_LOG_FAILURE_ONLY = ConfigOptions
			.key("sink.log-failures-only")
			.stringType()
			.noDefaultValue()
			.withDescription("Optional whether only log when write kafka failed.");

	public static final ConfigOption<Integer> SINK_IN_FLIGHT_BATCH_SIZE_FACTOR = ConfigOptions
			.key("sink.in-flight-batch-size-factor")
			.intType()
			.defaultValue(0)
			.withDescription("Optional the factor of in-flight data size. Max in-flight size is in-flight-factor times kafkaBatchSize.");

	public static final ConfigOption<Integer> SINK_IN_FLIGHT_MAX_NUM = ConfigOptions
		.key("sink.in-flight-max-num")
		.intType()
		.defaultValue(0)
		.withDescription("Optional number of in-flight records.");

	public static final ConfigOption<String> SINK_PRODUCER_FACTORY_CLASS = ConfigOptions
		.key("sink.producer-factory-class")
		.stringType()
		.defaultValue(KafkaProducerFactory.DefaultKafkaProducerFactory.class.getName())
		.withDescription("Factory class used to build kafka producer.");


	// --------------------------------------------------------------------------------------------
	// Option enumerations
	// --------------------------------------------------------------------------------------------

	// Start up offset.
	public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
	public static final String SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS = "group-offsets";
	public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS = "specific-offsets";
	public static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

	private static final Set<String> SCAN_STARTUP_MODE_ENUMS = new HashSet<>(Arrays.asList(
			SCAN_STARTUP_MODE_VALUE_EARLIEST,
			SCAN_STARTUP_MODE_VALUE_LATEST,
			SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS,
			SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS,
			SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

	// Sink partitioner.
	public static final String SINK_PARTITIONER_VALUE_FIXED = "fixed";
	public static final String SINK_PARTITIONER_VALUE_ROUND_ROBIN = "round-robin";
	public static final String SINK_PARTITIONER_ROW_FIELD_HASH = "row-fields-hash";
	public static final String SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY = "custom-class-with-keyby";

	private static final Set<String> SINK_PARTITIONER_ENUMS = new HashSet<>(Arrays.asList(
			SINK_PARTITIONER_VALUE_FIXED,
			SINK_PARTITIONER_VALUE_ROUND_ROBIN));

	// Prefix for Kafka specific properties.
	public static final String PROPERTIES_PREFIX = "properties.";

	// Other keywords.
	private static final String PARTITION = "partition";
	private static final String OFFSET = "offset";
	private static final String KAFKA_OWNER = "owner";
	private static final String KAFKA_PSM = "psm";
	private static final String KAFKA_TEAM = "team";

	// --------------------------------------------------------------------------------------------
	// Validation
	// --------------------------------------------------------------------------------------------

	public static void validateTableOptions(ReadableConfig tableOptions) {
		validateScanStartupMode(tableOptions);
		validateSinkPartitioner(tableOptions);
	}

	private static void validateScanStartupMode(ReadableConfig tableOptions) {
		tableOptions.getOptional(SCAN_STARTUP_MODE)
				.map(String::toLowerCase)
				.ifPresent(mode -> {
					if (!SCAN_STARTUP_MODE_ENUMS.contains(mode)) {
						throw new ValidationException(
								String.format("Invalid value for option '%s'. Supported values are %s, but was: %s",
										SCAN_STARTUP_MODE.key(),
										"[earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp]",
										mode));
					}

					if (mode.equals(SCAN_STARTUP_MODE_VALUE_TIMESTAMP)) {
						if (!tableOptions.getOptional(SCAN_STARTUP_TIMESTAMP_MILLIS).isPresent()) {
							throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
											+ " but missing.",
									SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
									SCAN_STARTUP_MODE_VALUE_TIMESTAMP));
						}
					}
					if (mode.equals(SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS)) {
						if (!tableOptions.getOptional(SCAN_STARTUP_SPECIFIC_OFFSETS).isPresent()) {
							throw new ValidationException(String.format("'%s' is required in '%s' startup mode"
											+ " but missing.",
									SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
									SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS));
						}
						String specificOffsets = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
						parseSpecificOffsets(specificOffsets, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
			}
		});
	}

	private static void validateSinkPartitioner(ReadableConfig tableOptions) {
		tableOptions.getOptional(SINK_PARTITIONER)
				.ifPresent(partitioner -> {
					if (!SINK_PARTITIONER_ENUMS.contains(partitioner.toLowerCase())) {
						if (partitioner.isEmpty()) {
							throw new ValidationException(
									String.format("Option '%s' should be a non-empty string.",
											SINK_PARTITIONER.key()));
						}
					}
				});
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	public static StartupOptions getStartupOptions(
			ReadableConfig tableOptions,
			String topic) {
		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		final StartupMode startupMode = tableOptions.getOptional(SCAN_STARTUP_MODE)
				.map(modeString -> {
					switch (modeString) {
					case SCAN_STARTUP_MODE_VALUE_EARLIEST:
						return StartupMode.EARLIEST;

					case SCAN_STARTUP_MODE_VALUE_LATEST:
						return StartupMode.LATEST;

					case SCAN_STARTUP_MODE_VALUE_GROUP_OFFSETS:
						return StartupMode.GROUP_OFFSETS;

					case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
						buildSpecificOffsets(tableOptions, topic, specificOffsets);
						return StartupMode.SPECIFIC_OFFSETS;

					case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
						return StartupMode.TIMESTAMP;

					default:
						throw new TableException("Unsupported startup mode. Validator should have checked that.");
					}
				}).orElse(StartupMode.GROUP_OFFSETS);
		final StartupOptions options = new StartupOptions();
		options.startupMode = startupMode;
		options.specificOffsets = specificOffsets;
		if (startupMode == StartupMode.TIMESTAMP) {
			options.startupTimestampMillis = tableOptions.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
		}
		return options;
	}

	private static void buildSpecificOffsets(
			ReadableConfig tableOptions,
			String topic,
			Map<KafkaTopicPartition, Long> specificOffsets) {
		String specificOffsetsStrOpt = tableOptions.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
		final Map<Integer, Long> offsetMap = parseSpecificOffsets(
				specificOffsetsStrOpt,
				SCAN_STARTUP_SPECIFIC_OFFSETS.key());
		offsetMap.forEach((partition, offset) -> {
			final KafkaTopicPartition topicPartition = new KafkaTopicPartition(topic, partition);
			specificOffsets.put(topicPartition, offset);
		});
	}

	public static Properties getKafkaProperties(Map<String, String> tableOptions, Configuration configuration) {
		final Properties kafkaProperties = new Properties();

		if (hasKafkaClientProperties(tableOptions)) {
			tableOptions.keySet().stream()
					.filter(key -> key.startsWith(PROPERTIES_PREFIX))
					.forEach(key -> {
						final String value = tableOptions.get(key);
						final String subKey = key.substring((PROPERTIES_PREFIX).length());
						kafkaProperties.put(subKey, value);
					});
		}

		String owner = System.getProperty(ConfigConstants.FLINK_OWNER_KEY,
			ConfigConstants.FLINK_OWNER_DEFAULT);
		String jobName = System.getProperty(ConfigConstants.JOB_NAME_KEY,
			ConfigConstants.JOB_NAME_DEFAULT);
		if (!kafkaProperties.containsKey(KAFKA_OWNER)) {
			kafkaProperties.put(KAFKA_OWNER, owner);
		}

		if (!kafkaProperties.containsKey(KAFKA_TEAM)) {
			kafkaProperties.put(KAFKA_TEAM, String.format(ConfigConstants.FLINK_TEAM_TEMPLATE, owner));
		}

		if (!kafkaProperties.containsKey(KAFKA_PSM)) {
			kafkaProperties.put(KAFKA_PSM, String.format(ConfigConstants.FLINK_PSM_TEMPLATE, jobName));
		}

		if (configuration.contains(ExecutionConfigOptions.TABLE_MQ_SPECIFIED_GROUP_PREFIX)) {
			String specifiedGroupPrefix = configuration.getString(ExecutionConfigOptions.TABLE_MQ_SPECIFIED_GROUP_PREFIX);
			if (!kafkaProperties.containsKey("group.id")
				|| !kafkaProperties.getProperty("group.id").startsWith(specifiedGroupPrefix)) {
				throw new FlinkRuntimeException("The mq group must prefix with: " + specifiedGroupPrefix
					+ ", maybe usage scene is session debug, means may have bug in the process.");
			}
		}

		return kafkaProperties;
	}

	/**
	 * The partitioner can be either "fixed", "round-robin" or a customized partitioner full class name.
	 */
	public static Optional<FlinkKafkaPartitioner<RowData>> getFlinkKafkaPartitioner(
			ReadableConfig tableOptions,
			ClassLoader classLoader,
			TableSchema schema) {
		return tableOptions.getOptional(SINK_PARTITIONER)
				.flatMap((String partitioner) -> {
					switch (partitioner) {
					case SINK_PARTITIONER_VALUE_FIXED:
						return Optional.of(new FlinkFixedPartitioner<>());
					case SINK_PARTITIONER_VALUE_ROUND_ROBIN:
						return Optional.empty();
					case SINK_PARTITIONER_ROW_FIELD_HASH:
						String keybyFieldNames =
							tableOptions.getOptional(SINK_PARTITIONER_FIELD)
										.orElseThrow(
											() -> new IllegalArgumentException(
												String.format("Use '%s' must specific field '%s'",
													SINK_PARTITIONER_ROW_FIELD_HASH, SINK_PARTITIONER_FIELD.key())));

						int[] fieldIndexList = schema.getIndexListFromFieldNames(keybyFieldNames);
						DataType[] dataTypes = new DataType[fieldIndexList.length];
						for (int i = 0; i < dataTypes.length; i++) {
							dataTypes[i] = schema.getFieldDataType(fieldIndexList[i]).get();
						}
						return Optional.of(new FlinkRowDataFieldHashPartitioner(fieldIndexList, dataTypes));
					case SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY:
						String customKeybyFieldNames =
							tableOptions.getOptional(SINK_PARTITIONER_FIELD)
										.orElseThrow(
											() -> new IllegalArgumentException(
												String.format("Use '%s' must specific field '%s'",
													SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY, SINK_PARTITIONER_FIELD.key())));

						String customClassName =
							tableOptions.getOptional(SINK_PARTITIONER_CLASS)
										.orElseThrow(
											() -> new IllegalArgumentException(
												String.format("Use '%s' must specific class by using '%s'",
													SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY, SINK_PARTITIONER_CLASS.key())));

						int[] customFieldIndexList = schema.getIndexListFromFieldNames(customKeybyFieldNames);

						DataType[] customDataTypes = new DataType[customFieldIndexList.length];
						for (int i = 0; i < customDataTypes.length; i++) {
							customDataTypes[i] = schema.getFieldDataType(customFieldIndexList[i]).get();
						}
						return Optional.of(initializePartitionerWithKeyby(customClassName, customFieldIndexList, customDataTypes, classLoader));
					// Default fallback to full class name of the partitioner.
					default:
						return Optional.of(initializePartitioner(partitioner, classLoader));
					}
				});
	}

	/**
	 * Parses SpecificOffsets String to Map.
	 *
	 * <p>SpecificOffsets String format was given as following:
	 *
	 * <pre>
	 *     scan.startup.specific-offsets = partition:0,offset:42;partition:1,offset:300
	 * </pre>
	 *
	 * @return SpecificOffsets with Map format, key is partition, and value is offset
	 */
	public static Map<Integer, Long> parseSpecificOffsets(
			String specificOffsetsStr,
			String optionKey) {
		final Map<Integer, Long> offsetMap = new HashMap<>();
		final String[] pairs = specificOffsetsStr.split(";");
		final String validationExceptionMessage = String.format(
				"Invalid properties '%s' should follow the format "
						+ "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
				optionKey,
				specificOffsetsStr);

		if (pairs.length == 0) {
			throw new ValidationException(validationExceptionMessage);
		}

		for (String pair : pairs) {
			if (null == pair || pair.length() == 0 || !pair.contains(",")) {
				throw new ValidationException(validationExceptionMessage);
			}

			final String[] kv = pair.split(",");
			if (kv.length != 2 ||
					!kv[0].startsWith(PARTITION + ':') ||
					!kv[1].startsWith(OFFSET + ':')) {
				throw new ValidationException(validationExceptionMessage);
			}

			String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
			String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
			try {
				final Integer partition = Integer.valueOf(partitionValue);
				final Long offset = Long.valueOf(offsetValue);
				offsetMap.put(partition, offset);
			} catch (NumberFormatException e) {
				throw new ValidationException(validationExceptionMessage, e);
			}
		}
		return offsetMap;
	}

	/** Decides if the table options contains Kafka client properties that start with prefix 'properties'. */
	private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
		return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
	}

	/**
	 * Returns a class value with the given class name.
	 */
	private static <T> FlinkKafkaPartitioner<T> initializePartitioner(String name, ClassLoader classLoader) {
		try {
			Class<?> clazz = Class.forName(name, true, classLoader);
			if (!FlinkKafkaPartitioner.class.isAssignableFrom(clazz)) {
				throw new ValidationException(
						String.format("Sink partitioner class '%s' should extend from the required class %s",
								name,
								FlinkKafkaPartitioner.class.getName()));
			}
			@SuppressWarnings("unchecked")
			final FlinkKafkaPartitioner<T> kafkaPartitioner = InstantiationUtil.instantiate(name, FlinkKafkaPartitioner.class, classLoader);

			return kafkaPartitioner;
		} catch (ClassNotFoundException | FlinkException e) {
			throw new ValidationException(
					String.format("Could not find and instantiate partitioner class '%s'", name), e);
		}
	}

	/**
	 * Returns a class value with the given class name and an int array constructor parameter.
	 */
	private static <T> FlinkKafkaPartitioner<T> initializePartitionerWithKeyby(String name, int[] fieldIndexList, DataType[] customDataTypes, ClassLoader classLoader) {
		try {
			Class<?> clazz = Class.forName(name, true, classLoader);
			if (!FlinkKafkaPartitioner.class.isAssignableFrom(clazz)) {
				throw new ValidationException(
					String.format("Sink partitioner class '%s' should extend from the required class %s",
						name,
						FlinkKafkaPartitioner.class.getName()));
			}

			try {
				Constructor<?> constructor = clazz.getConstructor(int[].class, DataType[].class);
				return  (FlinkKafkaPartitioner<T>) constructor.newInstance(fieldIndexList, customDataTypes);
			} catch (NoSuchMethodException e) {
				throw new TableException("No appropriate constructor for \"sink.partition-fields\" in \"connector.sink-partitioner-class\". class: " + name);
			} catch (Exception e) {
				throw new TableException("Exception on creating custom class: " + name + ", error: " + e.getMessage());
			}
		} catch (ClassNotFoundException e) {
			throw new ValidationException(
				String.format("Could not find and instantiate partitioner class '%s'", name), e);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Inner classes
	// --------------------------------------------------------------------------------------------

	/** Kafka startup options. **/
	public static class StartupOptions {
		public StartupMode startupMode;
		public Map<KafkaTopicPartition, Long> specificOffsets;
		public long startupTimestampMillis;
	}
}