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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkRowFieldHashPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.KafkaValidator;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_KEYBY_FIELDS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_LOG_FAILURES_ONLY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_GROUP_ID;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_KAFKA_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS_DEFAULT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_OWNER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PSM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RATE_LIMITING_NUM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RATE_LIMITING_UNIT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RELATIVE_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_IGNORE_TRANSACTION_TIMEOUT;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_CLASS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_FIXED;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_PARTITIONER_VALUE_ROW_FIELDS_HASH;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SINK_SEMANTIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_PARTITION_RANGE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_SAMPLE_INTERVAL;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SOURCE_SAMPLE_NUM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_OFFSET;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_OFFSETS_PARTITION;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_SPECIFIC_TIMESTAMP;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TEAM;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TOPIC;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_TYPE_VALUE_KAFKA;
import static org.apache.flink.table.descriptors.KafkaValidator.DISABLE_CURRENT_OFFSET_RATE_METRICS;
import static org.apache.flink.table.descriptors.KafkaValidator.METADATA_FIELDS_MAPPING;
import static org.apache.flink.table.descriptors.KafkaValidator.METADATA_FIELD_INDEX_MAPPING;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_PARAMETERS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;

/**
 * Abstract kafka source&sink factory base.
 */
public abstract class AbstractKafkaTableSourceSinkFactoryBase<T> implements
		StreamTableSourceFactory<Row>,
		StreamTableSinkFactory<T>{

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_KAFKA); // kafka
		context.put(CONNECTOR_VERSION, kafkaVersion()); // version
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_PROPERTY_VERSION);

		// kafka
		properties.add(CONNECTOR_TOPIC);
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_PSM);
		properties.add(CONNECTOR_TEAM);
		properties.add(CONNECTOR_OWNER);
		properties.add(CONNECTOR_GROUP_ID);
		properties.add(CONNECTOR_KAFKA_PROPERTIES + ".*");
		properties.add(CONNECTOR_PROPERTIES);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
		properties.add(CONNECTOR_STARTUP_MODE);
		properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_PARTITION);
		properties.add(CONNECTOR_SPECIFIC_OFFSETS + ".#." + CONNECTOR_SPECIFIC_OFFSETS_OFFSET);
		properties.add(CONNECTOR_SINK_PARTITIONER);
		properties.add(CONNECTOR_SINK_PARTITIONER_CLASS);
		properties.add(CONNECTOR_RELATIVE_OFFSET);
		properties.add(CONNECTOR_SPECIFIC_TIMESTAMP);
		properties.add(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION);
		properties.add(CONNECTOR_LOG_FAILURES_ONLY);

		// other configurations
		properties.add(CONNECTOR_PARALLELISM);
		properties.add(CONNECTOR_KEYBY_FIELDS);
		properties.add(CONNECTOR_RATE_LIMITING_NUM);
		properties.add(CONNECTOR_RATE_LIMITING_UNIT);
		properties.add(CONNECTOR_SOURCE_PARTITION_RANGE);
		properties.add(CONNECTOR_SOURCE_SAMPLE_INTERVAL);
		properties.add(CONNECTOR_SOURCE_SAMPLE_NUM);
		properties.add(METADATA_FIELDS_MAPPING);
		properties.add(DISABLE_CURRENT_OFFSET_RATE_METRICS);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		// time attributes
		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_PARAMETERS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		// format wildcard
		properties.add(FORMAT + ".*");

		return properties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final DeserializationSchema<Row> deserializationSchema = TableConnectorUtils.getDeserializationSchema(
			descriptorProperties.asMap(), this.getClass().getClassLoader());
		final StartupOptions startupOptions = getStartupOptions(descriptorProperties, topic);
		Map<String, String> configurations = getOtherConfigurations(descriptorProperties);

		return createKafkaTableSource(
			descriptorProperties.getTableSchema(SCHEMA),
			SchemaValidator.deriveProctimeAttribute(descriptorProperties),
			SchemaValidator.deriveRowtimeAttributes(descriptorProperties),
			SchemaValidator.deriveFieldMapping(
				descriptorProperties,
				Optional.of(deserializationSchema.getProducedType())),
			topic,
			getKafkaProperties(descriptorProperties),
			deserializationSchema,
			startupOptions.startupMode,
			startupOptions.specificOffsets,
			startupOptions.relativeOffset,
			startupOptions.timestamp,
			configurations);
	}

	@Override
	public StreamTableSink<T> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
		final String topic = descriptorProperties.getString(CONNECTOR_TOPIC);
		final Optional<String> proctime =
			SchemaValidator.deriveProctimeAttribute(descriptorProperties);
		final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors =
			SchemaValidator.deriveRowtimeAttributes(descriptorProperties);
		String updateMode =
			descriptorProperties.getOptionalString(UPDATE_MODE).orElse(UPDATE_MODE_VALUE_APPEND);
		Map<String, String> configurations = getOtherConfigurations(descriptorProperties);

		// see also FLINK-9870
		if (proctime.isPresent() || !rowtimeAttributeDescriptors.isEmpty() ||
				checkForCustomFieldMapping(descriptorProperties, schema)) {
			throw new TableException("Time attributes and custom field " +
				"mappings are not supported yet.");
		}
		return createKafkaTableSink(
			schema,
			topic,
			getKafkaProperties(descriptorProperties),
			getFlinkKafkaPartitioner(descriptorProperties),
			TableConnectorUtils.getSerializationSchema(descriptorProperties.asMap(), this.getClass().getClassLoader()),
			updateMode,
			configurations);
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific factories
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the Kafka version.
	 */
	protected abstract String kafkaVersion();

	/**
	 * True if the Kafka source supports Kafka timestamps, false otherwise.
	 *
	 * @return True if the Kafka source supports Kafka timestamps, false otherwise.
	 */
	protected abstract boolean supportsKafkaTimestamps();

	/**
	 * Constructs the version-specific Kafka table source.
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
	 */
	protected abstract KafkaTableSourceBase createKafkaTableSource(
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
		Map<String, String> configuration);

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param schema      Schema of the produced table.
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param partitioner Partitioner to select Kafka partition for each item.
	 */
	protected abstract StreamTableSink<T> createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		Optional<FlinkKafkaPartitioner<Row>> partitioner,
		SerializationSchema<Row> serializationSchema,
		String updateMode,
		Map<String, String> configuration);

	// --------------------------------------------------------------------------------------------
	// Helper methods
	// --------------------------------------------------------------------------------------------

	protected DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		// The origin properties is an UnmodifiableMap, so we create a new one.
		Map<String, String> newProperties = new HashMap<>(properties);
		addDefaultProperties(newProperties);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(newProperties);

		// allow Kafka timestamps to be used, watermarks can not be received from source
		new SchemaValidator(true, supportsKafkaTimestamps(), false).validate(descriptorProperties);
		new KafkaValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	protected Map<String, String> getOtherConfigurations(DescriptorProperties descriptorProperties) {
		Map<String, String> configurations = new HashMap<>();

		descriptorProperties.getOptionalString(CONNECTOR_PARALLELISM)
			.ifPresent(p -> configurations.put(CONNECTOR_PARALLELISM, p));
		descriptorProperties.getOptionalString(CONNECTOR_KEYBY_FIELDS)
			.ifPresent(f -> configurations.put(CONNECTOR_KEYBY_FIELDS, f));
		descriptorProperties.getOptionalString(CONNECTOR_LOG_FAILURES_ONLY)
			.ifPresent(l -> configurations.put(CONNECTOR_LOG_FAILURES_ONLY, l));
		descriptorProperties.getOptionalString(CONNECTOR_RATE_LIMITING_NUM)
			.ifPresent(n -> configurations.put(CONNECTOR_RATE_LIMITING_NUM, n));
		descriptorProperties.getOptionalString(CONNECTOR_RATE_LIMITING_UNIT)
			.ifPresent(u -> configurations.put(CONNECTOR_RATE_LIMITING_UNIT, u));
		descriptorProperties.getOptionalString(CONNECTOR_SOURCE_SAMPLE_NUM)
			.ifPresent(sn -> configurations.put(CONNECTOR_SOURCE_SAMPLE_NUM, sn));
		descriptorProperties.getOptionalString(CONNECTOR_SOURCE_SAMPLE_INTERVAL)
			.ifPresent(si -> configurations.put(CONNECTOR_SOURCE_SAMPLE_INTERVAL, si));
		descriptorProperties.getOptionalString(METADATA_FIELD_INDEX_MAPPING)
			.ifPresent(mf -> configurations.put(METADATA_FIELD_INDEX_MAPPING, mf));
		descriptorProperties.getOptionalBoolean(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION).ifPresent(value ->
			configurations.put(CONNECTOR_RESET_TO_EARLIEST_FOR_NEW_PARTITION, String.valueOf(value)));
		descriptorProperties.getOptionalBoolean(DISABLE_CURRENT_OFFSET_RATE_METRICS).ifPresent(value ->
			configurations.put(DISABLE_CURRENT_OFFSET_RATE_METRICS, String.valueOf(value)));
		descriptorProperties.getOptionalString(CONNECTOR_SOURCE_PARTITION_RANGE).ifPresent(value ->
			configurations.put(CONNECTOR_SOURCE_PARTITION_RANGE, value));
		descriptorProperties.getOptionalBoolean(CONNECTOR_SINK_IGNORE_TRANSACTION_TIMEOUT).ifPresent(value ->
			configurations.put(CONNECTOR_SINK_IGNORE_TRANSACTION_TIMEOUT, String.valueOf(value)));
		return configurations;
	}

	protected Properties getKafkaProperties(DescriptorProperties descriptorProperties) {
		final Properties kafkaProperties = new Properties();
		final List<Map<String, String>> propsList = descriptorProperties.getFixedIndexedProperties(
			CONNECTOR_PROPERTIES,
			Arrays.asList(CONNECTOR_PROPERTIES_KEY, CONNECTOR_PROPERTIES_VALUE));
		propsList.forEach(kv -> kafkaProperties.put(
			descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_KEY)),
			descriptorProperties.getString(kv.get(CONNECTOR_PROPERTIES_VALUE))
		));

		// Replace mainProperties with short keys (cut the prefix 'connector.'),
		// for example <'connector.cluster', 'cluster1'> -> <'cluster', 'cluster1'>
		List<String> mainProperties = getMainProperties();
		int prefixLength = (CONNECTOR + ".").length();
		mainProperties.forEach(p -> descriptorProperties.getOptionalString(p).ifPresent(
			v -> kafkaProperties.put(p.substring(prefixLength), v)));

		// Replace dynamic properties with short keys (cut the prefix 'connector.dynamic.')
		// for example <'connector.dynamic.ignore.dc.check', 'true'> -> <'ignore.dc.check', 'true'>
		int dynamicPrefixLength = (CONNECTOR_KAFKA_PROPERTIES + ".").length();
		descriptorProperties.asMap().entrySet().forEach(entry -> {
			if (entry.getKey().startsWith(CONNECTOR_KAFKA_PROPERTIES + ".")) {
				kafkaProperties.put(
					entry.getKey().substring(dynamicPrefixLength), entry.getValue());
			}
		});
		return kafkaProperties;
	}

	/**
	 * Add default psm, owner, team info to kafka properties.
	 *
	 * @param properties kafka properties.
	 * */
	private void addDefaultProperties(Map<String, String> properties) {
		String owner = System.getProperty(ConfigConstants.FLINK_OWNER_KEY,
			ConfigConstants.FLINK_OWNER_DEFAULT);
		String jobName = System.getProperty(ConfigConstants.JOB_NAME_KEY,
			ConfigConstants.JOB_NAME_DEFAULT);
		if (!properties.containsKey(CONNECTOR_PSM)) {
			properties.put(CONNECTOR_PSM,
				String.format(ConfigConstants.FLINK_PSM_TEMPLATE, jobName));
		}

		if (!properties.containsKey(CONNECTOR_OWNER)) {
			properties.put(CONNECTOR_OWNER, owner);
		}

		if (!properties.containsKey(CONNECTOR_TEAM)) {
			properties.put(CONNECTOR_TEAM,
				String.format(ConfigConstants.FLINK_TEAM_TEMPLATE, owner));
		}

		if (!properties.containsKey(CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS)) {
			properties.put(CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS,
				CONNECTOR_KAFKA_PROPERTIES_PARTITIONER_CLASS_DEFAULT);
		}
	}

	protected List<String> getMainProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_TOPIC);
		properties.add(CONNECTOR_PSM);
		properties.add(CONNECTOR_TEAM);
		properties.add(CONNECTOR_OWNER);
		properties.add(CONNECTOR_GROUP_ID);
		properties.add(CONNECTOR_SINK_SEMANTIC);
		return properties;
	}

	protected StartupOptions getStartupOptions(
			DescriptorProperties descriptorProperties,
			String topic) {
		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		final StartupMode startupMode = descriptorProperties
			.getOptionalString(CONNECTOR_STARTUP_MODE)
			.map(modeString -> {
				switch (modeString) {
					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_EARLIEST:
						return StartupMode.EARLIEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_LATEST:
						return StartupMode.LATEST;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_GROUP_OFFSETS:
						return StartupMode.GROUP_OFFSETS;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_TIMESTAMP:
						return StartupMode.TIMESTAMP;

					case KafkaValidator.CONNECTOR_STARTUP_MODE_VALUE_SPECIFIC_OFFSETS:
						final List<Map<String, String>> offsetList =
							descriptorProperties.getFixedIndexedProperties(
								CONNECTOR_SPECIFIC_OFFSETS,
								Arrays.asList(CONNECTOR_SPECIFIC_OFFSETS_PARTITION,
									CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
						offsetList.forEach(kv -> {
							final int partition =
								descriptorProperties.getInt(kv.get(CONNECTOR_SPECIFIC_OFFSETS_PARTITION));
							final long offset =
								descriptorProperties.getLong(kv.get(CONNECTOR_SPECIFIC_OFFSETS_OFFSET));
							final KafkaTopicPartition topicPartition =
								new KafkaTopicPartition(topic, partition);
							specificOffsets.put(topicPartition, offset);
						});
						return StartupMode.SPECIFIC_OFFSETS;
					default:
						throw new TableException("Unsupported startup mode. " +
							"Validator should have checked that.");
				}
			}).orElse(StartupMode.GROUP_OFFSETS);
		final StartupOptions options = new StartupOptions();
		options.startupMode = startupMode;
		options.specificOffsets = specificOffsets;
		if (descriptorProperties.containsKey(CONNECTOR_RELATIVE_OFFSET)) {
			options.relativeOffset = descriptorProperties.getLong(CONNECTOR_RELATIVE_OFFSET);
		}
		if (descriptorProperties.containsKey(CONNECTOR_SPECIFIC_TIMESTAMP)) {
			options.timestamp = descriptorProperties.getLong(CONNECTOR_SPECIFIC_TIMESTAMP);
		}
		return options;
	}

	@SuppressWarnings("unchecked")
	private Optional<FlinkKafkaPartitioner<Row>> getFlinkKafkaPartitioner(DescriptorProperties descriptorProperties) {
		return descriptorProperties
			.getOptionalString(CONNECTOR_SINK_PARTITIONER)
			.flatMap((String partitionerString) -> {
				switch (partitionerString) {
					case CONNECTOR_SINK_PARTITIONER_VALUE_FIXED:
						return Optional.of(new FlinkFixedPartitioner<>());
					case CONNECTOR_SINK_PARTITIONER_VALUE_ROUND_ROBIN:
						return Optional.empty();
					case CONNECTOR_SINK_PARTITIONER_VALUE_ROW_FIELDS_HASH:
						TableSchema schema = descriptorProperties.getTableSchema(SCHEMA);
						String keybyFiledNames = descriptorProperties.getString(CONNECTOR_KEYBY_FIELDS);
						return Optional.of(new FlinkRowFieldHashPartitioner(
							getKeybyFieldIndexArray(schema, keybyFiledNames)));
					case CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM:
						final Class<? extends FlinkKafkaPartitioner> partitionerClass =
							descriptorProperties.getClass(CONNECTOR_SINK_PARTITIONER_CLASS, FlinkKafkaPartitioner.class);
						return Optional.of((FlinkKafkaPartitioner<Row>) InstantiationUtil.instantiate(partitionerClass));
					case CONNECTOR_SINK_PARTITIONER_VALUE_CUSTOM_CLASS_WITH_KEYBY:
						final Class<? extends FlinkKafkaPartitioner>
							customClassWithKeybyPartitionerClass =
							descriptorProperties.getClass(CONNECTOR_SINK_PARTITIONER_CLASS, FlinkKafkaPartitioner.class);
						try {
							Constructor<?> constructor = customClassWithKeybyPartitionerClass.getConstructor(int[].class);
							Optional.of((FlinkKafkaPartitioner<Row>) constructor.newInstance(getKeybyFieldIndexArray(
								descriptorProperties.getTableSchema(SCHEMA),
								descriptorProperties.getString(CONNECTOR_KEYBY_FIELDS))));
						} catch (NoSuchMethodException e) {
							throw new TableException("No appropriate constructor for \"connector.keyby-fields\" in custom-class-with-keyby class: " + customClassWithKeybyPartitionerClass.getName());
						} catch (Exception e) {
							throw new TableException("Exception on creating custom class: " + customClassWithKeybyPartitionerClass.getName() + ", error: " + e.getMessage());
						}
					default:
						throw new TableException("Unsupported sink partitioner. " +
							"Validator should have checked that.");
				}
			});
	}

	private static int[] getKeybyFieldIndexArray(TableSchema schema, String keybyFiledNames) {
		String[] keybyFiledNameArray = keybyFiledNames.split(",");
		int [] keybyFieldIndexArray = new int[keybyFiledNameArray.length];

		for (int i = 0; i < keybyFiledNameArray.length; i++) {
			String keybyFieldName = keybyFiledNameArray[i].trim();
			int fieldIndex = schema.getFieldNameIndex(keybyFieldName)
				.orElseThrow(() -> new IllegalArgumentException(
					String.format("keyby field '%s' not found in table. All field names are : %s.",
						keybyFieldName,
						Arrays.asList(schema.getFieldNames()))));
			keybyFieldIndexArray[i] = fieldIndex;
		}

		return keybyFieldIndexArray;
	}

	private boolean checkForCustomFieldMapping(DescriptorProperties descriptorProperties,
		TableSchema schema) {
		final Map<String, String> fieldMapping = SchemaValidator.deriveFieldMapping(
			descriptorProperties,
			// until FLINK-9870 is fixed we assume that the table schema is the output type
			Optional.of(schema.toRowType()));
		return fieldMapping.size() != schema.getFieldNames().length ||
			!fieldMapping.entrySet().stream()
				.allMatch(mapping -> mapping.getKey().equals(mapping.getValue()));
	}

	/**
	 * class for startup options.
	 */
	protected static class StartupOptions {
		protected StartupMode startupMode;
		protected Map<KafkaTopicPartition, Long> specificOffsets;
		protected Long relativeOffset;
		protected Long timestamp;
	}
}
