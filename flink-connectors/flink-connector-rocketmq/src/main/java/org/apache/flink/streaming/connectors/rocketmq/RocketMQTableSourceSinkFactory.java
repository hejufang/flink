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

package org.apache.flink.streaming.connectors.rocketmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.rocketmq.selector.DefaultTopicSelector;
import org.apache.flink.streaming.connectors.rocketmq.selector.TopicSelector;
import org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ASYNC_MODE_ENABLED;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_BATCH_FLUSH_ENABLED;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_CONSUMER_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_CONSUMER_OFFSET_FROM_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_CONSUMER_OFFSET_RESET_TO;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_CONSUMER_TAG;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_CONSUMER_TOPIC;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_PRODUCER_GROUP;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_PRODUCER_TAG;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_PRODUCER_TOPIC;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ROCKETMQ_CONSUMER_PSM;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ROCKETMQ_PRODUCER_PSM;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_ROCKETMQ_PROPERTIES;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_STARTUP_MODE_FROM_TIMESTAMP;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_TOPIC_SELECTOR;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_TYPE_VALUE_ROCKETMQ;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONSUMER;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.DEFAULT_TOPIC_SELECTOR;
import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.PRODUCER;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_KEYBY_FIELDS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_LOG_FAILURES_ONLY;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
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

/**
 * RocketMQ table source sink factory.
 */
public class RocketMQTableSourceSinkFactory implements StreamTableSourceFactory<Row>,
	StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ROCKETMQ);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN);
		properties.add(CONNECTOR_ROCKETMQ_CONSUMER_PSM);
		properties.add(CONNECTOR_ROCKETMQ_PRODUCER_PSM);
		properties.add(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP);
		properties.add(CONNECTOR_CONSUMER_GROUP);
		properties.add(CONNECTOR_CONSUMER_TOPIC);
		properties.add(CONNECTOR_CONSUMER_TAG);
		properties.add(CONNECTOR_CONSUMER_OFFSET_RESET_TO);
		properties.add(CONNECTOR_STARTUP_MODE);
		properties.add(CONNECTOR_STARTUP_MODE_FROM_TIMESTAMP);
		properties.add(CONNECTOR_CONSUMER_OFFSET_FROM_TIMESTAMP);
		properties.add(CONNECTOR_PRODUCER_GROUP);
		properties.add(CONNECTOR_PRODUCER_TOPIC);
		properties.add(CONNECTOR_PRODUCER_TAG);
		properties.add(CONNECTOR_PARALLELISM);
		properties.add(CONNECTOR_TOPIC_SELECTOR);
		properties.add(CONNECTOR_BATCH_FLUSH_ENABLED);
		properties.add(CONNECTOR_BATCH_SIZE);
		properties.add(CONNECTOR_ASYNC_MODE_ENABLED);

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
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties, PRODUCER);

		final SerializationSchema<Row> serializationSchema = TableConnectorUtils.getSerializationSchema(
			descriptorProperties.asMap(), this.getClass().getClassLoader());
		Map<String, String> configurations = getOtherConfigurations(descriptorProperties);
		TopicSelector<Row> topicSelector = getTopicSelector(descriptorProperties);
		RocketMQOptions options = getRocketMQOptions(descriptorProperties);

		return RocketMQUpsertTableSink.builder().setSchema(descriptorProperties.getTableSchema(SCHEMA))
			.setConfigurations(configurations)
			.setProperties(getRocketMQProperties(descriptorProperties))
			.setTopicSelector(topicSelector)
			.setSerializationSchema(serializationSchema)
			.setOptions(options)
			.build();
	}

	/**
	 * Get topic selector, only DefaultTopicSelector is supported currently.
	 */
	private TopicSelector<Row> getTopicSelector(DescriptorProperties descriptorProperties) {
		TopicSelector<Row> topicSelector;
		String topicSelectorType =
			descriptorProperties.getOptionalString(CONNECTOR_TOPIC_SELECTOR).orElse(DEFAULT_TOPIC_SELECTOR);
		switch (topicSelectorType) {
			case DEFAULT_TOPIC_SELECTOR:
				String topicName =
					descriptorProperties.getOptionalString(CONNECTOR_PRODUCER_TOPIC).orElseThrow(
						() -> new FlinkRuntimeException(String.format("%s must be set when use %s.",
							CONNECTOR_PRODUCER_TOPIC, DEFAULT_TOPIC_SELECTOR)));
				String tagName =
					descriptorProperties.getOptionalString(CONNECTOR_PRODUCER_TAG).orElse("");
				topicSelector = new DefaultTopicSelector<>(topicName, tagName);
				break;
			default:
				throw new FlinkRuntimeException(
					String.format("Unsupported topic selector: %s, supported selector: %s",
						topicSelectorType, Collections.singleton(DEFAULT_TOPIC_SELECTOR)));
		}
		return topicSelector;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties, CONSUMER);

		final DeserializationSchema<Row> deserializationSchema = TableConnectorUtils.getDeserializationSchema(
			descriptorProperties.asMap(), this.getClass().getClassLoader());
		Map<String, String> configurations = getOtherConfigurations(descriptorProperties);

		return RocketMQTableSource.builder()
			.setSchema(descriptorProperties.getTableSchema(SCHEMA))
			.setProctimeAttribute(SchemaValidator.deriveProctimeAttribute(descriptorProperties).orElse(null))
			.setRowtimeAttributeDescriptors(SchemaValidator.deriveRowtimeAttributes(descriptorProperties))
			.setFieldMapping(Optional.of(SchemaValidator.deriveFieldMapping(
				descriptorProperties,
				Optional.of(deserializationSchema.getProducedType()))).orElse(null))
			.setProperties(getRocketMQProperties(descriptorProperties))
			.setConfigurations(configurations)
			.setDeserializationSchema(deserializationSchema)
			.build();
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties, String type) {
		// The origin properties is an UnmodifiableMap, so we create a new one.
		Map<String, String> newProperties = new HashMap<>(properties);
		addDefaultProperties(newProperties, type);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(newProperties);

		new SchemaValidator(true, true, false).validate(descriptorProperties);
		new RocketMQValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private Properties getRocketMQProperties(DescriptorProperties descriptorProperties) {
		final Properties rocketMQProperties = new Properties();

		// Replace mainProperties with short keys (cut the prefix 'connector.'),
		// for example <'connector.rocketmq.namesrv.domain', 'cluster1'> -> <'rocketmq.namesrv.domain', 'cluster1'>
		List<String> mainProperties = getMainProperties();
		int prefixLength = (CONNECTOR + ".").length();

		for (String prop : mainProperties) {
			descriptorProperties.getOptionalString(prop)
				.ifPresent(v -> rocketMQProperties.put(prop.substring(prefixLength), v));
		}

		// Replace dynamic properties with short keys (cut the prefix 'connector.rocketmq.properties')
		// for example <'connector.rocketmq.properties.producer.retry.times', '3'> -> <'producer.retry.times', '3'>
		int dynamicPrefixLength = (CONNECTOR_ROCKETMQ_PROPERTIES + ".").length();

		descriptorProperties.asMap().entrySet().stream()
			.filter(e -> e.getKey().startsWith(CONNECTOR_ROCKETMQ_PROPERTIES + "."))
			.forEach(e -> rocketMQProperties.put(e.getKey().substring(dynamicPrefixLength), e.getValue()));

		return rocketMQProperties;
	}

	private List<String> getMainProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN);
		properties.add(CONNECTOR_ROCKETMQ_CONSUMER_PSM);
		properties.add(CONNECTOR_ROCKETMQ_PRODUCER_PSM);
		properties.add(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP);
		properties.add(CONNECTOR_CONSUMER_GROUP);
		properties.add(CONNECTOR_CONSUMER_TOPIC);
		properties.add(CONNECTOR_PRODUCER_GROUP);
		properties.add(CONNECTOR_CONSUMER_TAG);
		properties.add(CONNECTOR_CONSUMER_OFFSET_RESET_TO);
		properties.add(CONNECTOR_STARTUP_MODE);
		properties.add(CONNECTOR_STARTUP_MODE_FROM_TIMESTAMP);
		properties.add(CONNECTOR_CONSUMER_OFFSET_FROM_TIMESTAMP);
		return properties;
	}

	/**
	 * Add default psm info to rocketmq properties.
	 *
	 * @param properties rocketmq properties.
	 */
	private void addDefaultProperties(Map<String, String> properties, String type) {
		String jobName = System.getProperty(ConfigConstants.JOB_NAME_KEY,
			ConfigConstants.JOB_NAME_DEFAULT);
		String defaultPsm = String.format(ConfigConstants.FLINK_PSM_TEMPLATE, jobName);
		if (CONSUMER.equals(type) && !properties.containsKey(CONNECTOR_ROCKETMQ_CONSUMER_PSM)) {
			properties.put(CONNECTOR_ROCKETMQ_CONSUMER_PSM, defaultPsm);
		}
		if (PRODUCER.equals(type) && !properties.containsKey(CONNECTOR_ROCKETMQ_PRODUCER_PSM)) {
			properties.put(CONNECTOR_ROCKETMQ_PRODUCER_PSM, defaultPsm);
		}
	}

	private Map<String, String> getOtherConfigurations(DescriptorProperties descriptorProperties) {
		Map<String, String> configurations = new HashMap<>();

		descriptorProperties.getOptionalString(CONNECTOR_PARALLELISM)
			.ifPresent(p -> configurations.put(CONNECTOR_PARALLELISM, p));
		descriptorProperties.getOptionalString(CONNECTOR_KEYBY_FIELDS)
			.ifPresent(f -> configurations.put(CONNECTOR_KEYBY_FIELDS, f));
		descriptorProperties.getOptionalString(CONNECTOR_LOG_FAILURES_ONLY)
			.ifPresent(l -> configurations.put(CONNECTOR_LOG_FAILURES_ONLY, l));
		return configurations;
	}

	private RocketMQOptions getRocketMQOptions(DescriptorProperties descriptorProperties) {
		RocketMQOptions.RocketMQOptionsBuilder builder =
			new RocketMQOptions.RocketMQOptionsBuilder();
		descriptorProperties.getOptionalBoolean(CONNECTOR_BATCH_FLUSH_ENABLED)
			.ifPresent(builder::setBatchFlushOnCheckpoint);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE)
			.ifPresent(builder::setBatchSize);
		descriptorProperties.getOptionalBoolean(CONNECTOR_ASYNC_MODE_ENABLED)
			.ifPresent(builder::setAsync);
		return builder.build();
	}
}
