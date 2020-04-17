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

package org.apache.flink.streaming.connectors.databus;

import org.apache.flink.api.common.serialization.IgnoreRetractMsgSerializationSchem;
import org.apache.flink.api.common.serialization.KeyedSerializationSchema;
import org.apache.flink.api.common.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DatabusValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.RetryManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_CHANNEL;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_DATABUS_MAX_BUFFER_BYTES;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_DATABUS_NEED_RESPONSE;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_RETRY_DELAY_MS;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_RETRY_DELAY_MS_DEFAULT;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_RETRY_MAX_TIMES;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_RETRY_MAX_TIMES_DEFAULT;
import static org.apache.flink.table.descriptors.DatabusValidator.CONNECTOR_RETRY_STRATEGY;
import static org.apache.flink.table.descriptors.DatabusValidator.DATABUS;
import static org.apache.flink.table.descriptors.DatabusValidator.EXPONENTIAL_BACKOFF;
import static org.apache.flink.table.descriptors.DatabusValidator.FIXED_DELAY;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Databus stream table sink factory.
 */
public class DatabusStreamTableSinkFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {
	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		final TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		RetryManager.Strategy retryStrategy = getRetryStrategy(descriptorProperties);
		DatabusOptions.DatabusOptionsBuilder<Tuple2<Boolean, Row>> builder = DatabusOptions.builder();
		builder.setRetryStrategy(retryStrategy);
		builder.setChannel(descriptorProperties.getString(CONNECTOR_CHANNEL));
		builder.setKeyedSerializationSchema(getKeyedSerializationSchema(properties));
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(builder::setParallelism);
		descriptorProperties.getOptionalInt(CONNECTOR_BATCH_SIZE).ifPresent(builder::setBatchSize);
		descriptorProperties.getOptionalLong(CONNECTOR_DATABUS_MAX_BUFFER_BYTES).ifPresent(builder::setDatabusBufferSize);
		descriptorProperties.getOptionalBoolean(CONNECTOR_DATABUS_NEED_RESPONSE).ifPresent(builder::setNeedResponse);
		return new DatabusUpsertStreamTableSink(tableSchema, builder.build());
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> requiredContext = new HashMap<>();
		requiredContext.put(CONNECTOR_TYPE, DATABUS);
		return requiredContext;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> supportedProperties = new ArrayList<>();

		supportedProperties.add(CONNECTOR_CHANNEL);
		supportedProperties.add(CONNECTOR_BATCH_SIZE);
		supportedProperties.add(CONNECTOR_DATABUS_MAX_BUFFER_BYTES);
		supportedProperties.add(CONNECTOR_DATABUS_NEED_RESPONSE);

		// retry
		supportedProperties.add(CONNECTOR_RETRY_STRATEGY);
		supportedProperties.add(CONNECTOR_RETRY_MAX_TIMES);
		supportedProperties.add(CONNECTOR_RETRY_DELAY_MS);

		// parallelism
		supportedProperties.add(CONNECTOR_PARALLELISM);

		// schema
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_NAME);

		// format wildcard
		supportedProperties.add(FORMAT + ".*");

		return supportedProperties;
	}

	private static RetryManager.Strategy getRetryStrategy(DescriptorProperties descriptorProperties) {
		String strategyName = descriptorProperties.getOptionalString(CONNECTOR_RETRY_STRATEGY).orElse(null);
		if (strategyName == null) {
			return null;
		}
		int maxRetryTimes = descriptorProperties.getOptionalInt(CONNECTOR_RETRY_MAX_TIMES)
			.orElse(CONNECTOR_RETRY_MAX_TIMES_DEFAULT);
		int retryDelayMs = descriptorProperties.getOptionalInt(CONNECTOR_RETRY_DELAY_MS)
			.orElse(CONNECTOR_RETRY_DELAY_MS_DEFAULT);

		switch (strategyName) {
			case EXPONENTIAL_BACKOFF:
				return RetryManager.createExponentialBackoffStrategy(maxRetryTimes, retryDelayMs);
			case FIXED_DELAY:
				return RetryManager.createFixedDelayStrategy(maxRetryTimes, retryDelayMs);
			default:
				throw new IllegalArgumentException(String.format("Unsupported retry strategy: %s.", strategyName));
		}
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new SchemaValidator(true, false, false).validate(descriptorProperties);
		new DatabusValidator().validate(descriptorProperties);

		return descriptorProperties;
	}

	private static KeyedSerializationSchema<Tuple2<Boolean, Row>> getKeyedSerializationSchema(Map<String, String> properties) {
		final SerializationSchema<Tuple2<Boolean, Row>> serializationSchema =
			new IgnoreRetractMsgSerializationSchem<>(
				TableConnectorUtils.getSerializationSchema(properties, DatabusStreamTableSinkFactory.class.getClassLoader()));

		return new KeyedSerializationSchemaWrapper<>(serializationSchema);
	}
}
