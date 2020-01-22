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

package org.apache.flink.connectors.metrics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.MetricsValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.MetricsValidator.CONNECTOR_METRICS_PREFIX;
import static org.apache.flink.table.descriptors.MetricsValidator.CONNECTOR_WRITE_FLUSH_INTERVAL;
import static org.apache.flink.table.descriptors.MetricsValidator.CONNECTOR_WRITE_FLUSH_MAX_ROWS;
import static org.apache.flink.table.descriptors.MetricsValidator.METRICS;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Metrics table factory.
 */
public class MetricsTableFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> requiredContext = new HashMap<>();
		requiredContext.put(CONNECTOR_TYPE, METRICS);
		return requiredContext;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> supportedProperties = new ArrayList<>();
		supportedProperties.add(CONNECTOR_PARALLELISM);
		supportedProperties.add(CONNECTOR_METRICS_PREFIX);
		supportedProperties.add(CONNECTOR_WRITE_FLUSH_MAX_ROWS);
		supportedProperties.add(CONNECTOR_WRITE_FLUSH_INTERVAL);
		// schema
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		supportedProperties.add(SCHEMA + ".#." + SCHEMA_NAME);
		return supportedProperties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		MetricsOptions.MetricsOptionsBuilder optionBuilder = MetricsOptions.builder();
		String metricsPrefix = descriptorProperties.getString(CONNECTOR_METRICS_PREFIX);
		optionBuilder.setMetricsPrefix(metricsPrefix);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM).ifPresent(optionBuilder::setParallelism);
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS).ifPresent(optionBuilder::setBatchSize);
		descriptorProperties.getOptionalInt(CONNECTOR_WRITE_FLUSH_INTERVAL).ifPresent(optionBuilder::setFlushIntervalMs);
		return new MetricsUpsertTableSink(optionBuilder.build());
	}

	public DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new MetricsValidator().validate(descriptorProperties);
		return descriptorProperties;
	}
}
