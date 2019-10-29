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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE_VALUE_APPEND;


/**
 * Factory for creating configured instances of {@link KafkaTableSourceBase}.
 */
public abstract class KafkaTableSourceSinkFactoryBase
	extends AbstractKafkaTableSourceSinkFactoryBase<Row> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = super.requiredContext();
		context.put(UPDATE_MODE, UPDATE_MODE_VALUE_APPEND);
		return context;
	}

	@Override
	public StreamTableSink<Row> createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		Optional<FlinkKafkaPartitioner<Row>> partitioner,
		SerializationSchema<Row> serializationSchema,
		String updateMode) {
		return createKafkaTableSink(
			schema,
			topic,
			properties,
			partitioner,
			serializationSchema);
	}

	/**
	 * Constructs the version-specific Kafka table sink.
	 *
	 * @param schema      Schema of the produced table.
	 * @param topic       Kafka topic to consume.
	 * @param properties  Properties for the Kafka consumer.
	 * @param partitioner Partitioner to select Kafka partition for each item.
	 */
	protected abstract StreamTableSink<Row> createKafkaTableSink(
		TableSchema schema,
		String topic,
		Properties properties,
		Optional<FlinkKafkaPartitioner<Row>> partitioner,
		SerializationSchema<Row> serializationSchema);
}
