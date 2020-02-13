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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.connectors.rocketmq.selector.TopicSelector;
import org.apache.flink.streaming.connectors.rocketmq.serialization.KeyValueSerializationSchemaWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;

/**
 * RocketMQ upsert table sink.
 */
public class RocketMQUpsertTableSink implements UpsertStreamTableSink<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQUpsertTableSink.class);
	/**
	 * Properties for the rocketmq.
	 */
	private final Properties properties;
	/**
	 * Serialization schema for encoding records to Kafka.
	 */
	private final SerializationSchema<Row> serializationSchema;
	/**
	 * The schema of the table.
	 */
	private final TableSchema schema;
	/**
	 * Other configurations for table source, such as keyby fields, parallelism and so on.
	 */
	private final Map<String, String> configurations;
	private final TopicSelector<Row> topicSelector;
	private final RocketMQOptions options;

	private RocketMQUpsertTableSink(
		TableSchema schema,
		Properties properties,
		Map<String, String> configurations,
		TopicSelector<Row> topicSelector,
		SerializationSchema<Row> serializationSchema,
		RocketMQOptions options) {
		this.schema = schema;
		this.properties = properties;
		this.configurations = configurations;
		this.topicSelector = topicSelector;
		this.serializationSchema = serializationSchema;
		this.options = options;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// rocketmq is always in append only mode, ignore it.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// ignore query keys.
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		if (!Arrays.equals(schema.getFieldNames(), fieldNames)
			|| !Arrays.equals(schema.getFieldTypes(), fieldTypes)) {
			throw new ValidationException("Reconfiguration with different fields is not allowed. " +
				"Expected: " + Arrays.toString(schema.getFieldNames()) + " / " +
				Arrays.toString(schema.getFieldTypes()) + ". " +
				"But was: " + Arrays.toString(fieldNames) + " / " + Arrays.toString(fieldTypes));
		}
		return this;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		RocketMQSink<Row> rowRocketMQSink =
			new RocketMQSink<>(new KeyValueSerializationSchemaWrapper<>(serializationSchema),
				topicSelector, properties, options);

		// filter retract rows
		DataStream<Row> rowDataStream = dataStream
			.map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> {
				if (value != null && value.f0) {
					return value.f1;
				}
				return null;
			}).setParallelism(dataStream.getParallelism())
			.filter(Objects::nonNull)
			.setParallelism(dataStream.getParallelism());

		DataStreamSink<Row> dataStreamSink = rowDataStream.addSink(rowRocketMQSink);
		int parallelism = Integer.valueOf(configurations.getOrDefault(CONNECTOR_PARALLELISM, "-1"));
		if (parallelism > 0) {
			LOG.info("Set parallelism to {} for rocketmq table sink.", parallelism);
			dataStreamSink.setParallelism(parallelism);
		}
		return dataStreamSink;
	}

	public static RocketMQUpsertTableSinkBuilder builder() {
		return new RocketMQUpsertTableSinkBuilder();
	}

	/**
	 * Builder for {@link RocketMQUpsertTableSink}.
	 * */
	public static class RocketMQUpsertTableSinkBuilder {
		private Properties properties;
		private SerializationSchema<Row> serializationSchema;
		private TableSchema schema;
		private Map<String, String> configurations;
		private TopicSelector<Row> topicSelector;
		private RocketMQOptions options;

		private RocketMQUpsertTableSinkBuilder() {
		}

		public RocketMQUpsertTableSinkBuilder setProperties(Properties properties) {
			this.properties = properties;
			return this;
		}

		public RocketMQUpsertTableSinkBuilder setSerializationSchema(SerializationSchema<Row> serializationSchema) {
			this.serializationSchema = serializationSchema;
			return this;
		}

		public RocketMQUpsertTableSinkBuilder setSchema(TableSchema schema) {
			this.schema = schema;
			return this;
		}

		public RocketMQUpsertTableSinkBuilder setConfigurations(Map<String, String> configurations) {
			this.configurations = configurations;
			return this;
		}

		public RocketMQUpsertTableSinkBuilder setTopicSelector(TopicSelector<Row> topicSelector) {
			this.topicSelector = topicSelector;
			return this;
		}

		public RocketMQUpsertTableSinkBuilder setOptions(RocketMQOptions options) {
			this.options = options;
			return this;
		}

		public RocketMQUpsertTableSink build() {
			return new RocketMQUpsertTableSink(schema, properties, configurations, topicSelector,
				serializationSchema, options);
		}
	}
}
