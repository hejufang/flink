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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

/**
 * A version-agnostic Kafka {@link UpsertStreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, SerializationSchema, Optional)}}.
 */
@Internal
public abstract class KafkaUpsertTableSinkBase implements UpsertStreamTableSink<Row> {

	/** The schema of the table. */
	private final TableSchema schema;

	/** The Kafka topic to write to. */
	protected final String topic;

	/** Properties for the Kafka producer. */
	protected final Properties properties;

	/** Serialization schema for encoding records to Kafka. */
	protected final SerializationSchema<Row> serializationSchema;

	/** Partitioner to select Kafka partition for each item. */
	protected final Optional<FlinkKafkaPartitioner<Row>> partitioner;

	protected KafkaUpsertTableSinkBase(
			TableSchema schema,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<Row>> partitioner,
			SerializationSchema<Row> serializationSchema) {
		this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.partitioner = Preconditions.checkNotNull(partitioner, "Partitioner must not be null.");
		this.serializationSchema = Preconditions.checkNotNull(serializationSchema, "Serialization schema must not be null.");
	}

	@Override
	public void setKeyFields(String[] keys) {
		// kafka is always in append only mode, ignore it.
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// ignore query keys.
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return schema.toRowType();
	}

	/**
	 * Returns the version-specific Kafka producer.
	 *
	 * @param topic               Kafka topic to produce to.
	 * @param properties          Properties for the Kafka producer.
	 * @param serializationSchema Serialization schema to use to create Kafka records.
	 * @param partitioner         Partitioner to select Kafka partition.
	 * @return The version-specific Kafka producer
	 */
	protected abstract SinkFunction<Row> createKafkaProducer(
		String topic,
		Properties properties,
		SerializationSchema<Row> serializationSchema,
		Optional<FlinkKafkaPartitioner<Row>> partitioner);

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		final SinkFunction<Row> kafkaProducer = createKafkaProducer(
			topic,
			properties,
			serializationSchema,
			partitioner);

		DataStream<Row> rowDataStream = dataStream.map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
			@Override
			public Row map(Tuple2<Boolean, Row> value) throws Exception {
				if (value != null && value.f0) {
					return value.f1;
				}
				return null;
			}
		});

		return rowDataStream
			.addSink(kafkaProducer)
			.setParallelism(dataStream.getParallelism())
			.name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public KafkaUpsertTableSinkBase configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
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
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final KafkaUpsertTableSinkBase that = (KafkaUpsertTableSinkBase) o;
		return Objects.equals(schema, that.schema) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(properties, that.properties) &&
			Objects.equals(serializationSchema, that.serializationSchema) &&
			Objects.equals(partitioner, that.partitioner);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			schema,
			topic,
			properties,
			serializationSchema,
			partitioner);
	}
}
