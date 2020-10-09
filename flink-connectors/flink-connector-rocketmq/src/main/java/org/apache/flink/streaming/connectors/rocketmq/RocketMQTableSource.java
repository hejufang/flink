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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rocketmq.serialization.RocketMQDeserializationSchemaWrapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.rocketmq.table.descriptors.RocketMQValidator.CONNECTOR_FORCE_AUTO_COMMIT_ENABLED;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_KEYBY_FIELDS;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PARALLELISM;

/**
 * RocketMQ Table Source.
 */
public class RocketMQTableSource implements
	StreamTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes,
	DefinedFieldMapping {

	/**
	 * The schema of the table.
	 */
	private final TableSchema schema;

	/**
	 * Field name of the processing time attribute, null if no processing time field is defined.
	 */
	private final String proctimeAttribute;

	/**
	 * Descriptor for a rowtime attribute.
	 */
	private final List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	/**
	 * Mapping for the fields of the table schema to fields of the physical returned type.
	 */
	private final Map<String, String> fieldMapping;

	/**
	 * Properties for the RocketMQ consumer.
	 */
	private final Properties properties;

	/**
	 * Other configurations for RocketMQ table source, such as keyby fields, parallelism and so on.
	 */
	private final Map<String, String> configurations;

	/**
	 * Deserialization schema for decoding records from RocketMQ.
	 */
	private final DeserializationSchema<Row> deserializationSchema;

	private RocketMQTableSource(
		TableSchema schema,
		String proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		Map<String, String> fieldMapping,
		Properties properties,
		Map<String, String> configurations,
		DeserializationSchema<Row> deserializationSchema) {
		this.schema = schema;
		this.proctimeAttribute = proctimeAttribute;
		this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
		this.fieldMapping = fieldMapping;
		this.properties = properties;
		this.configurations = configurations;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		RocketMQSource<Row> rowRocketMQSource = new RocketMQSource<>(
			new RocketMQDeserializationSchemaWrapper<>(deserializationSchema), properties);

		// Set force auto commit.
		if (configurations.containsKey(CONNECTOR_FORCE_AUTO_COMMIT_ENABLED)) {
			boolean forceAutoCommitEnabled =
				Boolean.parseBoolean(configurations.get(CONNECTOR_FORCE_AUTO_COMMIT_ENABLED));
			rowRocketMQSource.setForceAutoCommitEnabled(forceAutoCommitEnabled);
		}
		// Set source parallelism
		int parallelism = Integer.valueOf(configurations.getOrDefault(CONNECTOR_PARALLELISM, "-1"));

		DataStreamSource<Row> dataStreamSource = execEnv.addSource(rowRocketMQSource);
		if (parallelism > 0) {
			dataStreamSource = dataStreamSource.setParallelism(parallelism);
		}
		DataStream<Row> dataStream = dataStreamSource.name(explainSource());
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
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return deserializationSchema.getProducedType();
	}

	@Nullable
	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping;
	}

	@Nullable
	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute;
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	public static RocketMQTableSourceBuilder builder() {
		return new RocketMQTableSourceBuilder();
	}

	/**
	 * Builder for {@link RocketMQTableSource}.
	 * */
	public static class RocketMQTableSourceBuilder {
		private TableSchema schema;
		private String proctimeAttribute;
		private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;
		private Map<String, String> fieldMapping;
		private Properties properties;
		private Map<String, String> configurations;
		private DeserializationSchema<Row> deserializationSchema;

		private RocketMQTableSourceBuilder() {
		}

		public RocketMQTableSourceBuilder setSchema(TableSchema schema) {
			this.schema = schema;
			return this;
		}

		public RocketMQTableSourceBuilder setProctimeAttribute(String proctimeAttribute) {
			this.proctimeAttribute = proctimeAttribute;
			return this;
		}

		public RocketMQTableSourceBuilder setRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
			this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
			return this;
		}

		public RocketMQTableSourceBuilder setFieldMapping(Map<String, String> fieldMapping) {
			this.fieldMapping = fieldMapping;
			return this;
		}

		public RocketMQTableSourceBuilder setProperties(Properties properties) {
			this.properties = properties;
			return this;
		}

		public RocketMQTableSourceBuilder setConfigurations(Map<String, String> configurations) {
			this.configurations = configurations;
			return this;
		}

		public RocketMQTableSourceBuilder setDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return this;
		}

		public RocketMQTableSource build() {
			return new RocketMQTableSource(schema, proctimeAttribute, rowtimeAttributeDescriptors,
				fieldMapping, properties, configurations, deserializationSchema);
		}
	}
}
