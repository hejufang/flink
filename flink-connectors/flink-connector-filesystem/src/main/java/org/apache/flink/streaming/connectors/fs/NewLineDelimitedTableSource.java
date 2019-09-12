/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * newline delimited table source.
 */
public class NewLineDelimitedTableSource implements StreamTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes {

	private Path path;

	private Configuration config;

	private DeserializationSchema<Row> deserializationSchema;

	private Optional<String> proctimeAttribute;

	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	private NewLineDelimitedTableSource(Path path, Configuration config, Optional<String> proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		DeserializationSchema<Row> deserializationSchema) {
		this.path = path;
		this.config = config;
		this.proctimeAttribute = proctimeAttribute;
		this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.createInput(new NewLineDelimitedInputFormat(path, config, deserializationSchema),
			getReturnType()).name(explainSource()
		);
	}

	@Override
	public TableSchema getTableSchema() {
		RowTypeInfo rowTypeInfo = (RowTypeInfo) deserializationSchema.getProducedType();
		String[] fieldNames = rowTypeInfo.getFieldNames();
		TypeInformation<?>[] filedTypes = rowTypeInfo.getFieldTypes();

		TableSchema.Builder builder = new TableSchema.Builder();
		for (int i = 0; i < rowTypeInfo.getArity(); i++) {
			builder.field(fieldNames[i], filedTypes[i]);
		}
		return builder.build();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		RowTypeInfo rowTypeInfo = (RowTypeInfo) deserializationSchema.getProducedType();
		return rowTypeInfo;
	}

	@Override
	public String getProctimeAttribute() {
		return proctimeAttribute.orElse(null);
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return rowtimeAttributeDescriptors;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for NewLineDelimitedTableSource.
	 */
	public static class Builder {

		private LinkedHashMap<String, TypeInformation<?>> schema = new LinkedHashMap<>();

		private Path path;

		private Configuration config;

		private DeserializationSchema<Row> deserializationSchema;

		private Optional<String> proctimeAttribute;

		private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

		private Builder() {

		}

		/**
		 * Sets the path to the file. Required.
		 *
		 * @param path the path to the file
		 */
		public Builder setPath(String path) {
			this.path = new Path(path);
			return this;
		}

		public Builder setConfig(Configuration config) {
			this.config = config;
			return this;
		}

		public Builder setDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
			this.deserializationSchema = deserializationSchema;
			return this;
		}

		public Builder setField(String fieldName, TypeInformation<?> fieldType) {
			if (schema.containsKey(fieldName)) {
				throw new IllegalArgumentException("Duplicate field name " + fieldName);
			}
			schema.put(fieldName, fieldType);
			return this;
		}

		public Builder setProctimeAttribute(Optional<String> proctimeAttribute) {
			this.proctimeAttribute = proctimeAttribute;
			return this;
		}

		public Builder setRowtimeAttributeDescriptors(List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors) {
			this.rowtimeAttributeDescriptors = rowtimeAttributeDescriptors;
			return this;
		}

		public NewLineDelimitedTableSource build() {
			if (path == null) {
				throw new IllegalArgumentException("Path must be defined.");
			}

			return new NewLineDelimitedTableSource(
				path,
				config,
				proctimeAttribute,
				rowtimeAttributeDescriptors,
				deserializationSchema
			);
		}
	}
}
