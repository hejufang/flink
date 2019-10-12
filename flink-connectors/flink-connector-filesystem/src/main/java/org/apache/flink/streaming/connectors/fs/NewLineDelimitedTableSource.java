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
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * newline delimited table source.
 */
public class NewLineDelimitedTableSource implements StreamTableSource<Row>, LookupableTableSource<Row>,
	DefinedProctimeAttribute,
	DefinedRowtimeAttributes {

	private Path path;

	private TableSchema schema;

	private Configuration config;

	private DeserializationSchema<Row> deserializationSchema;

	private Optional<String> proctimeAttribute;

	private List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors;

	private NewLineDelimitedTableSource(Path path, TableSchema schema, Configuration config, Optional<String> proctimeAttribute,
		List<RowtimeAttributeDescriptor> rowtimeAttributeDescriptors,
		DeserializationSchema<Row> deserializationSchema) {
		this.path = path;
		this.schema = schema;
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
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return new NewLineDelimitedLookupFunction(path, schema.getFieldNames(), schema.getFieldTypes(), config, deserializationSchema, lookupKeys);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("NewLineDelimitedTableSource do not support async lookup");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
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

		private Path path;

		private TableSchema schema;

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

		public Builder setSchema(TableSchema schema) {
			this.schema = schema;
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
				schema,
				config,
				proctimeAttribute,
				rowtimeAttributeDescriptors,
				deserializationSchema
			);
		}
	}

	/**
	 * LookupFunction to support lookup in NewLineDelimitedTableSource.
	 */
	public static class NewLineDelimitedLookupFunction extends TableFunction<Row> {
		private static final long serialVersionUID = 1L;

		private final Path path;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private final Configuration config;
		private final DeserializationSchema<Row> deserializationSchema;

		private final List<Integer> sourceKeys = new ArrayList<>();
		private final List<Integer> targetKeys = new ArrayList<>();
		private final Map<Object, List<Row>> dataMap = new HashMap<>();

		NewLineDelimitedLookupFunction(Path path, String[] fieldNames, TypeInformation[] fieldTypes, Configuration config,
			DeserializationSchema<Row> deserializationSchema,
			String[] lookupKeys) {

			this.path = path;
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
			this.config = config;
			this.deserializationSchema = deserializationSchema;

			List<String> fields = Arrays.asList(fieldNames);
			for (int i = 0; i < lookupKeys.length; i++) {
				sourceKeys.add(i);
				int targetIdx = fields.indexOf(lookupKeys[i]);
				assert targetIdx != -1;
				targetKeys.add(targetIdx);
			}
		}

		@Override
		public TypeInformation<Row> getResultType() {
			return new RowTypeInfo(fieldTypes, fieldNames);
		}

		@Override
		public void open(FunctionContext context) throws Exception {
			super.open(context);
			TypeInformation<Row> rowType = getResultType();

			NewLineDelimitedInputFormat inputFormat = new NewLineDelimitedInputFormat(path, config, deserializationSchema);
			FileInputSplit[] inputSplits = inputFormat.createInputSplits(1);
			for (FileInputSplit split : inputSplits) {
				inputFormat.open(split);
				Row row = new Row(rowType.getArity());
				while (true) {
					Row r = (Row) inputFormat.nextRecord(row);
					if (r == null) {
						break;
					} else {
						Object key = getTargetKey(r);
						List<Row> rows = dataMap.computeIfAbsent(key, k -> new ArrayList<>());
						rows.add(Row.copy(r));
					}
				}
				inputFormat.close();
			}
		}

		public void eval(Object... values) {
			Object srcKey = getSourceKey(Row.of(values));
			if (dataMap.containsKey(srcKey)) {
				for (Row row1 : dataMap.get(srcKey)) {
					collect(row1);
				}
			}
		}

		private Object getSourceKey(Row source) {
			return getKey(source, sourceKeys);
		}

		private Object getTargetKey(Row target) {
			return getKey(target, targetKeys);
		}

		private Object getKey(Row input, List<Integer> keys) {
			if (keys.size() == 1) {
				int keyIdx = keys.get(0);
				if (input.getField(keyIdx) != null) {
					return input.getField(keyIdx);
				}
				return null;
			} else {
				Row key = new Row(keys.size());
				for (int i = 0; i < keys.size(); i++) {
					int keyIdx = keys.get(i);
					key.setField(i, input.getField(keyIdx));
				}
				return key;
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
		}
	}
}
