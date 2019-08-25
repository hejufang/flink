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

package org.apache.flink.connectors.redis;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Attention: only used in lookup table source now.
 */
public class RedisTableSource implements
		StreamTableSource<Row>,
		ProjectableTableSource<Row>,
		LookupableTableSource<Row> {
	private final RedisOptions options;
	private final RedisLookupOptions lookupOptions;
	private final TableSchema schema;

	// index of fields selected, null means that all fields are selected
	private final int[] selectFields;
	private final RowTypeInfo returnType;

	private RedisTableSource(RedisOptions options, RedisLookupOptions lookupOptions, TableSchema schema) {
		this(options, lookupOptions, schema, null);
	}

	private RedisTableSource(
			RedisOptions options, RedisLookupOptions lookupOptions,
			TableSchema schema, int[] selectFields) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;

		this.selectFields = selectFields;

		final TypeInformation<?>[] schemaTypeInfos = schema.getFieldTypes();
		final String[] schemaFieldNames = schema.getFieldNames();
		if (selectFields != null) {
			TypeInformation<?>[] typeInfos = new TypeInformation[selectFields.length];
			String[] typeNames = new String[selectFields.length];
			for (int i = 0; i < selectFields.length; i++) {
				typeInfos[i] = schemaTypeInfos[selectFields[i]];
				typeNames[i] = schemaFieldNames[selectFields[i]];
			}
			this.returnType = new RowTypeInfo(typeInfos, typeNames);
		} else {
			this.returnType = new RowTypeInfo(schemaTypeInfos, schemaFieldNames);
		}
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return RedisLookupFunction.builder()
				.setOptions(options)
				.setLookupOptions(lookupOptions)
				.setFieldTypes(returnType.getFieldTypes())
				.setFieldNames(returnType.getFieldNames())
				.setKeyNames(lookupKeys)
				.build();
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return returnType;
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		return new RedisTableSource(options, lookupOptions, schema, fields);
	}

	/**
	 * Builder for a {@link RedisTableSource}.
	 */
	public static class Builder {

		private RedisOptions options;
		private RedisLookupOptions lookupOptions;
		private TableSchema schema;

		/**
		 * optional, lookup related options.
		 * {@link RedisLookupOptions} only be used for {@link LookupableTableSource}.
		 */
		public Builder setLookupOptions(RedisLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(RedisOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * required, table schema of this table source.
		 */
		public Builder setSchema(TableSchema schema) {
			this.schema = schema;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCTableSource
		 */
		public RedisTableSource build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(schema, "No schema supplied.");

			if (lookupOptions == null) {
				lookupOptions = RedisLookupOptions.builder().build();
			}
			return new RedisTableSource(options, lookupOptions, schema);
		}
	}
}
