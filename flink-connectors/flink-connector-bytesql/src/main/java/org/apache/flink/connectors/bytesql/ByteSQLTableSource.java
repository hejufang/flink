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

package org.apache.flink.connectors.bytesql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * {@link TableSource} for ByteSQL.
 */
public class ByteSQLTableSource implements
		StreamTableSource<Row>,
		LookupableTableSource<Row> {
	private final ByteSQLOptions options;
	private final ByteSQLLookupOptions lookupOptions;
	private final TableSchema schema;
	private final RowTypeInfo returnType;

	public ByteSQLTableSource(
			ByteSQLOptions options,
			ByteSQLLookupOptions lookupOptions,
			TableSchema schema) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
		final TypeInformation<?>[] schemaTypeInfos = schema.getFieldTypes();
		final String[] schemaFieldNames = schema.getFieldNames();
		this.returnType = new RowTypeInfo(schemaTypeInfos, schemaFieldNames);
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
		return new ByteSQLLookupFunction(
			options,
			lookupOptions,
			returnType.getFieldNames(),
			returnType.getFieldTypes(),
			lookupKeys,
			rowType
		);
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
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return returnType;
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(lookupOptions.isInputKeyByEnabled());
	}
}
