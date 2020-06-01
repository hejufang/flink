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

package org.apache.flink.connectors.bytable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * Attention: only used in lookup table source now.
 */
public class BytableTableSource implements
		StreamTableSource<Row>,
		LookupableTableSource<Row> {

	private final BytableTableSchema bytableTableSchema;
	private final TableSchema tableSchema;
	private final BytableOption bytableOption;
	private final BytableLookupOptions bytableLookupOptions;

	public BytableTableSource(
			BytableTableSchema bytableTableSchema,
			TableSchema tableSchema,
			BytableOption bytableOption,
			BytableLookupOptions bytableLookupOptions) {
		this.bytableTableSchema = bytableTableSchema;
		this.tableSchema = tableSchema;
		this.bytableOption = bytableOption;
		this.bytableLookupOptions = bytableLookupOptions;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		Preconditions.checkState(
			bytableTableSchema.getRowKeyName().isPresent(),
			"Bytable schema must have a row key when used in lookup mode.");
		Preconditions.checkState(
			bytableTableSchema.getRowKeyName().get().equals(lookupKeys[0]),
			String.format("The lookup key : %s is not rowKey : %s of Bytable.",
				bytableTableSchema.getRowKeyName().get(), lookupKeys[0]));

		return new BytableLookupFunction(bytableTableSchema, bytableOption, bytableLookupOptions);
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return bytableTableSchema.convertsToTableSchema().toRowType();
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("Bytable doesn't support as source currently.");
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("Bytable doesn't support async lookup currently.");
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}
}
