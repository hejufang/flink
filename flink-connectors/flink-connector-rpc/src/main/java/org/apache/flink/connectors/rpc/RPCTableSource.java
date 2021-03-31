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

package org.apache.flink.connectors.rpc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Optional;

/**
 * Attention: only used in lookup table source now.
 */
public class RPCTableSource implements
		StreamTableSource<Row>,
		LookupableTableSource<Row> {

	private final TableSchema tableSchema;
	private final RPCOptions rpcOptions;
	private final RPCLookupOptions rpcLookupOptions;

	public RPCTableSource(
			TableSchema tableSchema,
			RPCOptions rpcOptions,
			RPCLookupOptions rpcLookupOptions) {
		this.tableSchema = tableSchema;
		this.rpcOptions = rpcOptions;
		this.rpcLookupOptions = rpcLookupOptions;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupFieldNames) {
		if (rpcLookupOptions.useBatchLookup()) {
			return new RPCBatchedLookupFunction(
				tableSchema.toRowType(),
				tableSchema.getFieldNames(),
				lookupFieldNames,
				rpcOptions,
				rpcLookupOptions,
				tableSchema.toRowDataType());
		} else {
			return new RPCLookupFunction(
				tableSchema.toRowType(),
				tableSchema.getFieldNames(),
				lookupFieldNames,
				rpcOptions,
				rpcLookupOptions,
				tableSchema.toRowDataType());
		}
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("RPC doesn't support async lookup currently.");
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
		throw new UnsupportedOperationException("RPC doesn't support as source currently.");
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(rpcLookupOptions.isInputKeyByEnabled());
	}
}
