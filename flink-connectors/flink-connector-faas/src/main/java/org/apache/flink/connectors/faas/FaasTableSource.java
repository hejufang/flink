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

package org.apache.flink.connectors.faas;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * faas table source.
 */
public class FaasTableSource implements StreamTableSource<Row>, LookupableTableSource<Row> {

	private final TableSchema tableSchema;
	private final FaasLookupOptions faasLookupOptions;

	public FaasTableSource(
			TableSchema tableSchema,
			FaasLookupOptions faasLookupOptions) {
		this.tableSchema = tableSchema;
		this.faasLookupOptions = faasLookupOptions;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		throw new UnsupportedOperationException("FaaS doesn't support sync lookup currently.");
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return tableSchema.toRowType();
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		throw new UnsupportedOperationException("FaaS doesn't support as source currently.");
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		return new FaasAsyncLookupFunction(
			faasLookupOptions,
			tableSchema.getFieldNames(),
			tableSchema.getFieldTypes(),
			lookupKeys);
	}

	@Override
	public boolean isAsyncEnabled() {
		return true;
	}
}
