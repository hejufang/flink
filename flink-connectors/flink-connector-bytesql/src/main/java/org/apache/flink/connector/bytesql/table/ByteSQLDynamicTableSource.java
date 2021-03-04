/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLLookupOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link DynamicTableSource} for ByteSQL.
 */
public class ByteSQLDynamicTableSource implements LookupTableSource {
	private final ByteSQLOptions options;
	private final ByteSQLLookupOptions lookupOptions;
	private final TableSchema schema;

	public ByteSQLDynamicTableSource(ByteSQLOptions options, ByteSQLLookupOptions lookupOptions, TableSchema schema) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		// ByteSQL only support non-nested look up keys
		String[] fieldNames = schema.getFieldNames();
		List<String> keyNames = new ArrayList<>();
		for (int i = 0; i < context.getKeys().length; i++) {
			int[] innerKeyArr = context.getKeys()[i];
			Preconditions.checkArgument(innerKeyArr.length == 1,
				"ByteSQL only support non-nested look up keys");
			keyNames.add(fieldNames[innerKeyArr[0]]);
		}
		final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
		ByteSQLLookupExecutor executor = new ByteSQLLookupExecutor(
			options,
			lookupOptions,
			keyNames,
			rowType);
		if (lookupOptions.isAsync()) {
			return TableFunctionProvider.of(new ByteSQLLookupFunction(executor));
		} else {
			return AsyncTableFunctionProvider.of(
				new ByteSQLAsyncLookupFunction(executor, lookupOptions.getAsyncConcurrency()));
		}
	}

	@Override
	public long getLaterJoinMs() {
		return lookupOptions.getLaterJoinLatency();
	}

	@Override
	public int getLaterJoinRetryTimes() {
		return lookupOptions.getLaterJoinRetryTimes();
	}

	@Override
	public DynamicTableSource copy() {
		return new ByteSQLDynamicTableSource(
			options,
			lookupOptions,
			schema
		);
	}

	@Override
	public String asSummaryString() {
		return "ByteSQL";
	}
}
