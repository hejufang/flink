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

package org.apache.flink.connector.rpc.table;

import org.apache.flink.connector.rpc.table.descriptors.RPCLookupOptions;
import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * A {@link DynamicTableSource} for RPC.
 */
public class RPCDynamicTableSource implements LookupTableSource {
	private final RPCOptions options;
	private final RPCLookupOptions lookupOptions;
	private final TableSchema schema;

	public RPCDynamicTableSource(RPCOptions options, RPCLookupOptions lookupOptions, TableSchema schema) {
		this.options = options;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		// RPC only support non-nested look up keys now.
		int[][] originKeyIndices = context.getKeys();
		int[] keyIndices = new int[originKeyIndices.length];

		for (int i = 0; i < originKeyIndices.length; i++) {
			int[] innerKeyArr = originKeyIndices[i];
			Preconditions.checkArgument(innerKeyArr.length == 1,
				"RPC only support non-nested look up keys");
			keyIndices[i] = innerKeyArr[0];
		}
		return AsyncTableFunctionProvider.of(new RPCRowDataLookupFunction(
			lookupOptions,
			options,
			keyIndices,
			schema.toRowDataType(),
			schema.getFieldNames()));
	}

	@Override
	public DynamicTableSource copy() {
		return new RPCDynamicTableSource(options, lookupOptions, schema);
	}

	@Override
	public String asSummaryString() {
		return "RPC";
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(lookupOptions.isInputKeyByEnabled());
	}

	@Override
	public boolean isBooleanKeySupported() {
		return true;
	}
}
