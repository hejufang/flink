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

package org.apache.flink.connector.doris;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

/**
 * Doris dynamic table sink.
 */
public class DorisDynamicTableSink implements DynamicTableSink {

	private final TableSchema tableSchema;
	private final DorisOptions dorisOptions;

	public DorisDynamicTableSink(TableSchema tableSchema, DorisOptions dorisOptions) {
		this.tableSchema = tableSchema;
		this.dorisOptions = dorisOptions;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		DorisSinkFunction sinkFunction = new DorisSinkFunction(tableSchema.getFieldDataTypes(), dorisOptions);
		return SinkFunctionProvider.of(sinkFunction);
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	@Override
	public DynamicTableSink copy() {
		return new DorisDynamicTableSink(tableSchema, dorisOptions);
	}

	@Override
	public String asSummaryString() {
		return "doris";
	}
}
