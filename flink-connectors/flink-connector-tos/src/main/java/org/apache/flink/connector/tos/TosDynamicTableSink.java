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

package org.apache.flink.connector.tos;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/**
 * Tos table sink implementation.
 */
@Internal
public class TosDynamicTableSink implements DynamicTableSink {

	private final TableSchema tableSchema;
	private final TosOptions tosOptions;
	private final EncodingFormat<SerializationSchema<RowData>> format;

	public TosDynamicTableSink(
			TableSchema tableSchema,
			TosOptions tosOptions,
			EncodingFormat<SerializationSchema<RowData>> format) {
		this.tableSchema = tableSchema;
		this.tosOptions = tosOptions;
		this.format = format;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		SerializationSchema<RowData> serializationSchema = this.format.createRuntimeEncoder(
			context,
			tableSchema.toRowDataType());
		TosSinkFunction sinkFunction = new TosSinkFunction(
			tosOptions,
			serializationSchema);
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
		return new TosDynamicTableSink(tableSchema, tosOptions, format);
	}

	@Override
	public String asSummaryString() {
		return "tos";
	}
}
