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

import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.metric.SinkMetricsOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DynamicTableSink} for ByteSQL.
 */
public class ByteSQLDynamicTableSink implements DynamicTableSink {
	private final ByteSQLOptions options;
	private final ByteSQLInsertOptions insertOptions;
	private final SinkMetricsOptions insertMetricsOptions;
	private final TableSchema schema;
	private boolean isAppendOnly;

	public ByteSQLDynamicTableSink(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			SinkMetricsOptions insertMetricsOptions,
			TableSchema schema) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.schema = schema;
		this.insertMetricsOptions = insertMetricsOptions;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		setIsAppendOnly(requestedMode);
		validatePrimaryKey();
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	private void setIsAppendOnly(ChangelogMode requestedMode) {
		isAppendOnly = ChangelogMode.insertOnly().equals(requestedMode);
	}

	private void validatePrimaryKey() {
		checkState(isAppendOnly || insertOptions.getKeyFields() != null,
			"please declare primary key for sink table when query contains update/delete record.");
		checkState(!insertOptions.isIgnoreNull() || insertOptions.getKeyFields() != null,
			"please declare primary key if ignore null mode is turned on.");
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		ByteSQLOutputFormat outputFormat = new ByteSQLOutputFormat(
			options,
			insertOptions,
			insertMetricsOptions,
			(RowType) schema.toRowDataType().getLogicalType(),
			isAppendOnly
		);
		return SinkFunctionProvider.of(
			new ByteSQLSinkFunction(
				outputFormat,
				insertOptions.getParallelism()
			)
		);
	}

	@Override
	public DynamicTableSink copy() {
		ByteSQLDynamicTableSink sink = new ByteSQLDynamicTableSink(
			options,
			insertOptions,
			insertMetricsOptions,
			schema
		);
		sink.isAppendOnly = this.isAppendOnly;
		return sink;
	}

	@Override
	public String asSummaryString() {
		return "ByteSQL";
	}
}
