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

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.options.AbaseSinkOptions;
import org.apache.flink.connector.abase.utils.AbaseSinkMode;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DynamicTableSink} for Abase.
 */
public class AbaseTableSink implements DynamicTableSink{
	private final AbaseNormalOptions normalOptions;
	private final AbaseSinkOptions sinkOptions;
	private final TableSchema schema;
	@Nullable
	protected final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

	public AbaseTableSink(
			AbaseNormalOptions normalOptions,
			AbaseSinkOptions sinkOptions,
			TableSchema schema,
			@Nullable EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		this.normalOptions = normalOptions;
		this.sinkOptions = sinkOptions;
		this.schema = schema;
		this.encodingFormat = encodingFormat;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
		validateChangelogMode(changelogMode);
		return ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
	}

	private void validateChangelogMode(ChangelogMode requestedMode) {
		if (!requestedMode.equals(ChangelogMode.insertOnly())) {
			checkState(!normalOptions.getAbaseValueType().isAppendOnly(),
				String.format("Upstream should be append only as %s cannot be updated.", normalOptions.getAbaseValueType()));
			checkState(sinkOptions.getMode() != AbaseSinkMode.INCR,
				"Upstream should be append only when incr mode is on, " +
					"as incremental value cannot be updated.");
		}
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		TableSchema realSchema = schema;
		if (encodingFormat != null && sinkOptions.isSkipFormatKey()) {
			TableSchema.Builder builder = new TableSchema.Builder();
			List<TableColumn> columns = schema.getTableColumns();
			if (normalOptions.getKeyIndex() > 0) {
				columns.remove(normalOptions.getKeyIndex());
			} else {
				// default primary key row.
				columns.remove(0);
			}
			columns.forEach(column -> builder.field(column.getName(), column.getType()));
			realSchema = builder.build();
		}
		DataType originalDataType = schema.toRowDataType();
		final RowType rowType = (RowType) originalDataType.getLogicalType();
		DataStructureConverter converter = context.createDataStructureConverter(originalDataType);
		AbaseOutputFormat outputFormat = new AbaseOutputFormat(
			normalOptions,
			sinkOptions,
			rowType,
			encodingFormat == null ? null : encodingFormat.createRuntimeEncoder(context, realSchema.toRowDataType()),
			converter
		);
		return SinkFunctionProvider.of(new AbaseSinkFunction(
			outputFormat,
			sinkOptions.getParallelism(),
			normalOptions.getRateLimiter()
		));
	}

	@Override
	public DynamicTableSink copy() {
		return new AbaseTableSink(
			normalOptions,
			sinkOptions,
			schema,
			encodingFormat
		);
	}

	@Override
	public String asSummaryString() {
		return normalOptions.getStorage();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AbaseTableSink)) {
			return false;
		}
		AbaseTableSink sink = (AbaseTableSink) o;
		return Objects.equals(normalOptions, sink.normalOptions) &&
			Objects.equals(sinkOptions, sink.sinkOptions) &&
			Objects.equals(schema, sink.schema) &&
			Objects.equals(encodingFormat, sink.encodingFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(normalOptions, sinkOptions, schema, encodingFormat);
	}
}
