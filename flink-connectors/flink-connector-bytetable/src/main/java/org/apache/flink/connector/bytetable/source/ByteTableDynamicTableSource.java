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

package org.apache.flink.connector.bytetable.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.hadoop.conf.Configuration;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * ByteTable table source implementation.
 */
@Internal
public class ByteTableDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

	private final Configuration conf;
	private final String tableName;
	private ByteTableSchema byteTableSchema;
	private final String nullStringLiteral;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final int parallelism;

	public ByteTableDynamicTableSource(
			Configuration conf,
			String tableName,
			ByteTableSchema byteTableSchema,
			String nullStringLiteral,
			FlinkConnectorRateLimiter rateLimiter,
			int parallelism) {
		this.conf = conf;
		this.tableName = tableName;
		this.byteTableSchema = byteTableSchema;
		this.nullStringLiteral = nullStringLiteral;
		this.rateLimiter = rateLimiter;
		this.parallelism = parallelism;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return InputFormatProvider.of(new ByteTableRowDataInputFormat(conf, tableName, byteTableSchema, nullStringLiteral, parallelism));
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1,
			"Currently, ByteTable table can only be lookup by single rowkey.");
		checkArgument(
			byteTableSchema.getRowKeyName().isPresent(),
			"ByteTable schema must have a row key when used in lookup mode.");
		checkArgument(
			byteTableSchema
				.convertsToTableSchema()
				.getTableColumn(context.getKeys()[0][0])
				.filter(f -> f.getName().equals(byteTableSchema.getRowKeyName().get()))
				.isPresent(),
			"Currently, ByteTable table only supports lookup by rowkey field.");

		return TableFunctionProvider.of(new ByteTableRowDataLookupFunction(
			conf, tableName, byteTableSchema, nullStringLiteral, rateLimiter));
	}

	@Override
	public boolean supportsNestedProjection() {
		// planner doesn't support nested projection push down yet.
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		TableSchema projectSchema = TableSchemaUtils.projectSchema(
			byteTableSchema.convertsToTableSchema(),
			projectedFields);
		this.byteTableSchema = ByteTableSchema.fromTableSchema(projectSchema);
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public DynamicTableSource copy() {
		return new ByteTableDynamicTableSource(conf, tableName, byteTableSchema, nullStringLiteral, rateLimiter, parallelism);
	}

	@Override
	public String asSummaryString() {
		return "ByteTable";
	}

	// -------------------------------------------------------------------------------------------

	@VisibleForTesting
	public ByteTableSchema getByteTableSchema() {
		return this.byteTableSchema;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ByteTableDynamicTableSource source = (ByteTableDynamicTableSource) o;
		return parallelism == source.parallelism
			&& Objects.equals(tableName, source.tableName)
			&& Objects.equals(byteTableSchema, source.byteTableSchema)
			&& Objects.equals(nullStringLiteral, source.nullStringLiteral);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableName, byteTableSchema, nullStringLiteral, parallelism);
	}
}

