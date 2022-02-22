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

package org.apache.flink.connector.bytetable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.bytetable.options.ByteTableOptions;
import org.apache.flink.connector.bytetable.options.ByteTableWriteOptions;
import org.apache.flink.connector.bytetable.util.BConstants;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Bytetable table sink implementation.
 */
@Internal
public class ByteTableDynamicTableSink implements DynamicTableSink {

	private final ByteTableSchema byteTableSchema;
	private final ByteTableOptions byteTableOptions;
	private final ByteTableWriteOptions writeOptions;
	private final String nullStringLiteral;

	public ByteTableDynamicTableSink(
			ByteTableSchema byteTableSchema,
			ByteTableOptions byteTableOptions,
			ByteTableWriteOptions writeOptions,
			String nullStringLiteral) {
		this.byteTableSchema = byteTableSchema;
		this.byteTableOptions = byteTableOptions;
		this.writeOptions = writeOptions;
		this.nullStringLiteral = nullStringLiteral;
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		Configuration byteTableClientConf = HBaseConfiguration.create();
		//Connection impl should be com.bytedance.bytetable.hbase.BytetableConnection when use ByteTable.
		byteTableClientConf.set(BConstants.HBASE_CLIENT_CONNECTION_IMPL, BConstants.BYTETABLE_CLIENT_IMPL);
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_PSM, byteTableOptions.getPsm());
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_CLUSTERNAME, byteTableOptions.getCluster());
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_SERVICENAME, byteTableOptions.getService());
		//ByteTable does not support RegionSizeCalculator, so set it false.
		byteTableClientConf.setBoolean(BConstants.HBASE_REGIONSIZECALCULATOR_ENABLE, false);
		//hbase use database_name:table_name to get table
		String byteTableName = byteTableOptions.getDatabase() + ":" + byteTableOptions.getTableName();
		ByteTableSinkFunction<RowData> sinkFunction = new ByteTableSinkFunction<>(
			byteTableName,
			byteTableClientConf,
			byteTableOptions,
			new RowDataToMutationConverter(byteTableSchema, nullStringLiteral, writeOptions.getCellTTLMicroSeconds()),
			writeOptions.getBufferFlushMaxSizeInBytes(),
			writeOptions.getBufferFlushMaxRows(),
			writeOptions.getBufferFlushIntervalMillis(),
			writeOptions.isIgnoreDelete(),
			byteTableOptions.getRateLimiter(),
			byteTableSchema.getCellVersionIndex());
		return SinkFunctionProvider.of(sinkFunction);
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		// UPSERT mode
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public DynamicTableSink copy() {
		return new ByteTableDynamicTableSink(byteTableSchema, byteTableOptions, writeOptions, nullStringLiteral);
	}

	@Override
	public String asSummaryString() {
		return "Bytetable";
	}

	// -------------------------------------------------------------------------------------------

	@VisibleForTesting
	public ByteTableSchema getByteTableSchema() {
		return this.byteTableSchema;
	}

	@VisibleForTesting
	public ByteTableOptions getByteTableOptions() {
		return byteTableOptions;
	}

	@VisibleForTesting
	public ByteTableWriteOptions getByteTableWriteOptions() {
		return writeOptions;
	}
}
