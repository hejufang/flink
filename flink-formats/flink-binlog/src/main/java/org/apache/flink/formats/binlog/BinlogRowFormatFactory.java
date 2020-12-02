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

package org.apache.flink.formats.binlog;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.bytedance.binlog.DRCEntry;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.binlog.BinlogOptions.BINLOG_BODY;
import static org.apache.flink.formats.binlog.BinlogOptions.BINLOG_HEADER;
import static org.apache.flink.formats.binlog.BinlogOptions.IGNORE_PARSER_ERROR;
import static org.apache.flink.formats.binlog.BinlogOptions.TARGET_TABLE;

/**
 * Binlog row format factory.
 */
public class BinlogRowFormatFactory implements
		DeserializationFormatFactory,
		TableSchemaInferrable {
	private static final String IDENTIFIER = "binlog";

	@Override
	public TableSchema getTableSchema(Map<String, String> formatOptions) {
		TableSchema.Builder tableBuilder = TableSchema.builder();
		DataType headerDataType = PbFormatUtils.createDataType(DRCEntry.EntryHeader.getDescriptor(), false);
		tableBuilder.field(BINLOG_HEADER, headerDataType);
		DataType bodyDataType = PbFormatUtils.createDataType(DRCEntry.EntryBody.getDescriptor(), false);
		tableBuilder.field(BINLOG_BODY, bodyDataType);
		return tableBuilder.build();
	}

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		final String tableName = formatOptions.get(TARGET_TABLE);
		final boolean ignoreErrors = formatOptions.get(IGNORE_PARSER_ERROR);
		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
					DynamicTableSource.Context context,
					DataType producedDataType) {
				return BinlogRowDeserializationSchema.builder()
						.setTargetTable(tableName)
						.setRowType((RowType) producedDataType.getLogicalType())
						.setIgnoreParseErrors(ignoreErrors)
						.setResultTypeInfo((TypeInformation<RowData>) context.createTypeInformation(producedDataType))
						.build();
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.singleton(TARGET_TABLE);
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Collections.singleton(IGNORE_PARSER_ERROR);
	}
}
