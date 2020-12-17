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

package org.apache.flink.connector.databus.table;

import org.apache.flink.api.common.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.databus.DatabusOutputFormat;
import org.apache.flink.connector.databus.DatabusSinkFunction;
import org.apache.flink.connector.databus.options.DatabusConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.serialization.IgnoreDeleteMsgSerializationSchema;
import org.apache.flink.types.RowKind;

/**
 * Databus dynamic table sink.
 */
public class DatabusDynamicTableSink implements DynamicTableSink {

	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private final DatabusConfig dataDatabusConfig;
	private final TableSchema schema;

	public DatabusDynamicTableSink(
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			DatabusConfig dataDatabusConfig,
			TableSchema schema) {
		this.encodingFormat = encodingFormat;
		this.dataDatabusConfig = dataDatabusConfig;
		this.schema = schema;
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
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		SerializationSchema<RowData> serializationSchema =
			encodingFormat.createRuntimeEncoder(context, schema.toRowDataType());

		IgnoreDeleteMsgSerializationSchema ignoreDeleteMsgSerializationSchema =
			new IgnoreDeleteMsgSerializationSchema(serializationSchema);
		KeyedSerializationSchemaWrapper<RowData> keyedSerializationSchemaWrapper =
			new KeyedSerializationSchemaWrapper<>(ignoreDeleteMsgSerializationSchema);

		DatabusOutputFormat<RowData> outputFormat =
			new DatabusOutputFormat<>(dataDatabusConfig, keyedSerializationSchemaWrapper);
		DatabusSinkFunction<RowData> databusSinkFunction =
			new DatabusSinkFunction<>(outputFormat, dataDatabusConfig.getParallelism());
		return SinkFunctionProvider.of(databusSinkFunction);
	}

	@Override
	public DynamicTableSink copy() {
		return new DatabusDynamicTableSink(encodingFormat, dataDatabusConfig, schema.copy());
	}

	@Override
	public String asSummaryString() {
		return "Databus table sink";
	}
}
