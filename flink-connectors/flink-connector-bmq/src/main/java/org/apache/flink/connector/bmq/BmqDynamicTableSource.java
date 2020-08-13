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

package org.apache.flink.connector.bmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * DynamicTableSource for BMQ.
 */
public class BmqDynamicTableSource implements ScanTableSource {

	private final String cluster;
	private final String topic;
	private final long startMs;
	private final long endMs;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType outputDataType;
	private final boolean ignoreUnhealthySegment;

	public BmqDynamicTableSource(
			String cluster,
			String topic,
			long startMs,
			long endMs,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType outputDataType,
			boolean ignoreUnhealthySegment) {
		this.cluster = cluster;
		this.topic = topic;
		this.startMs = startMs;
		this.endMs = endMs;
		this.decodingFormat = decodingFormat;
		this.outputDataType = outputDataType;
		this.ignoreUnhealthySegment = ignoreUnhealthySegment;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema =
			decodingFormat.createRuntimeDecoder(runtimeProviderContext, outputDataType);

		return InputFormatProvider.of(
			BmqInputFormat.builder()
				.setCluster(cluster)
				.setTopic(topic)
				.setStartMs(startMs)
				.setEndMs(endMs)
				.setDeserializationSchema(deserializationSchema)
				.setIgnoreUnhealthySegment(ignoreUnhealthySegment)
				.build());
	}

	@Override
	public DynamicTableSource copy() {
		return new BmqDynamicTableSource(
			cluster,
			topic,
			startMs,
			endMs,
			decodingFormat,
			outputDataType,
			ignoreUnhealthySegment);
	}

	@Override
	public String asSummaryString() {
		return "BMQ";
	}
}
