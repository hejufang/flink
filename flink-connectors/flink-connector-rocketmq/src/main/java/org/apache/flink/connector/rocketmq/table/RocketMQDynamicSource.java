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

package org.apache.flink.connector.rocketmq.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQConsumer;
import org.apache.flink.connector.rocketmq.RocketMQOptions;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchemaWrapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;

/**
 * RocketMQDynamicSource.
 */
public class RocketMQDynamicSource implements ScanTableSource {
	private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private DataType outputDataType;
	private Map<String, String> props;
	private RocketMQConfig<RowData> rocketMQConfig;

	public RocketMQDynamicSource(
			DataType outputDataType,
			Map<String, String> props,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			RocketMQConfig<RowData> rocketMQConfig) {
		this.outputDataType = outputDataType;
		this.props = props;
		this.decodingFormat = decodingFormat;
		this.rocketMQConfig = rocketMQConfig;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return decodingFormat.getChangelogMode();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> schema = decodingFormat.createRuntimeDecoder(runtimeProviderContext, outputDataType);
		RocketMQConsumer<RowData> consumer =
			new RocketMQConsumer<>(new RocketMQDeserializationSchemaWrapper<>(schema), props, rocketMQConfig);
		return SourceFunctionProvider.of(consumer, false);
	}

	@Override
	public DynamicTableSource copy() {
		return new RocketMQDynamicSource(outputDataType, props, decodingFormat, rocketMQConfig);
	}

	@Override
	public String asSummaryString() {
		return RocketMQOptions.CONNECTOR_TYPE_VALUE_ROCKETMQ;
	}
}
