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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQProducer;
import org.apache.flink.connector.rocketmq.serialization.KeyByPartitionSerializationSchema;
import org.apache.flink.connector.rocketmq.serialization.KeyValueSerializationSchemaWrapper;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Map;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.CONNECTOR_TYPE_VALUE_ROCKETMQ;

/**
 * RocketMQDynamicSink.
 */
public class RocketMQDynamicSink implements DynamicTableSink {
	private DataType consumedDataType;
	private Map<String, String> props;
	private EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private RocketMQConfig<RowData> rocketMQConfig;

	public RocketMQDynamicSink(
			DataType consumedDataType,
			Map<String, String> props,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			RocketMQConfig<RowData> rocketMQConfig) {
		this.consumedDataType = consumedDataType;
		this.props = props;
		this.encodingFormat = encodingFormat;
		this.rocketMQConfig = rocketMQConfig;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return encodingFormat.getChangelogMode();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		KeyValueSerializationSchemaWrapper<RowData> keyValueSerializationSchemaWrapper =
			getSchemaByConfig(encodingFormat.createRuntimeEncoder(context, consumedDataType), rocketMQConfig);
		final RocketMQProducer<RowData> rocketMQProducer =
			new RocketMQProducer<RowData>(keyValueSerializationSchemaWrapper, props, rocketMQConfig);
		return SinkFunctionProvider.of(rocketMQProducer);
	}

	@Override
	public DynamicTableSink copy() {
		return new RocketMQDynamicSink(consumedDataType, props, encodingFormat, rocketMQConfig);
	}

	@Override
	public String asSummaryString() {
		return CONNECTOR_TYPE_VALUE_ROCKETMQ;
	}

	private static KeyValueSerializationSchemaWrapper<RowData> getSchemaByConfig(
			SerializationSchema<RowData> serializationSchema,
			RocketMQConfig<RowData> rocketMQConfig) {
		if (rocketMQConfig.getSinkKeyByFields() != null) {
			return new KeyByPartitionSerializationSchema(serializationSchema, rocketMQConfig.getSinkKeyByFields());
		} else {
			return new KeyValueSerializationSchemaWrapper<>(serializationSchema);
		}
	}
}
