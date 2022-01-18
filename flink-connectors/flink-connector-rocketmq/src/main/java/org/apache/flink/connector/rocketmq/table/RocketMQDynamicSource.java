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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQConsumer;
import org.apache.flink.connector.rocketmq.RocketMQMetadata;
import org.apache.flink.connector.rocketmq.RocketMQOptions;
import org.apache.flink.connector.rocketmq.serialization.RocketMQBoundedDeserializationSchema;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchema;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchemaWrapper;
import org.apache.flink.rocketmq.source.RocketMQSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.mqproxy.proto.MessageExt;
import com.bytedance.rocketmq.clientv2.message.MessageQueue;

import java.util.Map;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_END_OFFSET;
import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_END_TIMESTAMP;

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
		RocketMQDeserializationSchema<RowData> rocketMQDeserializationSchema = createDeserializationSchema(schema);
		if (rocketMQConfig.isUseFlip27Source()) {
			return SourceProvider.of(getRocketMQSource(rocketMQDeserializationSchema, rocketMQConfig, props));
		}

		RocketMQConsumer<RowData> consumer =
			new RocketMQConsumer<>(rocketMQDeserializationSchema, props, rocketMQConfig);
		if (rocketMQConfig.getKeySelector() != null) {
			DataStreamScanProvider dataStreamScanProvider = new DataStreamScanProvider() {
				@Override
				public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
					RowDataTypeInfo typeInfo = (RowDataTypeInfo) consumer.getProducedType();
					return execEnv.addSource(consumer, typeInfo)
						.keyBy(rocketMQConfig.getKeySelector());
				}

				@Override
				public boolean isBounded() {
					return false;
				}
			};
			return dataStreamScanProvider;
		}
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

	private RocketMQDeserializationSchemaWrapper<RowData> createDeserializationSchema(
			DeserializationSchema<RowData> schema) {
		if (rocketMQConfig.getMetadataMap() != null) {
			return new RocketMQWithMetadataDeserializationSchema(outputDataType.getChildren().size(), schema,
				rocketMQConfig.getMetadataMap(), rocketMQConfig.getEndTimestamp(), rocketMQConfig.getEndOffset());
		} else if (rocketMQConfig.getEndOffset() != SCAN_END_OFFSET.defaultValue() ||
				rocketMQConfig.getEndTimestamp() != SCAN_END_TIMESTAMP.defaultValue()) {
			return new RocketMQBoundedDeserializationSchema<>(
				schema, rocketMQConfig.getEndTimestamp(), rocketMQConfig.getEndOffset());
		} else {
			return new RocketMQDeserializationSchemaWrapper<>(schema);
		}
	}

	private RocketMQSource<RowData> getRocketMQSource(
			RocketMQDeserializationSchema<RowData> schema,
			RocketMQConfig<RowData> rocketMQConfig,
			Map<String, String> props) {
		return new RocketMQSource<>(
			schema.isStreamingMode() ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED,
			schema,
			props,
			rocketMQConfig);
	}

	private static final class RocketMQWithMetadataDeserializationSchema
			extends RocketMQBoundedDeserializationSchema<RowData> {
		private final int outFieldNum;
		private final Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;

		public RocketMQWithMetadataDeserializationSchema(
				int outFieldNum,
				DeserializationSchema<RowData> deserializationSchema,
				Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap,
				long endTimestamp,
				long endOffset) {
			super(deserializationSchema, endTimestamp, endOffset);
			assert deserializationSchema.getProducedType().getArity() + metadataMap.size() == outFieldNum;
			this.metadataMap = metadataMap;
			this.outFieldNum = outFieldNum;
		}

		@Override
		public RowData deserialize(MessageQueue messageQueue, MessageExt record) throws Exception {
			return addMetadata(record, super.deserialize(messageQueue, record));
		}

		@Override
		public void deserialize(byte[] message, Collector<RowData> out) throws Exception {
			throw new FlinkRuntimeException("Shouldn't reach here.");
		}

		private RowData addMetadata(MessageExt record, RowData rowData) {
			GenericRowData oldRowData = (GenericRowData) rowData;
			GenericRowData newRowData = new GenericRowData(outFieldNum);
			for (int i = 0, j = 0; i < outFieldNum; i++) {
				RocketMQMetadata metadata = (RocketMQMetadata) this.metadataMap.get(i);
				if (metadata != null) {
					newRowData.setField(i, getMetadata(record, metadata));
				} else {
					newRowData.setField(i, oldRowData.getField(j++));
				}
			}
			return newRowData;
		}

		private Object getMetadata(MessageExt record, RocketMQMetadata metadata) {
			switch (metadata) {
				case OFFSET:
					return record.getQueueOffset();
				case TIMESTAMP:
					return record.getBornTimestamp();
				case QUEUE_ID:
					return (long) record.getMessageQueue().getQueueId();
				case BROKER_NAME:
					return StringData.fromString(record.getMessageQueue().getBrokerName());
				case MESSAGE_ID:
					return StringData.fromString(record.getMsgId());
				default:
					throw new FlinkRuntimeException("Unsupported metadata: " + metadata.getMetadata());
			}
		}
	}
}
