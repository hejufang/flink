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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.bmq.config.BmqSourceConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * DynamicTableSource for BMQ.
 */
public class BmqDynamicTableSource implements
		ScanTableSource,
		SupportsLimitPushDown,
		SupportsProjectionPushDown {

	private final DynamicTableFactory.Context context;
	private final BmqSourceConfig bmqSourceConfig;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final DataType outputDataType;
	// Nested projection push down is not supported yet.
	private int[] projectedFields;
	private final TableSchema schema;
	private long limit;

	public BmqDynamicTableSource(
			DynamicTableFactory.Context context,
			BmqSourceConfig bmqSourceConfig,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			DataType outputDataType) {
		this.context = context;
		this.bmqSourceConfig = bmqSourceConfig;
		this.decodingFormat = decodingFormat;
		this.outputDataType = outputDataType;
		this.schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		this.projectedFields = IntStream.range(0, schema.getFieldCount()).toArray();
		this.limit = -1L;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		String version = bmqSourceConfig.getVersion();
		String topic = bmqSourceConfig.getTopic();
		String cluster = bmqSourceConfig.getCluster();
		long startMs = bmqSourceConfig.getScanStartTimeMs();
		long endMs = bmqSourceConfig.getScanEndTimeMs();
		boolean ignoreUnhealthySegment = bmqSourceConfig.isIgnoreUnhealthySegment();
		Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap = bmqSourceConfig.getMetadataMap();

		if (version.equals("1")) {
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
		} else {
			return InputFormatProvider.of(
				BmqFileInputFormat.builder()
					.setCluster(cluster)
					.setTopic(topic)
					.setStartMs(startMs)
					.setEndMs(endMs)
					.setFullFieldNames(schema.getFieldNames())
					.setFullFieldTypes(schema.getFieldDataTypes())
					.setSelectedFields(projectedFields)
					.setMetadataMap(metadataMap)
					.setLimit(limit)
					.setConf(new Configuration())
					.setUtcTimeStamp(true)
					.build());
		}
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.projectedFields = Arrays.stream(projectedFields)
			.mapToInt(array -> this.projectedFields[array[0]]).toArray();
	}

	@Override
	public DynamicTableSource copy() {
		return new BmqDynamicTableSource(
			context,
			bmqSourceConfig,
			decodingFormat,
			outputDataType);
	}

	@Override
	public String asSummaryString() {
		return "BMQ";
	}

	@Override
	public void applyLimit(long limit) {
		this.limit = limit;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BmqDynamicTableSource that = (BmqDynamicTableSource) o;
		return limit == that.limit &&
			Objects.equals(bmqSourceConfig, that.bmqSourceConfig) &&
			Objects.equals(outputDataType, that.outputDataType) &&
			Arrays.equals(projectedFields, that.projectedFields);
	}

	@Override
	public String toString() {
		return "BmqDynamicTableSource{" +
			", bmqSourceConfig=" + bmqSourceConfig +
			", outputDataType=" + outputDataType +
			", projectedFields=" + Arrays.toString(projectedFields) +
			", limit=" + limit +
			'}';
	}

	@VisibleForTesting
	protected DataType getProducedDataType() {
		TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
		String[] schemaFieldNames = schema.getFieldNames();
		DataType[] schemaTypes = schema.getFieldDataTypes();

		return DataTypes.ROW(Arrays.stream(projectedFields)
				.mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
				.toArray(DataTypes.Field[]::new))
			.bridgedTo(RowData.class);
	}
}
