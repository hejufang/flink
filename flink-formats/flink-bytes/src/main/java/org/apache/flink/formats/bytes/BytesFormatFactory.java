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

package org.apache.flink.formats.bytes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory for creating bytes ser/deserializer.
 */
public class BytesFormatFactory implements
		DeserializationFormatFactory,
		SerializationFormatFactory {

	public static final String IDENTIFIER = "bytes";

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {

		validateSchema(context.getCatalogTable().getSchema(),
			FactoryUtil.parseMetadataColumn(formatOptions.getOptional(FactoryUtil.SOURCE_METADATA_COLUMNS)));

		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}

			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
					DynamicTableSource.Context context,
					DataType producedDataType) {
				//noinspection unchecked
				final TypeInformation<RowData> rowDataTypeInfo =
					(TypeInformation<RowData>) context.createTypeInformation(producedDataType);
				return new BytesDeserializationSchema(rowDataTypeInfo);
			}
		};
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {

		validateSchema(context.getCatalogTable().getSchema(), new HashSet<>());

		return new EncodingFormat<SerializationSchema<RowData>>() {
			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}

			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(
					DynamicTableSink.Context context,
					DataType consumedDataType) {
				return new BytesSerializationSchema();
			}
		};
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Collections.emptySet();
	}

	private void validateSchema(TableSchema schema, Set<String> ignoreColumns) {
		// TableSchema#toPhysicalRowDataType will remove generated fields (computed column)
		List<DataType> dataTypes = schema.toPhysicalRowDataTypeWithFilter(
			c -> !ignoreColumns.contains(c.getName())).getChildren();
		Preconditions.checkArgument(dataTypes.size() == 1,
			String.format("BytesFormat only accept one column except computed column, but you provide %d fields.",
				schema.getFieldCount()));
		Preconditions.checkArgument(
			DataTypes.BYTES().equals(dataTypes.get(0)),
			String.format("BytesFormat only accept varbinary type, but you provide %s.",
				schema.getFieldDataType(0)));
	}
}
