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

package org.apache.flink.formats.pb;

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.google.protobuf.Descriptors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.pb.PbOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.pb.PbOptions.IS_AD_INSTANCE_FORMAT;
import static org.apache.flink.formats.pb.PbOptions.PB_CLASS;
import static org.apache.flink.formats.pb.PbOptions.SINK_WITH_SIZE_HEADER;
import static org.apache.flink.formats.pb.PbOptions.SKIP_BYTES;
import static org.apache.flink.formats.pb.PbOptions.WITH_WRAPPER;

/**
 * Table format factory for providing configured instances of Pb to RowData {@link DeserializationSchema}.
 */
public class PbFormatFactory implements
		DeserializationFormatFactory,
		SerializationFormatFactory,
		TableSchemaInferrable {

	private static final String IDENTIFIER = "pb";

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);

		final String pbClass = formatOptions.get(PB_CLASS);
		final int skipBytes = formatOptions.get(SKIP_BYTES);
		final boolean withWrapper = formatOptions.get(WITH_WRAPPER);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
		final boolean isAdInstanceFormat = formatOptions.get(IS_AD_INSTANCE_FORMAT);

		return new DecodingFormat<DeserializationSchema<RowData>>() {
			@Override
			public DeserializationSchema<RowData> createRuntimeDecoder(
					DynamicTableSource.Context context,
					DataType producedDataType) {
				final RowType rowType = (RowType) producedDataType.getLogicalType();

				//noinspection unchecked
				TypeInformation<RowData> rowDataTypeInfo =
					(TypeInformation<RowData>) context.createTypeInformation(producedDataType);

				return PbRowDataDeserializationSchema.builder()
					.setPbDescriptorClass(pbClass)
					.setRowType(rowType)
					.setResultTypeInfo(rowDataTypeInfo)
					.setSkipBytes(skipBytes)
					.setWithWrapper(withWrapper)
					.setAdInstanceFormat(isAdInstanceFormat)
					.setIgnoreParseErrors(ignoreParseErrors)
					.build();
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);

		final String pbClass = formatOptions.get(PB_CLASS);
		final boolean withWrapper = formatOptions.get(WITH_WRAPPER);
		final boolean sinkWithSizeHeader = formatOptions.get(SINK_WITH_SIZE_HEADER);

		return new EncodingFormat<SerializationSchema<RowData>>() {
			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(
					DynamicTableSink.Context context,
					DataType consumedDataType) {
				final RowType rowType = (RowType) consumedDataType.getLogicalType();

				return PbRowDataSerializationSchema.builder()
					.setRowType(rowType)
					.setPbDescriptorClass(pbClass)
					.setWithWrapper(withWrapper)
					.setSinkWithSizeHeader(sinkWithSizeHeader)
					.build();
			}

			@Override
			public ChangelogMode getChangelogMode() {
				return ChangelogMode.insertOnly();
			}
		};
	}

	@Override
	public TableSchema getTableSchema(Map<String, String> formatOptions) {
		final String pbClass = formatOptions.get(fullKey(PB_CLASS.key()));
		final boolean withWrapper = Boolean.parseBoolean(formatOptions.get(fullKey(WITH_WRAPPER.key())));
		FieldsDataType fieldsDataType =
			createDataType(PbUtils.validateAndGetDescriptor(pbClass), withWrapper);

		RowType rowType = (RowType) fieldsDataType.getLogicalType();
		return TableSchema.builder()
			.fields(rowType.getFieldNames().toArray(new String[0]),
				fieldsDataType.getChildren().toArray(new DataType[0]))
			.build();
	}

	@VisibleForTesting
	static String fullKey(String key) {
		return IDENTIFIER + "." + key;
	}

	@VisibleForTesting
	static FieldsDataType createDataType(Descriptors.Descriptor root, boolean withWrapper) {
		int size = root.getFields().size();
		DataTypes.Field[] rowFields = new DataTypes.Field[size];

		for (int i = 0; i < size; i++) {
			Descriptors.FieldDescriptor field = root.getFields().get(i);
			String fieldName = field.getName();
			DataType dataType = createFieldDataType(field);
			rowFields[i] = DataTypes.FIELD(fieldName, dataType);
		}

		FieldsDataType dataType = (FieldsDataType) DataTypes.ROW(rowFields);

		if (withWrapper) {
			dataType = (FieldsDataType) DataTypes.ROW(DataTypes.FIELD(PbConstant.FORMAT_PB_WRAPPER_NAME, dataType));
		}

		return dataType;
	}

	private static DataType createFieldDataType(Descriptors.FieldDescriptor field) {
		Descriptors.FieldDescriptor.JavaType fieldType = field.getJavaType();

		DataType dataType;
		if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
			if (field.isMapField()) {
				return DataTypes.MAP(
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.KEY)),
					createFieldDataType(field.getMessageType().findFieldByName(PbConstant.VALUE)));
			} else if (field.isRepeated()) {
				return DataTypes.ARRAY(createDataType(field.getMessageType(), false));
			} else {
				return createDataType(field.getMessageType(), false);
			}
		} else {
			if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.STRING)) {
				dataType = DataTypes.STRING();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.LONG)) {
				dataType = DataTypes.BIGINT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.BOOLEAN)) {
				dataType = DataTypes.BOOLEAN();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.INT)) {
				dataType = DataTypes.INT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.DOUBLE)) {
				dataType = DataTypes.DOUBLE();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
				dataType = DataTypes.FLOAT();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
				dataType = DataTypes.STRING();
			} else if (fieldType.equals(Descriptors.FieldDescriptor.JavaType.BYTE_STRING)) {
				dataType = DataTypes.BYTES();
			} else {
				throw new FlinkRuntimeException(String.format("Unsupported fieldType: %s.", fieldType));
			}
			if (field.isRepeated()) {
				return DataTypes.ARRAY(dataType);
			}
			return dataType;
		}
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.singleton(PB_CLASS);
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(SKIP_BYTES);
		options.add(WITH_WRAPPER);
		options.add(IGNORE_PARSE_ERRORS);
		options.add(IS_AD_INSTANCE_FORMAT);
		return options;
	}
}
