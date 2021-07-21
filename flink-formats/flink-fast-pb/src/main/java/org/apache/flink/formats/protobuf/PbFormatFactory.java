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

package org.apache.flink.formats.protobuf;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.TableSchemaInferrable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.formats.protobuf.PbFormatUtils.createDataType;
import static org.apache.flink.formats.protobuf.PbFormatUtils.getDescriptor;

/**
 * Table format factory for providing configured instances of Protobuf to RowData {@link
 * SerializationSchema} and {@link DeserializationSchema}.
 */
public class PbFormatFactory implements
		DeserializationFormatFactory,
		SerializationFormatFactory,
		TableSchemaInferrable {

	public static final String IDENTIFIER = "fast-pb";

	private static PbFormatConfig buildConfig(ReadableConfig formatOptions) {
		PbFormatConfig.PbFormatConfigBuilder configBuilder =
			new PbFormatConfig.PbFormatConfigBuilder();
		configBuilder.pbDescriptorClass(formatOptions.get(PbFormatOptions.PB_CLASS));
		formatOptions
			.getOptional(PbFormatOptions.IGNORE_PARSE_ERRORS)
			.ifPresent(configBuilder::ignoreParseErrors);
		formatOptions
			.getOptional(PbFormatOptions.READ_DEFAULT_VALUES)
			.ifPresent(configBuilder::readDefaultValues);
		formatOptions
			.getOptional(PbFormatOptions.WRITE_NULL_STRING_LITERAL)
			.ifPresent(configBuilder::writeNullStringLiterals);
		formatOptions
			.getOptional(PbFormatOptions.WITH_WRAPPER)
			.ifPresent(configBuilder::withWrapper);
		formatOptions
			.getOptional(PbFormatOptions.SKIP_BYTES)
			.ifPresent(configBuilder::skipBytes);
		formatOptions
			.getOptional(PbFormatOptions.IS_AD_INSTANCE_FORMAT)
			.ifPresent(configBuilder::isAdInstanceFormat);
		formatOptions
			.getOptional(PbFormatOptions.SINK_WITH_SIZE_HEADER)
			.ifPresent(configBuilder::sinkWithSizeHeader);
		formatOptions
			.getOptional(PbFormatOptions.SIZE_HEADER_WITH_LITTLE_ENDIAN)
			.ifPresent(configBuilder::sizeHeaderWithLittleEndian);
		return configBuilder.build();
	}

	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
		DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		return new PbDecodingFormat(buildConfig(formatOptions));
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
		DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		return new PbEncodingFormat(buildConfig(formatOptions));
	}

	@Override
	public Optional<TableSchema> getOptionalTableSchema(Map<String, String> formatOptions) {
		final String pbClass = formatOptions.get(fullKey(PbFormatOptions.PB_CLASS.key()));
		if (pbClass == null || pbClass.isEmpty()) {
			return Optional.empty();
		}
		final boolean withWrapper = Boolean.parseBoolean(formatOptions.get(fullKey(PbFormatOptions.WITH_WRAPPER.key())));
		FieldsDataType fieldsDataType =
			createDataType(getDescriptor(pbClass), withWrapper);

		RowType rowType = (RowType) fieldsDataType.getLogicalType();
		TableSchema tableSchema = TableSchema.builder()
			.fields(rowType.getFieldNames().toArray(new String[0]),
				fieldsDataType.getChildren().toArray(new DataType[0]))
			.build();
		return Optional.of(tableSchema);
	}

	@VisibleForTesting
	static String fullKey(String key) {
		return IDENTIFIER + "." + key;
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> result = new HashSet<>();
		result.add(PbFormatOptions.PB_CLASS);
		return result;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> result = new HashSet<>();
		result.add(PbFormatOptions.IGNORE_PARSE_ERRORS);
		result.add(PbFormatOptions.READ_DEFAULT_VALUES);
		result.add(PbFormatOptions.WITH_WRAPPER);
		result.add(PbFormatOptions.SKIP_BYTES);
		result.add(PbFormatOptions.IS_AD_INSTANCE_FORMAT);
		result.add(PbFormatOptions.SINK_WITH_SIZE_HEADER);
		result.add(PbFormatOptions.SIZE_HEADER_WITH_LITTLE_ENDIAN);
		return result;
	}
}
