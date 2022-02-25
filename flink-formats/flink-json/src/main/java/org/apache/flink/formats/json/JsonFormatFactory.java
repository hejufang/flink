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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.ProjectionPushDownableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser.Feature;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.json.JsonOptions.BYTES_AS_JSON_NODE;
import static org.apache.flink.formats.json.JsonOptions.DEFAULT_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonOptions.ENCODE_IGNORE_NULL_VALUES;
import static org.apache.flink.formats.json.JsonOptions.ENFORCE_UTF8_ENCODING;
import static org.apache.flink.formats.json.JsonOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonOptions.LOG_ERROR_RECORDS_INTERVAL;
import static org.apache.flink.formats.json.JsonOptions.TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.JsonOptions.UNWRAPPED_FIELD_NAMES;

/**
 * Table format factory for providing configured instances of JSON to RowData
 * {@link SerializationSchema} and {@link DeserializationSchema}.
 */
public class JsonFormatFactory implements
		DeserializationFormatFactory,
		SerializationFormatFactory {

	public static final String IDENTIFIER = "json";
	public static final String PARSER_FEATURE_PREFIX = "json.parser-feature.";

	@SuppressWarnings("unchecked")
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);
		validateFormatOptions(formatOptions);

		final boolean failOnMissingField = formatOptions.get(FAIL_ON_MISSING_FIELD);
		final boolean defaultOnMissingField = formatOptions.get(DEFAULT_ON_MISSING_FIELD);
		final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
		final boolean byteAsJsonNode = formatOptions.get(BYTES_AS_JSON_NODE);
		final long logParseErrorsInterval = formatOptions.get(LOG_ERROR_RECORDS_INTERVAL).toMillis();
		Map<Feature, Boolean> parserFeature = getParserFeatureMap(context.getCatalogTable().getOptions());
		TimestampFormat timestampOption = formatOptions.get(TIMESTAMP_FORMAT);

		return new JsonDeserializationSchemaDecodingFormat(
				failOnMissingField,
				defaultOnMissingField,
				ignoreParseErrors,
				byteAsJsonNode,
				timestampOption,
				logParseErrorsInterval,
				parserFeature);
	}

	@Override
	public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
			DynamicTableFactory.Context context,
			ReadableConfig formatOptions) {
		FactoryUtil.validateFactoryOptions(this, formatOptions);

		TimestampFormat timestampOption = formatOptions.get(TIMESTAMP_FORMAT);
		final boolean enforceUTF8Encoding = formatOptions.get(ENFORCE_UTF8_ENCODING);
		final boolean ignoreNullValues = formatOptions.get(ENCODE_IGNORE_NULL_VALUES);
		final boolean byteAsJsonNode = formatOptions.get(BYTES_AS_JSON_NODE);
		final List<String> unwrappedFiledNameList = formatOptions.getOptional(UNWRAPPED_FIELD_NAMES).orElse(null);

		return new EncodingFormat<SerializationSchema<RowData>>() {
			@Override
			public SerializationSchema<RowData> createRuntimeEncoder(
					DynamicTableSink.Context context,
					DataType consumedDataType) {
				final RowType rowType = (RowType) consumedDataType.getLogicalType();
				return JsonRowDataSerializationSchema.builder()
						.setRowType(rowType)
						.setTimestampFormat(timestampOption)
						.setEnforceUTF8Encoding(enforceUTF8Encoding)
						.setIgnoreNullValues(ignoreNullValues)
						.setByteAsJsonNode(byteAsJsonNode)
						.setUnwrappedFiledNames(unwrappedFiledNameList)
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
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FAIL_ON_MISSING_FIELD);
		options.add(DEFAULT_ON_MISSING_FIELD);
		options.add(IGNORE_PARSE_ERRORS);
		options.add(LOG_ERROR_RECORDS_INTERVAL);
		options.add(TIMESTAMP_FORMAT);
		options.add(ENFORCE_UTF8_ENCODING);
		options.add(ENCODE_IGNORE_NULL_VALUES);
		options.add(BYTES_AS_JSON_NODE);
		options.add(UNWRAPPED_FIELD_NAMES);
		return options;
	}

	@Override
	public Set<String> optionalPrefixes() {
		Set<String> prefixes = new HashSet<>();
		prefixes.add(PARSER_FEATURE_PREFIX);
		return prefixes;
	}

	// ------------------------------------------------------------------------
	//  Validation
	// ------------------------------------------------------------------------

	static void validateFormatOptions(ReadableConfig tableOptions) {
		boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
		boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
		if (ignoreParseErrors && failOnMissingField) {
			throw new ValidationException(FAIL_ON_MISSING_FIELD.key()
					+ " and "
					+ IGNORE_PARSE_ERRORS.key()
					+ " shouldn't both be true.");
		}
	}

	public static Map<Feature, Boolean> getParserFeatureMap(Map<String, String> tableOptions) {
		Map<Feature, Boolean> res = new HashMap<>();

		tableOptions.keySet().stream()
			.filter(key -> key.startsWith(PARSER_FEATURE_PREFIX))
			.forEach(key -> {
				final String value = tableOptions.get(key);
				final String subKey = key.substring((PARSER_FEATURE_PREFIX).length()).toUpperCase();
				res.put(Feature.valueOf(subKey), Boolean.parseBoolean(value));
			});

		return res;
	}

	private static class JsonDeserializationSchemaDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>>, ProjectionPushDownableDecodingFormat {
		private final boolean failOnMissingField;
		private final boolean defaultOnMissingField;
		private final boolean ignoreParseErrors;
		private final boolean byteAsJsonNode;
		private final TimestampFormat timestampOption;
		private final long logParseErrorsInterval;
		private final Map<Feature, Boolean> parserFeature;

		public JsonDeserializationSchemaDecodingFormat(
				boolean failOnMissingField,
				boolean defaultOnMissingField,
				boolean ignoreParseErrors,
				boolean byteAsJsonNode,
				TimestampFormat timestampOption,
				long logParseErrorsInterval,
				Map<Feature, Boolean> parserFeature) {
			this.failOnMissingField = failOnMissingField;
			this.defaultOnMissingField = defaultOnMissingField;
			this.ignoreParseErrors = ignoreParseErrors;
			this.byteAsJsonNode = byteAsJsonNode;
			this.timestampOption = timestampOption;
			this.logParseErrorsInterval = logParseErrorsInterval;
			this.parserFeature = parserFeature;
		}

		@Override
		public DeserializationSchema<RowData> createRuntimeDecoder(
				DynamicTableSource.Context context,
				DataType producedDataType) {
			final RowType rowType = (RowType) producedDataType.getLogicalType();
			final TypeInformation<RowData> rowDataTypeInfo =
				(TypeInformation<RowData>) context.createTypeInformation(producedDataType);
			return new JsonRowDataDeserializationSchema(
				rowType,
				rowDataTypeInfo,
				failOnMissingField,
				defaultOnMissingField,
				ignoreParseErrors,
				byteAsJsonNode,
				timestampOption,
				logParseErrorsInterval,
				parserFeature
			);
		}

		@Override
		public ChangelogMode getChangelogMode() {
			return ChangelogMode.insertOnly();
		}
	}
}
