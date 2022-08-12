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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.JsonOptions.LOG_ERROR_RECORDS_INTERVAL;
import static org.apache.flink.formats.json.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIME_FORMAT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@Internal
public class JsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(JsonRowDataDeserializationSchema.class);

	private static final long serialVersionUID = 1L;

	/** Flag indicating whether to fail if a field is missing. */
	private final boolean failOnMissingField;

	/** Flag indicating whether to fill missing field with default value. */
	private final boolean defaultOnMissingField;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	/** Flag indicating whether to ignore fields (default: throw an exception). */
	private final boolean ignoreFieldParseErrors;

	/** Flag indicating whether to decode a json node to varbinary. */
	private final boolean byteAsJsonNode;

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Runtime converter that converts {@link JsonNode}s into
	 * objects of Flink SQL internal data structures. **/
	private final DeserializationRuntimeConverter runtimeConverter;

	/** Default-value schema for missing field. */
	private JsonDefaultValue defaultValueSchema = new JsonDefaultValue();

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	/** Timestamp format specification which is used to parse timestamp. */
	private final TimestampFormat timestampFormat;

	/** The interval between each time the error being logged. As logging all errors may cost a lot. */
	private final long logErrorInterval;
	private long lastLogErrorTimestamp;
	private boolean booleanNumberConversion;

	/**
	 * @deprecated use {@link JsonRowDataDeserializationSchema.Builder} instead
	 */
	@Deprecated
	public JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormat) {
		this(
			rowType,
			resultTypeInfo,
			failOnMissingField,
			false,
			ignoreParseErrors,
			false,
			false,
			timestampFormat,
			LOG_ERROR_RECORDS_INTERVAL.defaultValue().toMillis(),
			new HashMap<>(),
			false);
	}

	/**
	 * @deprecated use {@link JsonRowDataDeserializationSchema.Builder} instead
	 */
	@Deprecated
	public JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean ignoreParseErrors,
			boolean byteAsJsonNode,
			TimestampFormat timestampFormat,
			long logErrorInterval,
			Map<JsonParser.Feature, Boolean> jsonParserFeatureMap) {
		this(
			rowType,
			resultTypeInfo,
			failOnMissingField,
			false,
			ignoreParseErrors,
			false,
			byteAsJsonNode,
			timestampFormat,
			logErrorInterval,
			jsonParserFeatureMap,
			false);
	}

	/**
	 * @deprecated use {@link JsonRowDataDeserializationSchema.Builder} instead
	 */
	@Deprecated
	public JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean defaultOnMissingField,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormat) {
		this(
			rowType,
			resultTypeInfo,
			failOnMissingField,
			defaultOnMissingField,
			ignoreParseErrors,
			false,
			false,
			timestampFormat,
			LOG_ERROR_RECORDS_INTERVAL.defaultValue().toMillis(),
			new HashMap<>(),
			false);
	}

	/**
	 * @deprecated use {@link JsonRowDataDeserializationSchema.Builder} instead
	 */
	@Deprecated
	public JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean defaultOnMissingField,
			boolean ignoreParseErrors,
			boolean byteAsJsonNode,
			TimestampFormat timestampFormat,
			long logErrorInterval,
			Map<JsonParser.Feature, Boolean> jsonParserFeatureMap,
			boolean booleanNumberConversion) {
		this(
			rowType,
			resultTypeInfo,
			failOnMissingField,
			defaultOnMissingField,
			ignoreParseErrors,
			false,
			byteAsJsonNode,
			timestampFormat,
			logErrorInterval,
			jsonParserFeatureMap,
			booleanNumberConversion);
	}

	private JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean defaultOnMissingField,
			boolean ignoreParseErrors,
			boolean ignoreFieldParseErrors,
			boolean byteAsJsonNode,
			TimestampFormat timestampFormat,
			long logErrorInterval,
			Map<JsonParser.Feature, Boolean> jsonParserFeatureMap,
			boolean booleanNumberConversion) {
		if (ignoreParseErrors && failOnMissingField) {
			throw new IllegalArgumentException(
				"JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
		}
		if (ignoreFieldParseErrors && failOnMissingField) {
			throw new IllegalArgumentException(
				"JSON format doesn't support failOnMissingField and ignoreFieldParseErrors are both enabled.");
		}
		this.resultTypeInfo = checkNotNull(resultTypeInfo);
		this.failOnMissingField = failOnMissingField;
		this.defaultOnMissingField = defaultOnMissingField;
		this.ignoreParseErrors = ignoreParseErrors;
		this.ignoreFieldParseErrors = ignoreFieldParseErrors;
		this.byteAsJsonNode = byteAsJsonNode;
		this.runtimeConverter = createRowConverter(checkNotNull(rowType));
		this.timestampFormat = timestampFormat;
		this.logErrorInterval = logErrorInterval;
		jsonParserFeatureMap.forEach(objectMapper::configure);
		this.booleanNumberConversion = booleanNumberConversion;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder class of {@link JsonRowDataDeserializationSchema}.
	 */
	public static class Builder {
		private RowType rowType;
		private TypeInformation<RowData> resultTypeInfo;
		private boolean failOnMissingField;
		private boolean defaultOnMissingField;
		private boolean ignoreParseErrors;
		private boolean ignoreFieldParseErrors;
		private boolean byteAsJsonNode;
		private TimestampFormat timestampFormat = TimestampFormat.SQL;
		private long logErrorInterval;
		private Map<JsonParser.Feature, Boolean> jsonParserFeatureMap = new HashMap<>();
		private boolean booleanNumberConversion;

		public Builder setRowType(RowType rowType) {
			this.rowType = rowType;
			return this;
		}

		public Builder setResultTypeInfo(TypeInformation<RowData> resultTypeInfo) {
			this.resultTypeInfo = resultTypeInfo;
			return this;
		}

		public Builder setFailOnMissingField(boolean failOnMissingField) {
			this.failOnMissingField = failOnMissingField;
			return this;
		}

		public Builder setDefaultOnMissingField(boolean defaultOnMissingField) {
			this.defaultOnMissingField = defaultOnMissingField;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setIgnoreFieldParseErrors(boolean ignoreFieldParseErrors) {
			this.ignoreFieldParseErrors = ignoreFieldParseErrors;
			return this;
		}

		public Builder setByteAsJsonNode(boolean byteAsJsonNode) {
			this.byteAsJsonNode = byteAsJsonNode;
			return this;
		}

		public Builder setTimestampFormat(TimestampFormat timestampFormat) {
			this.timestampFormat = timestampFormat;
			return this;
		}

		public Builder setLogErrorInterval(long logErrorInterval) {
			this.logErrorInterval = logErrorInterval;
			return this;
		}

		public Builder setJsonParserFeatureMap(Map<JsonParser.Feature, Boolean> jsonParserFeatureMap) {
			this.jsonParserFeatureMap = jsonParserFeatureMap;
			return this;
		}

		public Builder setBooleanNumberConversion(boolean booleanNumberConversion) {
			this.booleanNumberConversion = booleanNumberConversion;
			return this;
		}

		public JsonRowDataDeserializationSchema build() {
			return new JsonRowDataDeserializationSchema(
				rowType,
				resultTypeInfo,
				failOnMissingField,
				defaultOnMissingField,
				ignoreParseErrors,
				ignoreFieldParseErrors,
				byteAsJsonNode,
				timestampFormat,
				logErrorInterval,
				jsonParserFeatureMap,
				booleanNumberConversion);
		}
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return (RowData) runtimeConverter.convert(root);
		} catch (Throwable t) {
			String messageStr = message == null ? null : new String(message);
			if (ignoreParseErrors) {
				long currentTime = System.currentTimeMillis();
				if (currentTime - lastLogErrorTimestamp > logErrorInterval) {
					lastLogErrorTimestamp = currentTime;
					LOG.warn("Failed to deserialize JSON '{}', cause: {}", messageStr, t.getMessage());
				}
				return null;
			}
			throw new IOException(format("Failed to deserialize JSON '%s'.", messageStr), t);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JsonRowDataDeserializationSchema that = (JsonRowDataDeserializationSchema) o;
		return failOnMissingField == that.failOnMissingField &&
				defaultOnMissingField == that.defaultOnMissingField &&
				ignoreParseErrors == that.ignoreParseErrors &&
				byteAsJsonNode == that.byteAsJsonNode &&
				resultTypeInfo.equals(that.resultTypeInfo) &&
				timestampFormat.equals(that.timestampFormat) &&
				logErrorInterval == that.logErrorInterval;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			failOnMissingField,
			defaultOnMissingField,
			ignoreParseErrors,
			byteAsJsonNode,
			resultTypeInfo,
			timestampFormat,
			logErrorInterval);
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(JsonNode jsonNode);
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(type);
	}

	/**
	 * Wrap notNullConverter and default-value Converter.
	 * defaultOnMissingField option has not need to be checked here.{@link JsonRowDataDeserializationSchema#convertField(DeserializationRuntimeConverter, String, JsonNode)}
	 * @param type
	 * @return
	 */
	private DeserializationRuntimeConverter wrapIntoNullableConverter(LogicalType type) {
		DeserializationRuntimeConverter baseConverter = createBaseConverter(type);
		JsonDefaultValue.DeserializationRuntimeConverter defaultValueConverter = defaultValueSchema.createConverter(type);
		return (jsonNode) -> {
			// When jsonNode == null, defaultOnMissingField will always be true.
			if (jsonNode == null) {
				return defaultValueConverter.convert();
			}
			if (jsonNode.isNull()) {
				return null;
			}
			try {
				return baseConverter.convert(jsonNode);
			} catch (Throwable t) {
				if (!ignoreFieldParseErrors) {
					throw t;
				}
				return null;
			}
		};
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createBaseConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return jsonNode -> null;
			case BOOLEAN:
				return this::convertToBoolean;
			case TINYINT:
				return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
			case SMALLINT:
				return jsonNode -> Short.parseShort(jsonNode.asText().trim());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return this::convertToInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return this::convertToLong;
			case DATE:
				return this::convertToDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToTime;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToTimestamp;
			case FLOAT:
				return this::convertToFloat;
			case DOUBLE:
				return this::convertToDouble;
			case CHAR:
			case VARCHAR:
				return this::convertToString;
			case BINARY:
			case VARBINARY:
				return this::convertToBytes;
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case MAP:
				MapType mapType = (MapType) type;
				return createMapConverter(
					mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
			case MULTISET:
				MultisetType multisetType = (MultisetType) type;
				return createMapConverter
					(multisetType.asSummaryString(), multisetType.getElementType(), new IntType());
			case ROW:
				return createRowConverter((RowType) type);
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(JsonNode jsonNode) {
		if (jsonNode.isBoolean() || (booleanNumberConversion && (jsonNode.isShort() || jsonNode.isInt() || jsonNode.isLong() || jsonNode.isBigInteger()))) {
			// avoid redundant toString and parseBoolean, for better performance
			return jsonNode.asBoolean();
		} else {
			return Boolean.parseBoolean(jsonNode.asText().trim());
		}
	}

	private int convertToInt(JsonNode jsonNode) {
		if (jsonNode.canConvertToInt() || (jsonNode.isBoolean() && booleanNumberConversion)) {
			// avoid redundant toString and parseInt, for better performance
			return jsonNode.asInt();
		} else {
			return Integer.parseInt(jsonNode.asText().trim());
		}
	}

	private long convertToLong(JsonNode jsonNode) {
		if (jsonNode.canConvertToLong() || (jsonNode.isBoolean() && booleanNumberConversion)) {
			// avoid redundant toString and parseLong, for better performance
			return jsonNode.asLong();
		} else {
			return Long.parseLong(jsonNode.asText().trim());
		}
	}

	private double convertToDouble(JsonNode jsonNode) {
		if (jsonNode.isDouble() || (jsonNode.isBoolean() && booleanNumberConversion)) {
			// avoid redundant toString and parseDouble, for better performance
			return jsonNode.asDouble();
		} else {
			return Double.parseDouble(jsonNode.asText().trim());
		}
	}

	private float convertToFloat(JsonNode jsonNode) {
		if (jsonNode.isDouble()) {
			// avoid redundant toString and parseDouble, for better performance
			return (float) jsonNode.asDouble();
		} else {
			return Float.parseFloat(jsonNode.asText().trim());
		}
	}

	private int convertToDate(JsonNode jsonNode) {
		LocalDate date = ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToTime(JsonNode jsonNode) {
		TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(jsonNode.asText());
		LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

		// get number of milliseconds of the day
		return localTime.toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(JsonNode jsonNode) {
		TemporalAccessor parsedTimestamp;
		if (jsonNode.canConvertToLong()) {
			return TimestampData.fromEpochMillis(jsonNode.asLong());
		}
		switch (timestampFormat){
			case SQL:
				parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			case ISO_8601:
				parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			case RFC_3339:
				parsedTimestamp = RFC3339_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			default:
				throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
		}
		LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

		return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
	}

	private StringData convertToString(JsonNode jsonNode) {
		if (jsonNode instanceof ContainerNode) {
			return StringData.fromString(jsonNode.toString());
		} else {
			return StringData.fromString(jsonNode.asText());
		}
	}

	private byte[] convertToBytes(JsonNode jsonNode) {
		if (byteAsJsonNode) {
			return jsonNode.toString().getBytes();
		}
		try {
			return jsonNode.binaryValue();
		} catch (IOException e) {
			throw new JsonParseException("Unable to deserialize byte array.", e);
		}
	}

	private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return jsonNode -> {
			BigDecimal bigDecimal;
			if (jsonNode.isBigDecimal()) {
				bigDecimal = jsonNode.decimalValue();
			} else {
				bigDecimal = new BigDecimal(jsonNode.asText());
			}
			return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
		};
	}

	private DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		DeserializationRuntimeConverter elementConverter = createConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return jsonNode -> {
			final ArrayNode node = (ArrayNode) jsonNode;
			final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
			for (int i = 0; i < node.size(); i++) {
				final JsonNode innerNode = node.get(i);
				array[i] = elementConverter.convert(innerNode);
			}
			return new GenericArrayData(array);
		};
	}

	private DeserializationRuntimeConverter createMapConverter(String typeSummary, LogicalType keyType, LogicalType valueType) {
		if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
			throw new UnsupportedOperationException(
				"JSON format doesn't support non-string as key type of map. " +
					"The type is: " + typeSummary);
		}
		final DeserializationRuntimeConverter keyConverter = createConverter(keyType);
		final DeserializationRuntimeConverter valueConverter = createConverter(valueType);

		return jsonNode -> {
			Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
			Map<Object, Object> result = new HashMap<>();
			while (fields.hasNext()) {
				Map.Entry<String, JsonNode> entry = fields.next();
				Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
				Object value = valueConverter.convert(entry.getValue());
				result.put(key, value);
			}
			return new GenericMapData(result);
		};
	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return jsonNode -> {
			ObjectNode node = (ObjectNode) jsonNode;
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				JsonNode field = node.get(fieldName);
				Object convertedField = convertField(fieldConverters[i], fieldName, field);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private Object convertField(
			DeserializationRuntimeConverter fieldConverter,
			String fieldName,
			JsonNode field) {
		if (field == null && !defaultOnMissingField) {
			if (failOnMissingField) {
				throw new JsonParseException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			return fieldConverter.convert(field);
		}
	}

	/**
	 * Exception which refers to parse errors in converters.
	 * */
	private static final class JsonParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public JsonParseException(String message) {
			super(message);
		}

		public JsonParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
