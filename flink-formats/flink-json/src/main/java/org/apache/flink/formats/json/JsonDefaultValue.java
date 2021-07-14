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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Time;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Optional;

/**
 * Default value schema that gives a default value to a json field.
 */
public class JsonDefaultValue {

	public static DeserializationRuntimeConverter createConverter(LogicalType typeInfo) {
		DeserializationRuntimeConverter baseConverter = createConverterFromTypeInfo(typeInfo)
			.orElseGet(() -> null);
		return baseConverter;
	}

	private static Optional<DeserializationRuntimeConverter> createConverterFromTypeInfo(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return Optional.of(() -> null);
			case BOOLEAN:
				return Optional.of(() -> false);
			case TINYINT:
				return Optional.of(() -> (byte) 0);
			case SMALLINT:
				return Optional.of(() -> (short) 0);
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return Optional.of(() -> 0);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Optional.of(() -> 0L);
			case FLOAT:
				return Optional.of(() -> 0.0f);
			case DOUBLE:
				return Optional.of(() -> 0.0d);
			case DATE:
				return Optional.of(createDateConverter());
			case TIME_WITHOUT_TIME_ZONE:
				return Optional.of(createTimeConverter());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return Optional.of(createTimestampConverter());
			case CHAR:
			case VARCHAR:
				return Optional.of(() -> StringData.fromString(""));
			case BINARY:
			case VARBINARY:
				return Optional.of(createByteArrayConverter());
			case DECIMAL:
				return Optional.of(createDecimalConverter());
			case ARRAY:
				return Optional.of(createArrayConverter((ArrayType) type));
			case MAP:
			case MULTISET:
				return Optional.of(createMapConverter());
			case ROW:
				return Optional.of(createRowConverter((RowType) type));
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type.getTypeRoot());
		}
	}

	private static DeserializationRuntimeConverter createMapConverter() {
		return () -> new GenericMapData(new HashMap<>());
	}

	private static DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(JsonDefaultValue::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
		return assembleRowConverter(fieldNames, fieldConverters);
	}

	private static DeserializationRuntimeConverter assembleRowConverter(
			String[] fieldNames,
			DeserializationRuntimeConverter[] fieldConverters) {
		int arity = fieldNames.length;
		return () -> {
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				Object convertField = fieldConverters[i].convert();
				row.setField(i, convertField);
			}
			return row;
		};
	}

	private static DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		DeserializationRuntimeConverter elementConverter = createConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return () -> {
			final Object[] array = (Object[]) Array.newInstance(elementClass, 1);
			array[0] = elementConverter.convert();
			return new GenericArrayData(array);
		};
	}

	private static DeserializationRuntimeConverter createByteArrayConverter() {
		return () -> new byte[0];
	}

	private static DeserializationRuntimeConverter createDecimalConverter() {
		return () -> DecimalData.fromBigDecimal(new BigDecimal(0), 0, 0);
	}

	private static DeserializationRuntimeConverter createDateConverter() {
		return () -> (int) LocalDate.now().toEpochDay();
	}

	private static DeserializationRuntimeConverter createTimestampConverter() {
		return () -> TimestampData.fromEpochMillis(System.currentTimeMillis());
	}

	private static DeserializationRuntimeConverter createTimeConverter() {
		return () -> new Time(System.currentTimeMillis()).toLocalTime().toSecondOfDay() * 1000;
	}

	/**
	 * Runtime converter to default value.
	 */
	@FunctionalInterface
	public interface DeserializationRuntimeConverter extends Serializable {
		Object convert();
	}

}

