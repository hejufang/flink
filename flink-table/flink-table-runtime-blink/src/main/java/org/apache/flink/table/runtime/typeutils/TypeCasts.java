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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.util.NumberUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Utils for performing casts.
 */
public class TypeCasts {
	/**
	 * See rules in {@link LogicalTypeCasts#supportsImplicitCast}.
	 */
	public static Object implicitCast(
			Object source,
			LogicalType sourceType,
			LogicalType targetType) {
		if (source == null) {
			//case 0: null value need no cast.
			return null;
		} else if (sourceType.equals(targetType)) {
			//case 1: identity
			return source;
		} else if (isBasicType(targetType) && LogicalTypeUtils.toInternalConversionClass(sourceType)
				.equals(LogicalTypeUtils.toInternalConversionClass(targetType))) {
			//case 2: when the source and target type have same internal representation.
			return source;
		} else if (TypeCheckUtils.isNumeric(targetType)) {
			//case 3: perform cast for numeric types
			Object originalValue;
			if (sourceType.getClass().equals(DecimalType.class)) {
				originalValue = ((DecimalData) source).toBigDecimal();
			} else {
				originalValue = source;
			}
			Object convertedValue =  NumberUtils.convertNumberToTargetClass((Number) originalValue,
				(Class<? extends Number>) targetType.getDefaultConversion());
			if (targetType.getClass().equals(DecimalType.class)) {
				BigDecimal bigDecimal = (BigDecimal) convertedValue;
				int precision = ((DecimalType) targetType).getPrecision();
				int scale = ((DecimalType) targetType).getScale();
				bigDecimal = bigDecimal.setScale(scale);
				convertedValue = DecimalData.fromUnscaledBytes(bigDecimal.unscaledValue().toByteArray(),
					precision, scale);
			}
			return convertedValue;
		} else {
			switch (targetType.getTypeRoot()) {
				case ARRAY:
					return implicitCastArrayData((ArrayData) source, sourceType, targetType);
				case MAP:
				case MULTISET:
					return implicitCastMapData((MapData) source, sourceType, targetType);
				case ROW:
					return implicitCastRowData((RowData) source, sourceType, targetType);
				default:
					throw new UnsupportedOperationException(String.format("Implicit Conversion between %s and %s is" +
						"not supported yet.", sourceType.asSummaryString(), targetType.asSummaryString()));

			}
		}
	}

	private static ArrayData implicitCastArrayData(ArrayData source, LogicalType sourceType, LogicalType targetType) {
		LogicalType sourceElementType = sourceType.getChildren().get(0);
		LogicalType targetElementType = targetType.getChildren().get(0);
		ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(sourceElementType);
		final Object[] values = new Object[source.size()];
		for (int pos = 0; pos < source.size(); pos++) {
			final Object value = elementGetter.getElementOrNull(source, pos);
			values[pos] = implicitCast(value, sourceElementType, targetElementType);
		}
		return new GenericArrayData(values);
	}

	private static MapData implicitCastMapData(MapData source, LogicalType sourceType, LogicalType targetType) {
		final ArrayData keyArray = source.keyArray();
		LogicalType sourceKeyType = sourceType.getChildren().get(0);
		LogicalType targetKeyType = targetType.getChildren().get(0);
		ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(sourceKeyType);
		final ArrayData valueArray = source.valueArray();
		LogicalType sourceValueType = sourceType.getChildren().get(1);
		LogicalType targetValueType = targetType.getChildren().get(1);
		ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(sourceValueType);
		final int length = source.size();
		final Map<Object, Object> map = new HashMap<>(source.size());
		for (int pos = 0; pos < length; pos++) {
			final Object keyValue = keyElementGetter.getElementOrNull(keyArray, pos);
			final Object valueValue = valueElementGetter.getElementOrNull(valueArray, pos);
			if (keyValue == null || valueValue == null) {
				throw new RuntimeException(String.format("Both key and value of the map should not be null. " +
					"The key is %s, the value is %s.", keyValue, valueValue));
			}
			map.put(
				implicitCast(keyValue, sourceKeyType, targetKeyType),
				implicitCast(valueValue, sourceValueType, targetValueType));
		}
		return new GenericMapData(map);
	}

	private static RowData implicitCastRowData(RowData source, LogicalType sourceType, LogicalType targetType) {
		List<LogicalType> sourceChildrenType = sourceType.getChildren();
		List<LogicalType> targetChildrenType = targetType.getChildren();
		RowData.FieldGetter[] fieldGetters = IntStream
			.range(0, sourceChildrenType.size())
			.mapToObj(pos -> RowData.createFieldGetter(sourceChildrenType.get(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
		GenericRowData rowData = new GenericRowData(source.getArity());
		for (int pos = 0; pos < source.getArity(); pos++) {
			final Object value = fieldGetters[pos].getFieldOrNull(source);
			rowData.setField(pos, implicitCast(value, sourceChildrenType.get(pos), targetChildrenType.get(pos)));
		}
		return rowData;
	}

	private static boolean isBasicType(LogicalType logicalType) {
		LogicalTypeRoot root = logicalType.getTypeRoot();
		switch (root) {
			case BOOLEAN:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
				return true;
			default:
				return false;
		}
	}
}
