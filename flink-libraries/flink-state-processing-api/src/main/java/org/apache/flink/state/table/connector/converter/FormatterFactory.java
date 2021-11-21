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

package org.apache.flink.state.table.connector.converter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MAP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;

/**
 * To make the results of the query more readable. This class Used for some special
 * types of format conversion in queries. Such as {@link BinaryRowData}, {@link RowDataSerializer}
 */

public class FormatterFactory {

	public static final String NULL_STRING = "null";

	/**
	 * StateIteratorFactory.
	 */
	public static Formatter getFormatter(TypeSerializer typeSerializer){
		if (typeSerializer instanceof RowDataSerializer){
			return new RowDataFormatter((RowDataSerializer) typeSerializer);
		} else if (typeSerializer instanceof MapSerializer) {
			return MapFormatter.build(typeSerializer);
		} else {
			return StringFormatter.build();
		}
	}

	/**
	 * Formatter used to format row.
	 */
	public interface Formatter {
		String format(Object o);
	}

	/**
	 * StringFormatter.
	 */
	public static class StringFormatter implements Formatter {

		private static final StringFormatter INSTANCE = new StringFormatter();

		public static StringFormatter build(){
			return INSTANCE;
		}

		@Override
		public String format(Object o) {

			if (o == null){
				return NULL_STRING;
			}
			return o.toString();
		}
	}

	/**
	 * use to format RowData.
	 */
	public static class RowDataFormatter implements Formatter {

		private LogicalType[] types;

		public RowDataFormatter(RowDataSerializer rowDataSerializer) {
			this.types = rowDataSerializer.getTypes();
		}

		@Override
		public String format(Object o) {
			if (o == null){
				return NULL_STRING;
			}
			return getRowString(types, (RowData) o);
		}

		public String getRowString(LogicalType[] types, RowData rowData) {
			StringBuilder sb = new StringBuilder();
			sb.append("Row(");
			for (int i = 0; i < types.length; i++) {
				if (i != 0) {
					sb.append(",");
				}
				LogicalType type = types[i];
				Object objectOrNull = RowData.createFieldGetter(type, i).getFieldOrNull(rowData);
				sb.append(getFieldString(type, objectOrNull));
			}
			sb.append(")");
			return sb.toString();
		}

		public String getArrayString(LogicalType type, ArrayData arrayData) {
			ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(type);
			StringBuilder sb = new StringBuilder();
			sb.append("Array(");
			for (int i = 0; i < arrayData.size(); i++) {
				if (i != 0) {
					sb.append(",");
				}
				sb.append(getFieldString(type, elementGetter.getElementOrNull(arrayData, i)));
			}
			sb.append(")");
			return sb.toString();
		}

		public String getMapString(MapType type, MapData mapData){
			LogicalType keyType = type.getKeyType();
			LogicalType valueType = type.getValueType();

			ArrayData keyArray = mapData.keyArray();
			ArrayData valueArray = mapData.valueArray();

			ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
			ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);

			StringBuilder sb = new StringBuilder();
			sb.append("Map(");
			for (int i = 0; i < keyArray.size(); i++) {
				if (i != 0) {
					sb.append(",");
				}
				sb.append(getFieldString(keyType, keyGetter.getElementOrNull(keyArray, i)))
					.append("=")
					.append(getFieldString(valueType, valueGetter.getElementOrNull(valueArray, i)));
			}
			sb.append(")");
			return sb.toString();
		}

		public String getFieldString(LogicalType type, Object fieldOrNull) {

			if (fieldOrNull == null){
				return NULL_STRING;
			}

			if (type.getTypeRoot().equals(ROW)) {
				List<LogicalType> logicalTypeList = ((RowType) type).getChildren();
				LogicalType[] childrenTypes = logicalTypeList.toArray(new LogicalType[logicalTypeList.size()]);
				return getRowString(childrenTypes, (RowData) fieldOrNull);
			}

			if (type.getTypeRoot().equals(MAP)) {
				return getMapString((MapType) type, (MapData) fieldOrNull);
			}

			if (type.getTypeRoot().equals(ARRAY)) {
				return getArrayString(type, (ArrayData) fieldOrNull);
			}
			return fieldOrNull.toString();
		}
	}

	/**
	 * MapFormatter use to Format MapState.
	 */
	public static class MapFormatter implements Formatter {

		private Formatter keyFormatter;
		private Formatter valueFormatter;

		MapFormatter(Formatter keyFormatter, Formatter valueFormatter){
			this.keyFormatter = keyFormatter;
			this.valueFormatter = valueFormatter;
		}

		@Override
		public String format(Object o) {
			if (o == null){
				return NULL_STRING;
			}

			Map.Entry entry = (Map.Entry) o;
			StringBuilder sb = new StringBuilder();
			sb.append(keyFormatter.format(entry.getKey()))
				.append("=")
				.append(valueFormatter.format(entry.getValue()));
			return sb.toString();
		}

		public static MapFormatter build(TypeSerializer keySerializer, TypeSerializer valueSerializer){
			return new MapFormatter(getFormatter(keySerializer), getFormatter(valueSerializer));
		}

		public static MapFormatter build(TypeSerializer typeSerializer) {
			if (typeSerializer instanceof MapSerializer) {
				MapSerializer mapSerializer = (MapSerializer) typeSerializer;
				return MapFormatter.build(mapSerializer.getKeySerializer(), mapSerializer.getValueSerializer());
			} else {
				throw new RuntimeException("UnSupported mapSerializer Type " + typeSerializer.getClass());
			}
		}
	}

	/**
	 * use to format DataType.
	 */
	public static class TypeFormatter implements Formatter {

		public static final TypeFormatter INSTANCE = new TypeFormatter();

		@Override
		public String format(Object o) {
			if (o instanceof RowDataSerializer){
				return  RowType.of(((RowDataSerializer) o).getTypes()).asSummaryString();
			} else {
				return ((TypeSerializer) o).createInstance().getClass().getSimpleName();
			}
		}
	}
}
