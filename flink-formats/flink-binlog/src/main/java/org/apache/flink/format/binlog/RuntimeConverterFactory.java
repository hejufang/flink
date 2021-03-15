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

package org.apache.flink.format.binlog;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.binlog.DRCEntry;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descripters.BinlogValidator.AFTER_IMAGE;
import static org.apache.flink.table.descripters.BinlogValidator.AFTER_PREFIX;
import static org.apache.flink.table.descripters.BinlogValidator.BEFORE_IMAGE;
import static org.apache.flink.table.descripters.BinlogValidator.BEFORE_PREFIX;
import static org.apache.flink.table.descripters.BinlogValidator.BIGINT;
import static org.apache.flink.table.descripters.BinlogValidator.BODY;
import static org.apache.flink.table.descripters.BinlogValidator.CHAR;
import static org.apache.flink.table.descripters.BinlogValidator.DATE;
import static org.apache.flink.table.descripters.BinlogValidator.DATETIME;
import static org.apache.flink.table.descripters.BinlogValidator.DOUBLE;
import static org.apache.flink.table.descripters.BinlogValidator.ENTRY;
import static org.apache.flink.table.descripters.BinlogValidator.FLOAT;
import static org.apache.flink.table.descripters.BinlogValidator.HEADER;
import static org.apache.flink.table.descripters.BinlogValidator.INT;
import static org.apache.flink.table.descripters.BinlogValidator.MESSAGE;
import static org.apache.flink.table.descripters.BinlogValidator.NAME_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.NULL_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.PAYLOAD;
import static org.apache.flink.table.descripters.BinlogValidator.REAL;
import static org.apache.flink.table.descripters.BinlogValidator.ROWDATAS;
import static org.apache.flink.table.descripters.BinlogValidator.SMALLINT;
import static org.apache.flink.table.descripters.BinlogValidator.SQL_TYPE_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.TABLE;
import static org.apache.flink.table.descripters.BinlogValidator.TEXT;
import static org.apache.flink.table.descripters.BinlogValidator.TIME;
import static org.apache.flink.table.descripters.BinlogValidator.TIMESTAMP;
import static org.apache.flink.table.descripters.BinlogValidator.TINYINT;
import static org.apache.flink.table.descripters.BinlogValidator.VALUE_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.VARCHAR;

/**
 * Runtime convert factory.
 */
public class RuntimeConverterFactory {
	static final Map<String, Descriptors.FieldDescriptor> FIELD_DESCRIPTORS = initFieldDescriptors();
	static final Map<String, Descriptors.Descriptor> DESCRIPTORS = initDescriptors();
	private static final Map<String, TypeInformation> JDBC_TO_FLINK_TYPE = getJdbcToFlinkType();
	private static final String ZERO_TIMESTAMP_STR = "0000-00-00 00:00:00";

	/**
	 * Create runtime converter according to RowTypeInfo.
	 */
	public static Map<String, RuntimeConverter> createConverter(RowTypeInfo rowTypeInfo) {
		Map<String, RuntimeConverter> converterMap = new HashMap<>();
		String[] columnNames = rowTypeInfo.getFieldNames();

		// First two field must be binlog_header & binlog_header, we ignore them. So we start from '2'.
		for (int i = 2; i < rowTypeInfo.getArity(); i++) {
			String columnName = columnNames[i];
			TypeInformation typeInformation = rowTypeInfo.getTypeAt(i);
			String fieldName = rowTypeInfo.getFieldNames()[i];
			if (!(typeInformation instanceof RowTypeInfo)) {
				// Ignore the time column, which is the only one without RowTypeInfo.
				continue;
			}
			List<RuntimeConverter> innerConverterList = new ArrayList<>();
			RowTypeInfo innerRowTypeInfo = (RowTypeInfo) typeInformation;
			String[] innerColumnNames = innerRowTypeInfo.getFieldNames();
			for (int j = 0; j < innerRowTypeInfo.getArity(); j++) {
				String innerColumnName = innerColumnNames[j];
				TypeInformation innerColumnTypeInfo = innerRowTypeInfo.getTypeAt(j);
				String realColumnName = removeColumnNamePrefix(innerColumnName);
				RuntimeConverter valueConverter =
					createValueConverter(innerColumnTypeInfo, realColumnName);
				innerConverterList.add(addConverterWrapper(valueConverter, innerColumnTypeInfo,
					realColumnName, fieldName));
			}

			converterMap.put(columnName, assembleRowConverter(columnName, innerRowTypeInfo, innerConverterList));
		}
		return converterMap;
	}

	/**
	 * Create converter for 'value' columns.
	 */
	private static RuntimeConverter createValueConverter(TypeInformation typeInfo, String realFiledName) {
		if (!VALUE_COLUMN.equals(realFiledName)) {
			// return the origin value for columns but 'value' column.
			return null;
		}
		if (typeInfo == Types.STRING) {
			return o -> o;
		} else if (typeInfo == Types.BYTE) {
			return o -> Byte.valueOf((String) o);
		} else if (typeInfo == Types.SHORT) {
			return o -> Short.valueOf((String) o);
		} else if (typeInfo == Types.INT) {
			return o -> Integer.valueOf((String) o);
		} else if (typeInfo == Types.LONG) {
			return o -> Long.valueOf((String) o);
		} else if (typeInfo == Types.FLOAT) {
			return o -> Float.valueOf((String) o);
		} else if (typeInfo == Types.DOUBLE) {
			return o -> Double.valueOf((String) o);
		} else if (typeInfo == Types.SQL_DATE) {
			return o -> Date.valueOf((String) o);
		} else if (typeInfo == Types.SQL_TIME) {
			return o -> Time.valueOf((String) o);
		} else if (typeInfo == Types.SQL_TIMESTAMP) {
			// Timestamp.valueOf will throw an exception if the value of o is
			// start with ZERO_TIMESTAMP_STR, so we made a judgment to handle this.
			return o -> ((String) o).startsWith(ZERO_TIMESTAMP_STR) ? new Timestamp(0) : Timestamp.valueOf((String) o);
		} else {
			throw new IllegalArgumentException(
				String.format("Unsupported type for 'value' column: %s.", typeInfo));
		}
	}

	private static RuntimeConverter assembleRowConverter(
			String columnName,
			RowTypeInfo rowTypeInfo,
			List<RuntimeConverter> fieldConverters) {
		return (msg) -> {
			DynamicMessage rowData = (DynamicMessage) msg;
			String[] innerColumns = rowTypeInfo.getFieldNames();
			List<DynamicMessage> beforeImageList = (List) rowData.getField(FIELD_DESCRIPTORS.get(BEFORE_IMAGE));
			List<DynamicMessage> afterImageList = (List) rowData.getField(FIELD_DESCRIPTORS.get(AFTER_IMAGE));

			Map<String, DynamicMessage> beforeImageMap =
				parseAllColumns(beforeImageList, FIELD_DESCRIPTORS.get(NAME_COLUMN));
			Map<String, DynamicMessage> afterImageMap =
				parseAllColumns(afterImageList, FIELD_DESCRIPTORS.get(NAME_COLUMN));
			Row row = new Row(rowTypeInfo.getArity());
			for (int i = 0; i < rowTypeInfo.getArity(); i++) {
				String innerColumn = innerColumns[i];
				DynamicMessage columnMessage;
				if (innerColumn.startsWith(BEFORE_PREFIX)) {
					columnMessage = beforeImageMap.get(columnName);
				} else {
					columnMessage = afterImageMap.get(columnName);
				}
				if (columnMessage == null) {
					row.setField(i, null);
				} else {
					row.setField(i, fieldConverters.get(i).convert(columnMessage));
				}
			}
			return row;
		};
	}

	/**
	 * We do the common work here when converter data, including:
	 * 1. check mismatch for jdbc type & flink type.
	 * 2. handle null data.
	 */
	private static RuntimeConverter addConverterWrapper(
			RuntimeConverter converter,
			TypeInformation typeInfo,
			String innerColumnName,
			String fieldName) {
		return o -> {
			DynamicMessage columnMessage = (DynamicMessage) o;

			if (VALUE_COLUMN.equals(innerColumnName)) {
				// validate type for column 'value'.
				String jdbcTypeName = (String) columnMessage.getField(FIELD_DESCRIPTORS.get(SQL_TYPE_COLUMN));
				validateType(jdbcTypeName, typeInfo);
				boolean isNull = (boolean) columnMessage.getField(FIELD_DESCRIPTORS.get(NULL_COLUMN));
				if (isNull) {
					// This indicates the column value is null.
					return null;
				}
				String valueInString = (String) columnMessage.getField(FIELD_DESCRIPTORS.get(VALUE_COLUMN));
				try {
					return converter.convert(valueInString);
				} catch (Throwable t) {
					throw new FlinkRuntimeException(String.format("Failed to convert value '%s' to " +
						"type '%s' for column '%s'.", valueInString, typeInfo, fieldName), t);
				}
			}

			return columnMessage.getField(DRCEntry.Column.getDescriptor().findFieldByName(innerColumnName));
		};
	}

	private static Map<String, TypeInformation> getJdbcToFlinkType() {
		Map<String, TypeInformation> map = new HashMap<>();
		map.put(CHAR, Types.STRING);
		map.put(VARCHAR, Types.STRING);
		map.put(TEXT, Types.STRING);
		map.put(TINYINT, Types.BYTE);
		map.put(SMALLINT, Types.SHORT);
		map.put(INT, Types.INT);
		map.put(BIGINT, Types.LONG);
		map.put(REAL, Types.FLOAT);
		map.put(FLOAT, Types.FLOAT);
		map.put(DOUBLE, Types.DOUBLE);
		map.put(DATE, Types.SQL_DATE);
		map.put(TIME, Types.SQL_TIME);
		map.put(TIMESTAMP, Types.SQL_TIMESTAMP);
		map.put(DATETIME, Types.SQL_TIMESTAMP);
		return map;
	}

	private static void validateType(String jdbcTypeName, TypeInformation typeInfo) {
		TypeInformation expectedTypeInfo = JDBC_TO_FLINK_TYPE.get(jdbcTypeName);
		if (expectedTypeInfo == null) {
			throw new IllegalArgumentException(String.format("Unsupported jdbc type: %s.", jdbcTypeName));
		}
		if (!expectedTypeInfo.equals(typeInfo)) {
			throw new IllegalArgumentException(
				String.format("Expect flink type for jdbc type '%s' is %s, but we get '%s' parsed " +
					"from DDL.", jdbcTypeName, expectedTypeInfo, typeInfo));
		}
	}

	private static String removeColumnNamePrefix(String columnName) {
		if (columnName == null) {
			return null;
		}
		if (columnName.startsWith(BEFORE_PREFIX)) {
			return columnName.substring(BEFORE_PREFIX.length());
		} else if (columnName.startsWith(AFTER_PREFIX)) {
			return columnName.substring(AFTER_PREFIX.length());
		}
		return columnName;
	}

	private static Map<String, Descriptors.FieldDescriptor> initFieldDescriptors() {
		Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap = new HashMap<>();

		fieldDescriptorMap.put(HEADER, DRCEntry.Entry.getDescriptor().findFieldByName(HEADER));
		fieldDescriptorMap.put(BODY, DRCEntry.Entry.getDescriptor().findFieldByName(BODY));
		fieldDescriptorMap.put(BEFORE_IMAGE, DRCEntry.RowData.getDescriptor().findFieldByName(BEFORE_IMAGE));
		fieldDescriptorMap.put(AFTER_IMAGE, DRCEntry.RowData.getDescriptor().findFieldByName(AFTER_IMAGE));
		fieldDescriptorMap.put(SQL_TYPE_COLUMN, DRCEntry.Column.getDescriptor().findFieldByName(SQL_TYPE_COLUMN));
		fieldDescriptorMap.put(NAME_COLUMN, DRCEntry.Column.getDescriptor().findFieldByName(NAME_COLUMN));
		fieldDescriptorMap.put(NULL_COLUMN, DRCEntry.Column.getDescriptor().findFieldByName(NULL_COLUMN));
		fieldDescriptorMap.put(VALUE_COLUMN, DRCEntry.Column.getDescriptor().findFieldByName(VALUE_COLUMN));
		fieldDescriptorMap.put(PAYLOAD, DRCEntry.Message.getDescriptor().findFieldByName(PAYLOAD));
		fieldDescriptorMap.put(TABLE, DRCEntry.EntryHeader.getDescriptor().findFieldByName(TABLE));
		fieldDescriptorMap.put(ROWDATAS, DRCEntry.EntryBody.getDescriptor().findFieldByName(ROWDATAS));

		return fieldDescriptorMap;
	}

	private static Map<String, Descriptors.Descriptor> initDescriptors() {
		Map<String, Descriptors.Descriptor> descriptorMap = new HashMap<>();

		descriptorMap.put(MESSAGE, DRCEntry.Message.getDescriptor());
		descriptorMap.put(ENTRY, DRCEntry.Entry.getDescriptor());
		descriptorMap.put(HEADER, DRCEntry.EntryHeader.getDescriptor());
		descriptorMap.put(BODY, DRCEntry.EntryBody.getDescriptor());

		return descriptorMap;

	}

	/**
	 * Transform column list to name-column map.
	 */
	private static Map<String, DynamicMessage> parseAllColumns(
		List<DynamicMessage> msgList,
		Descriptors.FieldDescriptor nameFieldDescriptor) {
		if (msgList == null || msgList.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<String, DynamicMessage> map = new HashMap<>();
		for (DynamicMessage msg : msgList) {
			map.put((String) msg.getField(nameFieldDescriptor), msg);
		}
		return map;
	}

	interface RuntimeConverter extends Serializable {
		Object convert(Object o);
	}
}
