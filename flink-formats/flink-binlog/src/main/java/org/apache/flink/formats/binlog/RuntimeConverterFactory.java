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

package org.apache.flink.formats.binlog;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.binlog.DRCEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.binlog.BinlogOptions.AFTER_IMAGE;
import static org.apache.flink.formats.binlog.BinlogOptions.AFTER_PREFIX;
import static org.apache.flink.formats.binlog.BinlogOptions.BEFORE_IMAGE;
import static org.apache.flink.formats.binlog.BinlogOptions.BEFORE_PREFIX;
import static org.apache.flink.formats.binlog.BinlogOptions.BIGINT;
import static org.apache.flink.formats.binlog.BinlogOptions.BINARY;
import static org.apache.flink.formats.binlog.BinlogOptions.BLOB;
import static org.apache.flink.formats.binlog.BinlogOptions.BODY;
import static org.apache.flink.formats.binlog.BinlogOptions.CHAR;
import static org.apache.flink.formats.binlog.BinlogOptions.DATE;
import static org.apache.flink.formats.binlog.BinlogOptions.DATETIME;
import static org.apache.flink.formats.binlog.BinlogOptions.DECIMAL;
import static org.apache.flink.formats.binlog.BinlogOptions.DOUBLE;
import static org.apache.flink.formats.binlog.BinlogOptions.ENTRY;
import static org.apache.flink.formats.binlog.BinlogOptions.FLOAT;
import static org.apache.flink.formats.binlog.BinlogOptions.HEADER;
import static org.apache.flink.formats.binlog.BinlogOptions.INT;
import static org.apache.flink.formats.binlog.BinlogOptions.LONGBLOB;
import static org.apache.flink.formats.binlog.BinlogOptions.LONGTEXT;
import static org.apache.flink.formats.binlog.BinlogOptions.MEDIUMBLOB;
import static org.apache.flink.formats.binlog.BinlogOptions.MEDIUMTEXT;
import static org.apache.flink.formats.binlog.BinlogOptions.MESSAGE;
import static org.apache.flink.formats.binlog.BinlogOptions.NAME_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.NULL_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.PAYLOAD;
import static org.apache.flink.formats.binlog.BinlogOptions.REAL;
import static org.apache.flink.formats.binlog.BinlogOptions.ROWDATAS;
import static org.apache.flink.formats.binlog.BinlogOptions.SMALLINT;
import static org.apache.flink.formats.binlog.BinlogOptions.SQL_TYPE_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.TABLE;
import static org.apache.flink.formats.binlog.BinlogOptions.TEXT;
import static org.apache.flink.formats.binlog.BinlogOptions.TIME;
import static org.apache.flink.formats.binlog.BinlogOptions.TIMESTAMP;
import static org.apache.flink.formats.binlog.BinlogOptions.TINYBLOB;
import static org.apache.flink.formats.binlog.BinlogOptions.TINYINT;
import static org.apache.flink.formats.binlog.BinlogOptions.VALUE_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.VARBINARY;
import static org.apache.flink.formats.binlog.BinlogOptions.VARCHAR;

/**
 * Runtime convert factory.
 */
public class RuntimeConverterFactory {
	public static final Map<String, Descriptors.FieldDescriptor> FIELD_DESCRIPTORS = initFieldDescriptors();
	public static final Map<String, Descriptors.Descriptor> DESCRIPTORS = initDescriptors();
	private static final Map<String, Class<? extends LogicalType>> JDBC_TO_FLINK_TYPE = getJdbcToFlinkType();
	private static final Map<String, Class<? extends LogicalType>> JDBC_TO_COMPATIBLE_TYPE = getJdbcToCompatibleType();
	private static final String ZERO_TIMESTAMP_STR = "0000-00-00 00:00:00";
	private static final String ZERO_DATE_STR = "0000-00-00";

	/**
	 * Create runtime converter according to RowTypeInfo.
	 */
	public static Map<String, RuntimeConverter> createConverter(RowType rowType, String headerName, String bodyName) {
		Map<String, RuntimeConverter> converterMap = new HashMap<>();
		List<String> columnNames = rowType.getFieldNames();

		// Binlog_header & binlog_body is optional for binlog type.
		for (int i = 0; i < rowType.getFieldCount(); i++) {
			String columnName = columnNames.get(i);
			LogicalType logicalType = rowType.getTypeAt(i);
			String fieldName = columnNames.get(i);
			if (!(logicalType instanceof RowType) || columnName.equals(headerName) || columnName.equals(bodyName)) {
				continue;
			}

			RowType innerRowType = (RowType) logicalType;
			List<String> innerColumnNames = innerRowType.getFieldNames();
			List<RuntimeConverter> innerConverterList = new ArrayList<>();
			for (int j = 0; j < innerRowType.getFieldCount(); j++) {
				String innerColumnName = innerColumnNames.get(j);
				LogicalType innerLogicalType = innerRowType.getTypeAt(j);
				String realColumnName = removeColumnNamePrefix(innerColumnName);
				RuntimeConverter valueConverter =
					createValueConverter(innerLogicalType, realColumnName);
				innerConverterList.add(addConverterWrapper(valueConverter, innerLogicalType,
					realColumnName, fieldName));
			}

			converterMap.put(columnName, assembleRowConverter(columnName, innerRowType, innerConverterList));
		}
		return converterMap;
	}

	private static RuntimeConverter assembleRowConverter(
			String columnName,
			RowType rowType,
			List<RuntimeConverter> fieldConverters) {
		return (msg) -> {
			DynamicMessage rowData = (DynamicMessage) msg;
			List<String> innerColumns = rowType.getFieldNames();
			List<DynamicMessage> beforeImageList = (List) rowData.getField(FIELD_DESCRIPTORS.get(BEFORE_IMAGE));
			List<DynamicMessage> afterImageList = (List) rowData.getField(FIELD_DESCRIPTORS.get(AFTER_IMAGE));

			Map<String, DynamicMessage> beforeImageMap =
				parseAllColumns(beforeImageList, FIELD_DESCRIPTORS.get(NAME_COLUMN));
			Map<String, DynamicMessage> afterImageMap =
				parseAllColumns(afterImageList, FIELD_DESCRIPTORS.get(NAME_COLUMN));
			GenericRowData row = new GenericRowData(rowType.getFieldCount());
			for (int i = 0; i < rowType.getFieldCount(); i++) {
				String innerColumn = innerColumns.get(i);
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
			LogicalType logicalType,
			String innerColumnName,
			String fieldName) {
		return o -> {
			DynamicMessage columnMessage = (DynamicMessage) o;

			if (VALUE_COLUMN.equals(innerColumnName)) {
				// for binary type, we don't need validate the type
				if (!(logicalType instanceof VarBinaryType)) {
					// validate type for column 'value'.
					String jdbcTypeName = (String) columnMessage.getField(FIELD_DESCRIPTORS.get(SQL_TYPE_COLUMN));
					validateType(jdbcTypeName, logicalType, fieldName, innerColumnName);
				}
				boolean isNull = (boolean) columnMessage.getField(FIELD_DESCRIPTORS.get(NULL_COLUMN));
				if (isNull) {
					// This indicates the column value is null.
					return null;
				}
				ByteString valueInBytes = (ByteString) columnMessage.getField(FIELD_DESCRIPTORS.get(VALUE_COLUMN));
				try {
					return converter.convert(valueInBytes);
				} catch (Throwable t) {
					throw new FlinkRuntimeException(
						String.format("Failed to convert value '%s' to type '%s' for column '%s'.",
							getValidString(valueInBytes.toByteArray()), logicalType, fieldName), t);
				}
			}

			return columnMessage.getField(DRCEntry.Column.getDescriptor().findFieldByName(innerColumnName));
		};
	}

	private static String getValidString(byte[] bytes) {
		String value = new String(bytes);
		if (Arrays.equals(value.getBytes(), bytes)) {
			return value;
		} else {
			return Base64.getEncoder().encodeToString(bytes);
		}
	}

	private static void validateType(String jdbcTypeName, LogicalType logicalType, String fieldName, String innerName) {
		Class<? extends LogicalType> expectedTypeInfo = JDBC_TO_FLINK_TYPE.get(jdbcTypeName);
		if (expectedTypeInfo == null) {
			throw new IllegalArgumentException(String.format("Unsupported jdbc type: %s.", jdbcTypeName));
		}
		if (!expectedTypeInfo.isInstance(logicalType) && !isCompatibleType(jdbcTypeName, logicalType)) {
			throw new IllegalArgumentException(
				String.format("Expect flink type for jdbc type '%s' is %s, but we get '%s' parsed from DDL, " +
					"field name: %s, inner column name %s.", jdbcTypeName, expectedTypeInfo, logicalType, fieldName, innerName));
		}
	}

	private static boolean isCompatibleType(String jdbcTypeName, LogicalType logicalType) {
		Class<? extends LogicalType> expectedTypeInfo = JDBC_TO_COMPATIBLE_TYPE.get(jdbcTypeName);
		return expectedTypeInfo != null && expectedTypeInfo.isInstance(logicalType);
	}

	private static Map<String, Class<? extends LogicalType>> getJdbcToCompatibleType() {
		Map<String, Class<? extends LogicalType>> map = new HashMap<>();

		map.put(TINYINT, SmallIntType.class);
		map.put(SMALLINT, IntType.class);
		map.put(INT, BigIntType.class);
		map.put(BIGINT, DecimalType.class);

		return map;
	}

	private static Map<String, Class<? extends LogicalType>> getJdbcToFlinkType() {
		Map<String, Class<? extends LogicalType>> map = new HashMap<>();
		map.put(CHAR, CharType.class);
		map.put(VARCHAR, VarCharType.class);
		map.put(TEXT, VarCharType.class);
		map.put(TINYINT, TinyIntType.class);
		map.put(SMALLINT, SmallIntType.class);
		map.put(INT, IntType.class);
		map.put(BIGINT, BigIntType.class);
		map.put(REAL, FloatType.class);
		map.put(FLOAT, FloatType.class);
		map.put(DOUBLE, DoubleType.class);
		map.put(DATE, DateType.class);
		map.put(TIME, TimeType.class);
		map.put(TIMESTAMP, TimestampType.class);
		map.put(DATETIME, TimestampType.class);
		map.put(LONGTEXT, VarCharType.class);
		map.put(MEDIUMTEXT, VarCharType.class);
		map.put(DECIMAL, DecimalType.class);
		map.put(BINARY, VarBinaryType.class);
		map.put(VARBINARY, VarBinaryType.class);
		map.put(TINYBLOB, VarBinaryType.class);
		map.put(MEDIUMBLOB, VarBinaryType.class);
		map.put(BLOB, VarBinaryType.class);
		map.put(LONGBLOB, VarBinaryType.class);
		return map;
	}

	/**
	 * Create converter for 'value' columns.
	 */
	private static RuntimeConverter createValueConverter(LogicalType logicalType, String realFiledName) {
		if (!VALUE_COLUMN.equals(realFiledName)) {
			// return the origin value for columns but 'value' column.
			return null;
		}
		if (logicalType instanceof VarCharType) {
			return o -> StringData.fromString(((ByteString) o).toStringUtf8());
		} if (logicalType instanceof CharType) {
			return o -> StringData.fromString(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof TinyIntType) {
			return o -> Byte.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof SmallIntType) {
			return o -> Short.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof IntType) {
			return o -> Integer.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof BigIntType) {
			return o -> Long.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof FloatType) {
			return o -> Float.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof DoubleType) {
			return o -> Double.valueOf(((ByteString) o).toStringUtf8());
		} else if (logicalType instanceof DateType) {
			return o -> {
				String v = ((ByteString) o).toStringUtf8();
				return SqlDateTimeUtils.dateToInternal(
					v.contains(ZERO_DATE_STR) ? new Date(0) : Date.valueOf(v));
			};
		} else if (logicalType instanceof TimeType) {
			return o -> SqlDateTimeUtils.timeToInternal(Time.valueOf(((ByteString) o).toStringUtf8()));
		} else if (logicalType instanceof VarBinaryType) {
			return o -> ((ByteString) o).toByteArray();
		} else if (logicalType instanceof TimestampType) {
			// Timestamp.valueOf will throw an exception if the value of o is
			// equal to ZERO_TIMESTAMP_STR, so we made a judgment to handle this.
			return o -> {
				String v = ((ByteString) o).toStringUtf8();
				return TimestampData.fromTimestamp(
					v.contains(ZERO_TIMESTAMP_STR) ? new Timestamp(0) : Timestamp.valueOf(v));
			};
		} else if (logicalType instanceof DecimalType) {
			return o -> {
				DecimalType decimalType = (DecimalType) logicalType;
				return DecimalData.fromBigDecimal(new BigDecimal(((ByteString) o).toStringUtf8()),
					decimalType.getPrecision(), decimalType.getScale());
			};
		} else {
			throw new IllegalArgumentException(
				String.format("Unsupported type for 'value' column: %s.", logicalType));
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
