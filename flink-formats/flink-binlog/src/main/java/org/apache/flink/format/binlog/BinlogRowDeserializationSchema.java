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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.pb.DeserializationRuntimeConverter;
import org.apache.flink.formats.pb.DeserializationRuntimeConverterFactory;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.format.binlog.RuntimeConverterFactory.DESCRIPTORS;
import static org.apache.flink.format.binlog.RuntimeConverterFactory.FIELD_DESCRIPTORS;
import static org.apache.flink.table.descripters.BinlogValidator.AFTER_PREFIX;
import static org.apache.flink.table.descripters.BinlogValidator.BEFORE_PREFIX;
import static org.apache.flink.table.descripters.BinlogValidator.BINLOG_BODY;
import static org.apache.flink.table.descripters.BinlogValidator.BINLOG_HEADER;
import static org.apache.flink.table.descripters.BinlogValidator.BODY;
import static org.apache.flink.table.descripters.BinlogValidator.ENTRY;
import static org.apache.flink.table.descripters.BinlogValidator.HEADER;
import static org.apache.flink.table.descripters.BinlogValidator.INDEX_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.IS_NULLABLE_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.IS_PK_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.IS_UNSIGNED_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.MESSAGE;
import static org.apache.flink.table.descripters.BinlogValidator.NAME_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.NULL_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.PAYLOAD;
import static org.apache.flink.table.descripters.BinlogValidator.ROWDATAS;
import static org.apache.flink.table.descripters.BinlogValidator.SQL_TYPE_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.TABLE;
import static org.apache.flink.table.descripters.BinlogValidator.UPDATED_COLUMN;
import static org.apache.flink.table.descripters.BinlogValidator.VALUE_COLUMN;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Binlog row deserialization schema.
 */
public class BinlogRowDeserializationSchema implements DeserializationSchema<Row> {
	private static final Map<String, TypeInformation> FIXED_TYPE_INFO = initFixedTypeInfo();
	private final Map<String, RuntimeConverterFactory.RuntimeConverter> runtimeConverterMap;
	private final DeserializationRuntimeConverter headerRuntimeConverter;
	private final DeserializationRuntimeConverter bodyRuntimeConverter;

	private final RowTypeInfo typeInfo;
	private final boolean ignoreParseErrors;
	private final String rowtimeColumn;
	private final String proctimeColumn;
	private final Pattern targetTablePattern;

	public BinlogRowDeserializationSchema(
			RowTypeInfo typeInfo,
			String targetTable,
			String rowtimeColumn,
			String proctimeColumn,
			boolean ignoreParseErrors) {
		checkArgument(targetTable != null && !targetTable.isEmpty(),
			"targetTable cannot be null or empty.");
		checkArgument(rowtimeColumn == null || proctimeColumn == null,
			"rowtimeColumn and proctimeColumn cannot be set to non-null at the same time.");
		this.typeInfo = typeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		this.targetTablePattern = Pattern.compile(targetTable);
		this.rowtimeColumn = rowtimeColumn;
		this.proctimeColumn = proctimeColumn;
		this.headerRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(
			typeInfo.getTypeAt(typeInfo.getFieldIndex(BINLOG_HEADER)));
		this.bodyRuntimeConverter = DeserializationRuntimeConverterFactory.createConverter(
			typeInfo.getTypeAt(typeInfo.getFieldIndex(BINLOG_BODY)));
		this.runtimeConverterMap = RuntimeConverterFactory.createConverter(typeInfo);
		checkTypes();
	}

	public static Builder builder(RowTypeInfo typeInfo) {
		return new Builder(typeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			DynamicMessage binlogMessage = DynamicMessage.parseFrom(DESCRIPTORS.get(MESSAGE), message);
			DynamicMessage entryMessage =
				DynamicMessage.parseFrom(DESCRIPTORS.get(ENTRY),
					(ByteString) binlogMessage.getField(FIELD_DESCRIPTORS.get(PAYLOAD)));
			String[] fieldNames = typeInfo.getFieldNames();
			Row row = new Row(typeInfo.getArity());

			// 1. deserialize header
			DynamicMessage header = (DynamicMessage) entryMessage.getField(FIELD_DESCRIPTORS.get(HEADER));

			// filter the target data.
			String tableName = (String) header.getField(FIELD_DESCRIPTORS.get(TABLE));
			if (!targetTablePattern.matcher(tableName).matches()) {
				// return null if the data are not from the target table.
				return null;
			}

			Object headerRow =
				headerRuntimeConverter.convert(header, DESCRIPTORS.get(HEADER).getFields());
			row.setField(0, headerRow);

			// 2. deserialize body
			DynamicMessage body = (DynamicMessage) entryMessage.getField(FIELD_DESCRIPTORS.get(BODY));
			Object bodyRow =
				bodyRuntimeConverter.convert(body, DESCRIPTORS.get(BODY).getFields());
			row.setField(1, bodyRow);

			// Fill user defind fields.
			for (int i = 2; i < row.getArity(); i++) {
				String fieldName = fieldNames[i];
				if (isTimeColumn(fieldName)) {
					// set default value for computed time column.
					row.setField(i, new Timestamp(System.currentTimeMillis()));
				} else {
					// 3. deserialize rowdata
					List<DynamicMessage> rowDataList =
						(List<DynamicMessage>) body.getField(FIELD_DESCRIPTORS.get(ROWDATAS));
					// Although rowDataList is a list, but binlog ensure that it has and only has one element.
					DynamicMessage rowData = (DynamicMessage) rowDataList.get(0);
					RuntimeConverterFactory.RuntimeConverter converter =
						runtimeConverterMap.get(fieldName);
					row.setField(i, converter.convert(rowData));
				}
			}
			return row;
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize Binlog object.", t);
		}
	}

	private static Map<String, TypeInformation> initFixedTypeInfo() {
		Map<String, TypeInformation> fixedTypeInfoMap = new HashMap<>();

		fixedTypeInfoMap.put(INDEX_COLUMN, Types.INT);
		fixedTypeInfoMap.put(NAME_COLUMN, Types.STRING);
		fixedTypeInfoMap.put(IS_PK_COLUMN, Types.BOOLEAN);
		fixedTypeInfoMap.put(UPDATED_COLUMN, Types.BOOLEAN);
		fixedTypeInfoMap.put(IS_NULLABLE_COLUMN, Types.BOOLEAN);
		fixedTypeInfoMap.put(NULL_COLUMN, Types.BOOLEAN);
		fixedTypeInfoMap.put(SQL_TYPE_COLUMN, Types.STRING);
		fixedTypeInfoMap.put(IS_UNSIGNED_COLUMN, Types.BOOLEAN);
		fixedTypeInfoMap.put(VALUE_COLUMN, Types.STRING);

		return fixedTypeInfoMap;
	}

	/**
	 * Ensure that:
	 * 1. All types of user defined column are row types except time attribute columns.
	 * 2. All inner column names must be in format: 'before_'/'after_' + value in VALID_FIELD.
	 * 3. All inner column type must be matched with FIXED_TYPE_INFO.
	 */
	private void checkTypes() {
		String[] fieldNames = typeInfo.getFieldNames();
		for (int i = 0; i < typeInfo.getArity(); i++) {
			String typeName = fieldNames[i];
			if (isTimeColumn(typeName) || BINLOG_HEADER.equals(typeName) || BINLOG_BODY.equals(typeName)) {
				continue;
			}
			TypeInformation<Row> typeInformation = typeInfo.getTypeAt(i);

			if (!(typeInformation instanceof RowTypeInfo)) {
				throw new ValidationException(String.format("All user defined types have " +
					"to be Row type. But type of %s is %s.", typeName, typeInformation));
			}

			RowTypeInfo innerRowTypeInfo = (RowTypeInfo) typeInformation;
			String[] innerColumnNames = innerRowTypeInfo.getFieldNames();

			for (int j = 0; j < innerRowTypeInfo.getArity(); j++) {
				String innerColumn = innerColumnNames[j];
				TypeInformation innerColumnTypeInfo = innerRowTypeInfo.getTypeAt(j);
				if (innerColumn == null) {
					throw new ValidationException("Filed names in row cannot be null.");
				}

				String realField;
				if (innerColumn.startsWith(BEFORE_PREFIX)) {
					realField = innerColumn.substring(BEFORE_PREFIX.length());
				} else if (innerColumn.startsWith(AFTER_PREFIX)) {
					realField = innerColumn.substring(AFTER_PREFIX.length());
				} else {
					throw new ValidationException(
						String.format("Inner column name must be start with %s or %s, " +
							"but column name is %s.", BEFORE_PREFIX, AFTER_PREFIX, innerColumn));
				}
				TypeInformation expectedTypeInfo = FIXED_TYPE_INFO.get(realField);
				if (expectedTypeInfo == null) {
					throw new ValidationException(
						String.format("Invalid field: %s. Valid fields must be " +
								"%s/%s + value in list %s, e.g. 'before_value'.",
							innerColumn, BEFORE_PREFIX, AFTER_PREFIX, FIXED_TYPE_INFO.values()));
				}

				// We do not need to validate types for colume 'value' here.
				if (!VALUE_COLUMN.equals(realField) && !expectedTypeInfo.equals(innerColumnTypeInfo)) {
					throw new ValidationException(
						String.format("Expect flink type for column '%s' is %s, but we get '%s' " +
							"parsed from DDL.", innerColumn, expectedTypeInfo, innerColumnTypeInfo));
				}
			}
		}
	}

	private boolean isTimeColumn(String columnName) {
		if (columnName == null) {
			return false;
		}
		return columnName.equals(rowtimeColumn) || columnName.equals(proctimeColumn);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	/**
	 * Builder for {@link BinlogRowDeserializationSchema}.
	 * */
	public static class Builder {
		private RowTypeInfo typeInfo;
		private String targetTable;
		private boolean ignoreParseErrors;
		private String rowtimeColumn;
		private String proctimeColumn;

		private Builder(RowTypeInfo typeInfo) {
			this.typeInfo = typeInfo;
		}

		public Builder setTargetTable(String targetTable) {
			this.targetTable = targetTable;
			return this;
		}

		public Builder setRowtimeColumn(String rowtimeColumn) {
			this.rowtimeColumn = rowtimeColumn;
			return this;
		}

		public Builder setProctimeColumn(String proctimeColumn) {
			this.proctimeColumn = proctimeColumn;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public BinlogRowDeserializationSchema build() {
			return new BinlogRowDeserializationSchema(typeInfo, targetTable, rowtimeColumn, proctimeColumn, ignoreParseErrors);
		}
	}
}
