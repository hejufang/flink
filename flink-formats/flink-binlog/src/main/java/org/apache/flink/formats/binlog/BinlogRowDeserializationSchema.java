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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.pb.DeserializationRuntimeConverterFactory;
import org.apache.flink.formats.pb.PbFormatUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.binlog.DRCEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flink.formats.binlog.BinlogOptions.AFTER_PREFIX;
import static org.apache.flink.formats.binlog.BinlogOptions.BEFORE_PREFIX;
import static org.apache.flink.formats.binlog.BinlogOptions.BODY;
import static org.apache.flink.formats.binlog.BinlogOptions.ENTRY;
import static org.apache.flink.formats.binlog.BinlogOptions.HEADER;
import static org.apache.flink.formats.binlog.BinlogOptions.INDEX_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.IS_NULLABLE_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.IS_PK_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.IS_UNSIGNED_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.MESSAGE;
import static org.apache.flink.formats.binlog.BinlogOptions.NAME_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.NULL_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.PAYLOAD;
import static org.apache.flink.formats.binlog.BinlogOptions.ROWDATAS;
import static org.apache.flink.formats.binlog.BinlogOptions.SQL_TYPE_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.TABLE;
import static org.apache.flink.formats.binlog.BinlogOptions.UPDATED_COLUMN;
import static org.apache.flink.formats.binlog.BinlogOptions.VALUE_COLUMN;
import static org.apache.flink.formats.binlog.RuntimeConverterFactory.DESCRIPTORS;
import static org.apache.flink.formats.binlog.RuntimeConverterFactory.FIELD_DESCRIPTORS;
import static org.apache.flink.formats.pb.DeserializationRuntimeConverterFactory.DeserializationRuntimeConverter;

/**
 * Binlog row deserialization schema.
 */
public class BinlogRowDeserializationSchema implements DeserializationSchema<RowData> {
	private static final Logger LOG = LoggerFactory.getLogger(BinlogRowDeserializationSchema.class);

	private static final Map<String, Class<? extends LogicalType>> FIXED_TYPE_INFO = initFixedTypeInfo();
	private transient Map<String, RuntimeConverterFactory.RuntimeConverter> runtimeConverterMap;
	private transient DeserializationRuntimeConverter headerRuntimeConverter;
	private transient DeserializationRuntimeConverter bodyRuntimeConverter;
	private transient int bodyIndex;
	private transient int headIndex;

	private final RowType rowType;
	private final TypeInformation<RowData> resultTypeInfo;
	private final boolean ignoreParseErrors;
	private final Pattern targetTablePattern;
	private final String binlogHeaderName;
	private final String binlogBodyName;

	public BinlogRowDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			String targetTable,
			boolean ignoreParseErrors,
			String binlogHeaderName,
			String binlogBodyName) {
		this.rowType = rowType;
		this.resultTypeInfo = resultTypeInfo;
		this.ignoreParseErrors = ignoreParseErrors;
		if (targetTable != null) {
			this.targetTablePattern = Pattern.compile(targetTable);
		} else {
			this.targetTablePattern = null;
		}
		this.binlogHeaderName = binlogHeaderName;
		this.binlogBodyName = binlogBodyName;
		checkTypes();
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		RowType headerRowType = (RowType) PbFormatUtils.createDataType(
			DRCEntry.EntryHeader.getDescriptor(), false).getLogicalType();
		this.headerRuntimeConverter = DeserializationRuntimeConverterFactory.createRowConverter(
			headerRowType, DRCEntry.EntryHeader.getDescriptor());
		RowType bodyRowType = (RowType) PbFormatUtils.createDataType(
			DRCEntry.EntryBody.getDescriptor(), false).getLogicalType();
		this.bodyRuntimeConverter = DeserializationRuntimeConverterFactory.createRowConverter(
			bodyRowType, DRCEntry.EntryBody.getDescriptor());
		this.runtimeConverterMap =
			RuntimeConverterFactory.createConverter(this.rowType, binlogHeaderName, binlogBodyName);
		this.headIndex = this.rowType.getFieldIndex(binlogHeaderName);
		this.bodyIndex = this.rowType.getFieldIndex(binlogBodyName);
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			DynamicMessage binlogMessage = DynamicMessage.parseFrom(DESCRIPTORS.get(MESSAGE), message);
			DynamicMessage entryMessage =
				DynamicMessage.parseFrom(DESCRIPTORS.get(ENTRY),
					(ByteString) binlogMessage.getField(FIELD_DESCRIPTORS.get(PAYLOAD)));
			List<String> fieldNames = rowType.getFieldNames();
			GenericRowData rowData = new GenericRowData(rowType.getFieldCount());
			// 1. deserialize header
			DynamicMessage header = (DynamicMessage) entryMessage.getField(FIELD_DESCRIPTORS.get(HEADER));

			// filter the target data.
			String tableName = (String) header.getField(FIELD_DESCRIPTORS.get(TABLE));
			if (targetTablePattern != null && !targetTablePattern.matcher(tableName).matches()) {
				// return null if the data are not from the target table.
				return null;
			}

			Object headerRow =
				headerRuntimeConverter.convert(header);
			if (headIndex != -1) {
				rowData.setField(headIndex, headerRow);
			}

			// 2. deserialize body
			DynamicMessage body = (DynamicMessage) entryMessage.getField(FIELD_DESCRIPTORS.get(BODY));
			Object bodyRow =
				bodyRuntimeConverter.convert(body);
			if (bodyIndex != -1) {
				rowData.setField(bodyIndex, bodyRow);
			}

			// Fill user defind fields.
			for (int i = 0; i < rowData.getArity(); i++) {
				if (i == headIndex || i == bodyIndex) {
					continue;
				}
				String fieldName = fieldNames.get(i);
				// 3. deserialize rowdata
				List<DynamicMessage> rowDataList =
					(List<DynamicMessage>) body.getField(FIELD_DESCRIPTORS.get(ROWDATAS));
				// Although rowDataList is a list, but binlog ensure that it has and only has one element.
				DynamicMessage dynamicMessage = (DynamicMessage) rowDataList.get(0);
				RuntimeConverterFactory.RuntimeConverter converter =
					runtimeConverterMap.get(fieldName);
				rowData.setField(i, converter.convert(dynamicMessage));
			}
			return rowData;
		} catch (Throwable t) {
			String errorMsg = String.format("Deserialized failed base64 `%s`.", Base64.getEncoder().encodeToString(message));
			if (ignoreParseErrors) {
				LOG.warn(errorMsg, t);
				return null;
			}
			throw new FlinkRuntimeException(errorMsg, t);
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

	private static Map<String, Class<? extends LogicalType>> initFixedTypeInfo() {
		Map<String, Class<? extends LogicalType>> fixedTypeInfoMap = new HashMap<>();

		fixedTypeInfoMap.put(INDEX_COLUMN, IntType.class);
		fixedTypeInfoMap.put(NAME_COLUMN, VarCharType.class);
		fixedTypeInfoMap.put(IS_PK_COLUMN, BooleanType.class);
		fixedTypeInfoMap.put(UPDATED_COLUMN, BooleanType.class);
		fixedTypeInfoMap.put(IS_NULLABLE_COLUMN, BooleanType.class);
		fixedTypeInfoMap.put(NULL_COLUMN, BooleanType.class);
		fixedTypeInfoMap.put(SQL_TYPE_COLUMN, VarCharType.class);
		fixedTypeInfoMap.put(IS_UNSIGNED_COLUMN, BooleanType.class);
		fixedTypeInfoMap.put(VALUE_COLUMN, VarCharType.class);

		return fixedTypeInfoMap;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Ensure that:
	 * 1. All types of user defined column are row types except time attribute columns.
	 * 2. All inner column names must be in format: 'before_'/'after_' + value in VALID_FIELD.
	 * 3. All inner column type must be matched with FIXED_TYPE_INFO.
	 */
	private void checkTypes() {
		List<String> fieldNames = rowType.getFieldNames();
		for (int i = 0; i < rowType.getFieldCount(); i++) {
			String typeName = fieldNames.get(i);
			if (binlogHeaderName.equals(typeName) || binlogBodyName.equals(typeName)) {
				continue;
			}
			LogicalType typeInformation = rowType.getTypeAt(i);

			if (!(typeInformation instanceof RowType)) {
				throw new ValidationException(String.format("All user defined types have " +
					"to be Row type. But type of %s is %s.", typeName, typeInformation));
			}

			RowType innerRowType = (RowType) typeInformation;
			List<String> innerColumnNames = innerRowType.getFieldNames();
			for (int j = 0; j < innerRowType.getFieldCount(); j++) {
				String innerColumnName = innerColumnNames.get(j);
				LogicalType innerColumnLogicalType = innerRowType.getTypeAt(j);
				if (innerColumnName == null) {
					throw new ValidationException("Filed names in row cannot be null.");
				}

				String realField;
				if (innerColumnName.startsWith(BEFORE_PREFIX)) {
					realField = innerColumnName.substring(BEFORE_PREFIX.length());
				} else if (innerColumnName.startsWith(AFTER_PREFIX)) {
					realField = innerColumnName.substring(AFTER_PREFIX.length());
				} else {
					throw new ValidationException(
						String.format("Inner column name must be start with %s or %s, " +
							"but column name is %s.", BEFORE_PREFIX, AFTER_PREFIX, innerColumnName));
				}
				Class<? extends LogicalType> expectedTypeInfo = FIXED_TYPE_INFO.get(realField);
				if (expectedTypeInfo == null) {
					throw new ValidationException(
						String.format("Invalid field: %s. Valid fields must be " +
								"%s/%s + value in list %s, e.g. 'before_value'.",
							innerColumnName, BEFORE_PREFIX, AFTER_PREFIX, FIXED_TYPE_INFO.values()));
				}

				// We do not need to validate types for colume 'value' here.
				if (!VALUE_COLUMN.equals(realField) && !expectedTypeInfo.isInstance(innerColumnLogicalType)) {
					throw new ValidationException(
						String.format("Expect flink type for column '%s' is %s, but we get '%s' " +
							"parsed from DDL.", innerColumnName, expectedTypeInfo, innerColumnLogicalType));
				}
			}
		}
	}

	/**
	 * BinlogRowDeserializationSchema builder.
	 */
	public static class Builder {
		private RowType rowType;
		private TypeInformation<RowData> resultTypeInfo;
		private String targetTable;
		private boolean ignoreParseErrors;
		private String binlogHeaderName;
		private String binlogBodyName;

		private Builder() {
		}

		public Builder setRowType(RowType rowType) {
			this.rowType = rowType;
			return this;
		}

		public Builder setResultTypeInfo(TypeInformation<RowData> resultTypeInfo) {
			this.resultTypeInfo = resultTypeInfo;
			return this;
		}

		public Builder setTargetTable(String targetTable) {
			this.targetTable = targetTable;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setBinlogHeaderName(String binlogHeaderName) {
			this.binlogHeaderName = binlogHeaderName;
			return this;
		}

		public Builder setBinlogBodyName(String binlogBodyName) {
			this.binlogBodyName = binlogBodyName;
			return this;
		}

		public BinlogRowDeserializationSchema build() {
			return new BinlogRowDeserializationSchema(rowType,
				resultTypeInfo, targetTable, ignoreParseErrors, binlogHeaderName, binlogBodyName);
		}
	}
}
