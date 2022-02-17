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

package org.apache.flink.connector.bytesql.internal;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.bytedance.infra.bytesql4j.proto.ColumnDef;
import com.bytedance.infra.bytesql4j.proto.ColumnType;
import com.bytedance.infra.bytesql4j.proto.ResultRow;
import com.bytedance.infra.bytesql4j.proto.ResultSet;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeBool;
import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeDatetime;
import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeFloat32;
import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeFloat64;
import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeInt8;
import static com.bytedance.infra.bytesql4j.proto.ColumnType.kColumnTypeString;

/**
 * ResultSet for byteSQL.
 */
public class ByteSQLResultSet {
	private final List<ResultRow> rows;
	private final List<ColumnDef> columnDefs;
	private final int size;
	private int nextColumnIndex;
	private ResultRow currentRow;

	private ByteSQLResultSet(List<ResultRow> rows, List<ColumnDef> columnDefs) {
		this.rows = rows;
		this.columnDefs = columnDefs;
		this.size = rows.size();
		this.nextColumnIndex = -1;
	}

	public static ByteSQLResultSet from(ResultSet resultSet) {
		return new ByteSQLResultSet(resultSet.getRowsList(), resultSet.getColumnDefsList());
	}

	public boolean next() {
		if (++nextColumnIndex < size) {
			currentRow = rows.get(nextColumnIndex);
			return true;
		} else {
			return false;
		}
	}

	public StringData getString(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeString)) {
			throw new SQLException(String.format("Invalid value for getString, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return StringData.fromString(currentRow.getValues(columnIndex).getStrVal());
	}

	public Boolean getBoolean(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeBool)) {
			throw new SQLException(String.format("Invalid value for getBoolean, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return currentRow.getValues(columnIndex).getUnsignedVal() != 0;
	}

	public Byte getByte(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeInt8)) {
			throw new SQLException(String.format("Invalid value for getByte, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return (byte) ((int) currentRow.getValues(columnIndex).getSignedVal());
	}

	public Short getShort(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		switch(type) {
			case kColumnTypeInt8:
			case kColumnTypeInt16:
				return (short) ((int) currentRow.getValues(columnIndex).getSignedVal());
			case kColumnTypeUInt8:
				return (short) ((int) currentRow.getValues(columnIndex).getUnsignedVal());
			default:
				throw new SQLException(String.format("Invalid value for getShort, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
	}

	public Integer getInt(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		switch(type) {
			case kColumnTypeInt8:
			case kColumnTypeInt16:
			case kColumnTypeInt32:
				return (int) currentRow.getValues(columnIndex).getSignedVal();
			case kColumnTypeUInt8:
			case kColumnTypeUInt16:
				return (int) currentRow.getValues(columnIndex).getUnsignedVal();
			default:
				throw new SQLException(String.format("Invalid value for getInt, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
	}

	public Long getLong(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		switch(type) {
			case kColumnTypeInt8:
			case kColumnTypeInt16:
			case kColumnTypeInt32:
			case kColumnTypeInt64:
				return currentRow.getValues(columnIndex).getSignedVal();
			case kColumnTypeUInt8:
			case kColumnTypeUInt16:
			case kColumnTypeUInt32:
				return currentRow.getValues(columnIndex).getUnsignedVal();
			default:
				throw new SQLException(String.format("Invalid value for getLong, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
	}

	public DecimalData getBigDecimal(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		BigDecimal decimal;
		switch(type) {
			case kColumnTypeInt8:
			case kColumnTypeInt16:
			case kColumnTypeInt32:
			case kColumnTypeInt64:
				decimal = BigDecimal.valueOf(currentRow.getValues(columnIndex).getSignedVal());
				break;
			case kColumnTypeUInt8:
			case kColumnTypeUInt16:
			case kColumnTypeUInt32:
				decimal = BigDecimal.valueOf(currentRow.getValues(columnIndex).getUnsignedVal());
				break;
			case kColumnTypeUInt64:
				decimal = new BigDecimal(Long.toUnsignedString(currentRow.getValues(columnIndex).getUnsignedVal()));
				break;
			default:
				throw new SQLException(String.format("Invalid value for getBigInteger, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return DecimalData.fromBigDecimal(decimal, decimal.precision(), decimal.scale());
	}

	public Float getFloat(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeFloat32)) {
			throw new SQLException(String.format("Invalid value for getFloat, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return (float) currentRow.getValues(columnIndex).getFloatVal();
	}

	public Double getDouble(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeFloat64) && !type.equals(kColumnTypeFloat32)) {
			throw new SQLException(String.format("Invalid value for getDouble, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return currentRow.getValues(columnIndex).getFloatVal();
	}

	public TimestampData getTimestamp(int columnIndex) throws SQLException {
		if (isNullAt(columnIndex)) {
			return null;
		}
		ColumnType type = columnDefs.get(columnIndex).getType();
		if (!type.equals(kColumnTypeDatetime)) {
			throw new SQLException(String.format("Invalid value for getTimestamp, the column type is : %s, " +
				"the column index is %d.", type, columnIndex));
		}
		return TimestampData.fromTimestamp(Timestamp.valueOf(currentRow.getValues(columnIndex).getStrVal()));
	}

	public boolean isNullAt(int columnIndex) {
		return currentRow.getValues(columnIndex).getIsNull();
	}
}
