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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.stream.IntStream;

/**
 * Converter that is responsible to convert between ByteSQL and Flink SQL internal data structure {@link RowData}.
 */
public class ByteSQLRowConverter implements Serializable {
	private static final long serialVersionUID = 1L;
	private final FieldGetter[] fieldGetters;

	public ByteSQLRowConverter(RowType rowType) {
		this.fieldGetters = IntStream
			.range(0, rowType.getFields().size())
			.mapToObj(pos -> createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(FieldGetter[]::new);
	}

	/**
	 * Convert data retrieved from {@link ByteSQLResultSet} to internal {@link RowData}.
	 *
	 * @param resultSet ResultSet from JDBC
	 */
	public RowData toInternal(ByteSQLResultSet resultSet) throws SQLException {
		final int length = fieldGetters.length;
		final GenericRowData row = new GenericRowData(length);
		for (int pos = 0; pos < length; pos++) {
			row.setField(pos, fieldGetters[pos].getFieldOrNull(resultSet));
		}
		return row;
	}

	/**
	 * Creates an accessor for getting elements in an {@link ByteSQLResultSet} at the
	 * given position.
	 *
	 * @param fieldType the element type of the row
	 * @param fieldPos the element type of the row
	 */
	static FieldGetter createFieldGetter(LogicalType fieldType, int fieldPos) {
		final FieldGetter fieldGetter;
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				fieldGetter = row -> row.getString(fieldPos);
				break;
			case BOOLEAN:
				fieldGetter = row -> row.getBoolean(fieldPos);
				break;
			case DECIMAL:
				fieldGetter = row -> row.getBigDecimal(fieldPos);
				break;
			case TINYINT:
				fieldGetter = row -> row.getByte(fieldPos);
				break;
			case SMALLINT:
				fieldGetter = row -> row.getShort(fieldPos);
				break;
			case INTEGER:
				fieldGetter = row -> row.getInt(fieldPos);
				break;
			case BIGINT:
				fieldGetter = row -> row.getLong(fieldPos);
				break;
			case FLOAT:
				fieldGetter = row -> row.getFloat(fieldPos);
				break;
			case DOUBLE:
				fieldGetter = row -> row.getDouble(fieldPos);
				break;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				fieldGetter = row -> row.getTimestamp(fieldPos);
				break;
			default:
				throw new UnsupportedOperationException("Unsupported type for ByteSQL source");
		}
		return fieldGetter;
	}

	/**
	 * Accessor for getting the field of a row during runtime.
	 *
	 * @see #createFieldGetter(LogicalType, int)
	 */
	interface FieldGetter extends Serializable {
		@Nullable
		Object getFieldOrNull(ByteSQLResultSet resultSet) throws SQLException;
	}
}
