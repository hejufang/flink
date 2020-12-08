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

package org.apache.flink.connectors.bytesql.utils;

import org.apache.flink.connectors.bytesql.ByteSQLResultSet;
import org.apache.flink.connectors.bytesql.internal.ByteSQLRowConverter;
import org.apache.flink.types.Row;

import com.bytedance.infra.bytesql4j.SqlValue;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;
import com.bytedance.infra.bytesql4j.exception.UnSupportedException;
import com.bytedance.infra.bytesql4j.proto.ByteSQLErrno;
import com.bytedance.infra.bytesql4j.proto.QueryResponse;
import com.bytedance.infra.bytesql4j.proto.QueryResponse.ResultSet;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utils for processing response from ByteSQL.
 */
public class ByteSQLUtils {

	public static String quoteIdentifier(String identifier) {
		return "`" + identifier + "`";
	}

	/**
	 * Get select fields statement by condition fields.
	 */
	public static String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		String selectExpressions = String.join(", ", selectFields);
		String fieldExpressions = Arrays.stream(conditionFields)
			.map(f -> f + "=?")
			.collect(Collectors.joining(" AND "));
		return "SELECT " + selectExpressions + " FROM " +
			tableName + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
	}

	public static String getInsertIntoStatement(String tableName, String[] fieldNames) {
		String columns = Arrays.stream(fieldNames)
			.map(ByteSQLUtils::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames)
			.map(f -> "?")
			.collect(Collectors.joining(", "));
		return "INSERT INTO " + quoteIdentifier(tableName) +
			"(" + columns + ")" + " VALUES (" + placeholders + ")";
	}

	public static String getUpsertStatement(String tableName, String[] fieldNames) {
		String updateClause = Arrays.stream(fieldNames)
			.map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
			.collect(Collectors.joining(", "));
		return getInsertIntoStatement(tableName, fieldNames) +
			" ON DUPLICATE KEY UPDATE " + updateClause;
	}

	public static String getDeleteStatement(String tableName, String[] conditionFields) {
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
	}

	public static String generateActualSql(String sqlQuery, Row row) throws ByteSQLException {
		String[] parts = sqlQuery.split("\\?");
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < parts.length; i++) {
			String part = parts[i];
			sb.append(part);
			if (i < row.getArity()) {
				sb.append(format2sql(row.getField(i)));
			}
		}

		return sb.toString();
	}

	public static List<Row> convertResponseToRows(
			QueryResponse response,
			ByteSQLRowConverter rowConverter) throws ByteSQLException, SQLException {
		List<Row> resultList = new ArrayList<>();
		handleResponse(response.getErrCode(), response.hasErrorMsg() ? response.getErrorMsg() : "");
		if (response.getErrCode() == ByteSQLErrno.ER_OK) {
			ResultSet resultSet = response.getResultset();
			if (resultSet.getRowsCount() > 0) {
				ByteSQLResultSet byteSQLResultSet = ByteSQLResultSet.from(resultSet);
				while (byteSQLResultSet.next()) {
					resultList.add(rowConverter.toInternal(byteSQLResultSet));
				}
			}
		}
		return resultList;
	}

	private static void handleResponse(ByteSQLErrno errno, String errMsg) throws ByteSQLException {
		switch(errno) {
			case ER_OK:
				return;
			case ER_DUP_ENTRY:
				throw new DuplicatedEntryException(errMsg);
			default:
				throw new ByteSQLException(errno, errMsg);
		}
	}

	private static String format2sql(Object value) throws ByteSQLException {
		if (value == null) {
			return "NULL";
		} else if (value instanceof SqlValue) {
			return value.toString();
		} else if (!(value instanceof Boolean) && !(value instanceof Byte) && !(value instanceof Short) && !(value instanceof Integer) && !(value instanceof Long) && !(value instanceof Float) && !(value instanceof Double)) {
			if (value instanceof String) {
				String str = new String(escapeStringBackslash((String) value));
				return String.format("'%s'", str);
			} else if (value instanceof Timestamp) {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
				return String.format("'%s'", dateFormat.format((Timestamp) value));
			} else if (value instanceof BigInteger) {
				return ((BigInteger) value).toString(10);
			} else {
				throw new UnSupportedException(String.format("%s is not supported", value.getClass().getTypeName()));
			}
		} else {
			return value.toString();
		}
	}

	private static byte[] escapeBytesBackslash(byte[] value) {
		byte[] bytes = new byte[value.length * 2];
		int pos = 0;
		byte[] var3 = value;
		int var4 = value.length;

		for (int var5 = 0; var5 < var4; ++var5) {
			byte c = var3[var5];
			switch(c) {
				case 0:
					bytes[pos] = 92;
					bytes[pos + 1] = 48;
					pos += 2;
					break;
				case 10:
					bytes[pos] = 92;
					bytes[pos + 1] = 110;
					pos += 2;
					break;
				case 13:
					bytes[pos] = 92;
					bytes[pos + 1] = 114;
					pos += 2;
					break;
				case 26:
					bytes[pos] = 92;
					bytes[pos + 1] = 90;
					pos += 2;
					break;
				case 34:
					bytes[pos] = 92;
					bytes[pos + 1] = 34;
					pos += 2;
					break;
				case 39:
					bytes[pos] = 92;
					bytes[pos + 1] = 39;
					pos += 2;
					break;
				case 92:
					bytes[pos] = 92;
					bytes[pos + 1] = 92;
					pos += 2;
					break;
				default:
					bytes[pos] = c;
					++pos;
			}
		}

		return Arrays.copyOfRange(bytes, 0, pos);
	}

	private static byte[] escapeStringBackslash(String value) {
		return escapeBytesBackslash(value.getBytes(Charset.forName("UTF-8")));
	}
}
