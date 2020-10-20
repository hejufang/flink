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

import com.bytedance.infra.bytesql4j.ByteSQLProtos;
import com.bytedance.infra.bytesql4j.ByteSQLProtos.ByteSQLErrno;
import com.bytedance.infra.bytesql4j.ByteSQLProtos.QueryResponse;
import com.bytedance.infra.bytesql4j.ByteSQLProtos.QueryResponse.ResultSet;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;

import java.sql.SQLException;
import java.sql.Timestamp;
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

	public static String generateActualSql(String sqlQuery, Row row) {
		String[] parts = sqlQuery.split("\\?");
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < parts.length; i++) {
			String part = parts[i];
			sb.append(part);
			if (i < row.getArity()) {
				sb.append(formatParameter(row.getField(i)));
			}
		}

		return sb.toString();
	}

	private static String formatParameter(Object parameter) {
		if (parameter == null) {
			return "NULL";
		} else {
			if (parameter instanceof String) {
				return "'" + ((String) parameter).replace("'", "''") + "'";
			} else if (parameter instanceof Timestamp) {
				return "to_timestamp('" + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS").
					format(parameter) + "', 'mm/dd/yyyy hh24:mi:ss.ff3')";
			} else if (parameter instanceof Boolean) {
				return (Boolean) parameter ? "1" : "0";
			} else {
				return parameter.toString();
			}
		}
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

	private static void handleResponse(ByteSQLProtos.ByteSQLErrno errno, String errMsg) throws ByteSQLException {
		switch(errno) {
			case ER_OK:
				return;
			case ER_DUP_ENTRY:
				throw new DuplicatedEntryException(errMsg);
			default:
				throw new ByteSQLException(errno, errMsg);
		}
	}
}
