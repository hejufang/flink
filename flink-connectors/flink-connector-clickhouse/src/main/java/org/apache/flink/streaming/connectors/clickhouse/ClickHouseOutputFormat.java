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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * ClickHouse output format.
 */
public class ClickHouseOutputFormat extends RichOutputFormat<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseOutputFormat.class);
	private String dbName;
	private String tableName;
	private String username;
	private String password;
	private String drivername;
	private String dbURL;

	private int[] typesArray;
	private String[] columnNames;
	private transient TableSchema tableSchema;

	private Connection dbConn;
	private PreparedStatement insertStatement;
	private PreparedStatement updateStatement;
	private PreparedStatement deleteStatement;
	private int taskNumber;

	private String[] primaryKey;
	private int[] primaryKeyIndex;
	private static final String BINLOG_EVENT_TYPE_COLUMN = "event_type"; // binlog后中记录类型字段名

	private String insertQuery;
	private String updateQuery;
	private String deleteQuery;

	@Override
	public void configure(Configuration parameters) {
	}

	private int[] getPrimaryKeyIndex() {
		int[] index = new int[primaryKey.length];
		for (int i = 0; i < primaryKey.length; i++) {
			int idx = ArrayUtils.indexOf(columnNames, primaryKey[i]);
			if (-1 == idx) {
				throw new RuntimeException("Key " + primaryKey[i] + " not in columnNames: " + String.join(",", columnNames));
			}
			index[i] = idx;
		}
		return index;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		insertQuery = getInsertQuery();
		deleteQuery = getDeleteQuery();
		updateQuery = getUpdateQuery();

		primaryKeyIndex = getPrimaryKeyIndex();

		try {
			this.taskNumber = taskNumber;
			establishConnection();

			insertStatement = dbConn.prepareStatement(insertQuery);
			deleteStatement = dbConn.prepareStatement(deleteQuery);
			updateStatement = dbConn.prepareStatement(updateQuery);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	private String getInsertQuery() {
		String[] values = new String[columnNames.length];
		Arrays.fill(values, "?");

		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + dbName + "." + tableName + "(`");
		sb.append(String.join("`,`", columnNames));
		sb.append("`)");
		sb.append(" values (");
		sb.append(String.join(",", values));
		sb.append(")");
		return sb.toString();
	}

	private String getDeleteQuery() {
		String query = "delete from " + dbName + "." + tableName + " where " + getFilterQueryByPK();
		return query;
	}

	private String getFilterQueryByPK() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (String key : primaryKey) {
			sb.append("`" + key + "` = ? and ");
		}
		sb.delete(sb.length() - " and ".length(), sb.length());
		sb.append(")");
		return sb.toString();
	}

	private String getUpdateQuery() {
		StringBuilder sb = new StringBuilder();
		sb.append("update " + dbName + "." + tableName + " set ");
		for (String columnName : columnNames) {
			sb.append("`" + columnName + "`=?,");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(" where " + getFilterQueryByPK());
		return sb.toString();
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (username == null) {
			dbConn = DriverManager.getConnection(dbURL);
		} else {
			dbConn = DriverManager.getConnection(dbURL, username, password);
		}

	}

	@Override
	public void writeRecord(Row record) throws IOException {
		if (typesArray != null && typesArray.length > 0 && typesArray.length != record.getArity()) {
			LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}

		// 默认row的最后一列是binlog事件类型
		String binlogEventType = (String) record.getField(record.getArity() - 1);
		switch (binlogEventType.toUpperCase()) {
			case "INSERT":
				insertRecord(record);
				break;
			case "UPDATE":
				updateRecord(record);
				break;
			case "DELETE":
				deleteRecord(record);
				break;
			default:
				throw new RuntimeException("Unsopported binlogEventType: " + binlogEventType);
		}
	}

	// indexOfStatement和indexOfRow都是从0开始
	private PreparedStatement setStatementByIndex(PreparedStatement statement, int indexOfStatement, Row row, int indexOfRow) throws SQLException {
		// casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
		switch (typesArray[indexOfRow]) {
			case java.sql.Types.NULL:
				statement.setNull(indexOfStatement + 1, typesArray[indexOfRow]);
				break;
			case java.sql.Types.BOOLEAN:
			case java.sql.Types.BIT:
				statement.setBoolean(indexOfStatement + 1, (boolean) row.getField(indexOfRow));
				break;
			case java.sql.Types.CHAR:
			case java.sql.Types.NCHAR:
			case java.sql.Types.VARCHAR:
			case java.sql.Types.LONGVARCHAR:
			case java.sql.Types.LONGNVARCHAR:
				statement.setString(indexOfStatement + 1, (String) row.getField(indexOfRow));
				break;
			case java.sql.Types.TINYINT:
				statement.setByte(indexOfStatement + 1, (byte) row.getField(indexOfRow));
				break;
			case java.sql.Types.SMALLINT:
				statement.setShort(indexOfStatement + 1, (short) row.getField(indexOfRow));
				break;
			case java.sql.Types.INTEGER:
				statement.setInt(indexOfStatement + 1, (int) row.getField(indexOfRow));
				break;
			case java.sql.Types.BIGINT:
				statement.setLong(indexOfStatement + 1, (long) row.getField(indexOfRow));
				break;
			case java.sql.Types.REAL:
				statement.setFloat(indexOfStatement + 1, (float) row.getField(indexOfRow));
				break;
			case java.sql.Types.FLOAT:
			case java.sql.Types.DOUBLE:
				statement.setDouble(indexOfStatement + 1, (double) row.getField(indexOfRow));
				break;
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				statement.setBigDecimal(indexOfStatement + 1, (java.math.BigDecimal) row.getField(indexOfRow));
				break;
			case java.sql.Types.DATE:
				statement.setDate(indexOfStatement + 1, (java.sql.Date) row.getField(indexOfRow));
				break;
			case java.sql.Types.TIME:
				statement.setTime(indexOfStatement + 1, (java.sql.Time) row.getField(indexOfRow));
				break;
			case java.sql.Types.TIMESTAMP:
				statement.setTimestamp(indexOfStatement + 1, (java.sql.Timestamp) row.getField(indexOfRow));
				break;
			case java.sql.Types.BINARY:
			case java.sql.Types.VARBINARY:
			case java.sql.Types.LONGVARBINARY:
				statement.setBytes(indexOfStatement + 1, (byte[]) row.getField(indexOfRow));
				break;
			default:
				statement.setObject(indexOfStatement + 1, row.getField(indexOfRow));
				LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
					typesArray[indexOfRow], indexOfStatement + 1, row.getField(indexOfRow));
				// case java.sql.Types.SQLXML
				// case java.sql.Types.ARRAY:
				// case java.sql.Types.JAVA_OBJECT:
				// case java.sql.Types.BLOB:
				// case java.sql.Types.CLOB:
				// case java.sql.Types.NCLOB:
				// case java.sql.Types.DATALINK:
				// case java.sql.Types.DISTINCT:
				// case java.sql.Types.OTHER:
				// case java.sql.Types.REF:
				// case java.sql.Types.ROWID:
				// case java.sql.Types.STRUC
		}
		return statement;
	}

	public PreparedStatement setStatementByRow(PreparedStatement statement, Row row) throws SQLException {
		// types provided
		for (int index = 0; index < row.getArity() - 1; index++) { // 最后一列是BINLOG_EVENT_TYPE

			if (row.getField(index) == null) {
				statement.setNull(index + 1, typesArray[index]);
			} else {
				statement = setStatementByIndex(statement, index, row, index);
			}
		}
		return statement;
	}

	/**
	 * TODO: 对insert/update/delete操作 1)加重试配置,允许重试多次 2)加配置项 多次重试仍失败,抛异常or打log.
	 * 详见https://bytedance.feishu.cn/space/doc/doccnkWvAoxl6eYRut6ygfkXOUh#.
	 */
	public void insertRecord(Row row) {
		try {
			if (typesArray == null) {
				// no types provided
				for (int index = 0; index < row.getArity() - 1; index++) {// 最后一列是BINLOG_EVENT_TYPE
					LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
					insertStatement.setObject(index + 1, row.getField(index));
				}
			} else {
				insertStatement = setStatementByRow(insertStatement, row);
			}
			insertStatement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}
	}

	public PreparedStatement setStatementByPrimaryKey(PreparedStatement statement, int indexOfStatement, Row row, int pKindexOfRow) throws SQLException {
		return setStatementByIndex(statement, indexOfStatement, row, pKindexOfRow);
	}

	public void deleteRecord(Row row) {
		try {
			if (typesArray == null) {
				// no types provided
				for (int i = 0; i < primaryKeyIndex.length; i++) {
					LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", primaryKeyIndex[i], row.getField(primaryKeyIndex[i]));
					deleteStatement.setObject(i + 1, row.getField(primaryKeyIndex[i]));
				}

			} else {
				for (int i = 0; i < primaryKeyIndex.length; i++) {
					deleteStatement = setStatementByPrimaryKey(deleteStatement, i, row, primaryKeyIndex[i]);
				}
			}

			deleteStatement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}
	}

	public void updateRecord(Row row) {
		try {
			updateStatement = setStatementByRow(updateStatement, row);
			for (int i = 0; i < primaryKeyIndex.length; i++) {
				updateStatement = setStatementByPrimaryKey(updateStatement, columnNames.length + i, row, primaryKeyIndex[i]);
			}
			updateStatement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}

	}

	private void closeStatement(PreparedStatement statement) {
		if (statement != null) {
			flush();
			// close the connection
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.error("JDBC statement could not be closed.", e);
			} finally {
				statement = null;
			}
		}
	}

	@Override
	public void close() {
		closeStatement(insertStatement);
		closeStatement(deleteStatement);
		closeStatement(updateStatement);

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException se) {
				LOG.error("JDBC connection could not be closed.", se);
			} finally {
				dbConn = null;
			}
		}
	}

	void flush() {
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	public static ClickHouseOutputFormatBuilder buildClickHouseOutputFormat() {
		return new ClickHouseOutputFormatBuilder();
	}

	/**
	 * Builder for a {@link ClickHouseOutputFormat}.
	 */
	public static class ClickHouseOutputFormatBuilder {
		private final ClickHouseOutputFormat format;

		protected ClickHouseOutputFormatBuilder() {
			this.format = new ClickHouseOutputFormat();
		}

		public ClickHouseOutputFormatBuilder setDrivername(String drivername) {
			format.drivername = drivername;
			return this;
		}

		public ClickHouseOutputFormatBuilder setDbURL(String dbURL) {
			format.dbURL = dbURL;
			return this;
		}

		public ClickHouseOutputFormatBuilder setUserName(String username) {
			format.username = username;
			return this;
		}

		public ClickHouseOutputFormatBuilder setPassword(String password) {
			format.password = password;
			return this;
		}

		public ClickHouseOutputFormatBuilder setDbName(String dbName) {
			format.dbName = dbName;
			return this;
		}

		public ClickHouseOutputFormatBuilder setTableName(String tableName) {
			format.tableName = tableName;
			return this;
		}

		public ClickHouseOutputFormatBuilder setSqlTypes(int[] typesArray) {
			format.typesArray = typesArray;
			return this;
		}

		public ClickHouseOutputFormatBuilder setColumnNames(String[] columnNames) {
			format.columnNames = columnNames;
			return this;
		}

		public ClickHouseOutputFormatBuilder setPrimaryKey(String[] primaryKey) {
			format.primaryKey = primaryKey;
			return this;
		}

		public ClickHouseOutputFormatBuilder setTableScehma(TableSchema tableScehma) {
			format.tableSchema = tableScehma;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured ClickHouseOutputFormat
		 */
		public ClickHouseOutputFormat build() {
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied.");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied.");
			}

			return format;
		}
	}

}
