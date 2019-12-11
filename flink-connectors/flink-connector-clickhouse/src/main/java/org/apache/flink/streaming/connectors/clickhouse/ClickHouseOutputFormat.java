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
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.commons.consul.Discovery;
import com.bytedance.commons.consul.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * ClickHouse output format.
 */
public class ClickHouseOutputFormat extends RichOutputFormat<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(ClickHouseOutputFormat.class);
	private static final String USER = "user";
	private static final String PASSWORD = "password";
	private static final String REPLICA_NUM = "replica_num";

	private String dbName;
	private String tableName;
	private String username;
	private String password;
	private String drivername;
	private String dbURL;
	private String dbPsm;
	// clickhouse里特殊的列, 1代表insert, -1代表delete
	private String signColumnName;

	private int[] typesArray;
	private String[] columnNames;
	private transient TableSchema tableSchema;

	private Connection dbConn;
	private PreparedStatement insertStatement;

	private int batchCount = 0;
	private int flushMaxSize;
	private int taskNumber;
	private int parallelism;
	private Properties properties;

	private String insertQuery;

	static {
		DriverManager.setLogWriter(new PrintWriter(System.out));
	}

	public int getParallelism() {
		return parallelism;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		insertQuery = getInsertQuery();

		try {
			this.taskNumber = taskNumber;
			establishConnection();

			insertStatement = dbConn.prepareStatement(insertQuery);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	private String getInsertQuery() {
		List<String> columns = new ArrayList<>(Arrays.asList(columnNames));
		if (signColumnName != null) {
			columns.add(signColumnName); // 加上sign列
		}
		String[] values = new String[columns.size()];
		Arrays.fill(values, "?");

		StringBuilder sb = new StringBuilder();
		sb.append("insert into " + dbName + "." + tableName + "(`");
		sb.append(String.join("`,`", columns));
		sb.append("`)");
		sb.append(" values (");
		sb.append(String.join(",", values));
		sb.append(")");
		return sb.toString();
	}

	private void establishConnection() throws SQLException, ClassNotFoundException {
		Class.forName(drivername);
		if (dbPsm != null) {
			dbURL = getDbUrlFromPsm();
		}
		if (username != null) {
			properties.put(USER, username);
		}
		if (password != null) {
			properties.put(PASSWORD, password);
		}
		dbConn = DriverManager.getConnection(dbURL, properties);
	}

	@Override
	public void writeRecord(Row record) {
		if (typesArray != null && typesArray.length > 0 && typesArray.length != record.getArity() - 1) {
			LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
		}

		// 默认row的最后一列是事件类型(insert/delete)
		String eventTypeValue = (String) record.getField(record.getArity() - 1);
		EventType eventType = EventType.valueOf(eventTypeValue.toUpperCase());
		insertRecord(record, eventType);

		try {
			if (batchCount >= flushMaxSize) {
				// execute batch
				flush();
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Writing records to ClickHouse JDBC failed.", e);
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

	public void insertRecord(Row row, EventType eventType) {
		try {
			switch (eventType) {
				//insert对应的sign列是1, delete对应的sign列是-1, update需拆成delete+insert
				case INSERT:
					if (signColumnName != null) {
						insertStatement.setInt(row.getArity(), 1);
					}
					break;
				case DELETE:
					if (signColumnName == null) {
						throw new FlinkRuntimeException("eventType can not be delete/DELETE when sign column is null.");
					}
					insertStatement.setInt(row.getArity(), -1);
					break;
				default:
					throw new FlinkRuntimeException("Unsupported EventType: " + eventType);
			}

			insertStatement = setStatementByRow(insertStatement, row);
			insertStatement.addBatch();
			batchCount++;
		} catch (SQLException e) {
			throw new FlinkRuntimeException("Preparation of JDBC statement failed.", e);
		}
	}

	private void closeStatement(PreparedStatement statement) {
		if (statement != null) {
			try {
				flush();
			} catch (Exception e) {
				throw new FlinkRuntimeException("Writing records to ClickHouse JDBC failed.", e);
			}
			// close the connection
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.error("ClickHouse JDBC statement could not be closed.", e);
			} finally {
				statement = null;
			}
		}
	}

	@Override
	public void close() {
		closeStatement(insertStatement);

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
		try {
			insertStatement.executeBatch();
			batchCount = 0;
		} catch (SQLException e) {
			throw new FlinkRuntimeException("Execution of ClickHouse statement failed.", e);
		}
	}

	public TableSchema getTableSchema() {
		return tableSchema;
	}

	private String getDbUrlFromPsm() {
		if (dbPsm == null) {
			return null;
		}

		String dbUrl;
		Discovery discovery = new Discovery();
		List<ServiceNode> serviceNodeList = discovery.translateOne(dbPsm);
		List<ServiceNode> serviceNodeShuffleList;
		List<ServiceNode> serviceNodeListWithReplicaNum = discovery.translateOne(dbPsm).stream()
			.filter(x -> x.getTags().containsKey(REPLICA_NUM))
			.collect(Collectors.toList());
		if (serviceNodeListWithReplicaNum.isEmpty()){
			// 没有replica_num参数的可以随机选一个节点
			serviceNodeShuffleList = serviceNodeList;
			LOG.info("Psm " + dbPsm + " has no replica_num tag");
		} else {
			// replica_num是1代表主, 2代表备. 写分布式表时既可用主也可以用备, 写本地表时只能用主. 此处统一选主节点.
			List<ServiceNode> masterServiceNodeList = serviceNodeList.stream()
				.filter(x -> x.getTags().getOrDefault(REPLICA_NUM, "").equals("1"))
				.collect(Collectors.toList());
			serviceNodeShuffleList = masterServiceNodeList;
			LOG.info("Psm " + dbPsm + "has replica_num tag");
		}
		if (serviceNodeShuffleList == null || 0 == serviceNodeShuffleList.size()) {
			throw new IllegalArgumentException("Invalid clickhouse psm: " + dbPsm);
		}
		Collections.shuffle(serviceNodeShuffleList, new Random(System.currentTimeMillis()));
		ServiceNode sn = serviceNodeShuffleList.get(0);
		String host = sn.getHost();
		int port = sn.getPort();
		dbUrl = "jdbc:clickhouse://" + host + ":" + port + "/";
		LOG.info("Get dbUrl from clickhouse psm: " + dbUrl);

		LOG.info("dbUrl is : " + dbUrl);
		return dbUrl;
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

		public ClickHouseOutputFormatBuilder setDbPsm(String dbPsm) {
			format.dbPsm = dbPsm;
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

		public ClickHouseOutputFormatBuilder setSignColumnName(String signColumnName) {
			format.signColumnName = signColumnName;
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

		public ClickHouseOutputFormatBuilder setTableScehma(TableSchema tableScehma) {
			format.tableSchema = tableScehma;
			return this;
		}

		public ClickHouseOutputFormatBuilder setFlushMaxSize(int flushMaxSize) {
			format.flushMaxSize = flushMaxSize;
			return this;
		}

		public ClickHouseOutputFormatBuilder setParallelism(int parallelism) {
			format.parallelism = parallelism;
			return this;
		}

		public ClickHouseOutputFormatBuilder setProperties(Properties properties) {
			format.properties = properties;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured ClickHouseOutputFormat
		 */
		public ClickHouseOutputFormat build() {
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied.");
			}

			return format;
		}
	}

	/**
	 * EventType.
	 */
	public enum EventType {
		INSERT,
		DELETE
	}

}
