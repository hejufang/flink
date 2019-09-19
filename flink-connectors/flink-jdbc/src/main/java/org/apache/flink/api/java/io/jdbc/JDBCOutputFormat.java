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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
public class JDBCOutputFormat extends AbstractJDBCOutputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCOutputFormat.class);

	private final String query;
	private final int batchInterval;
	private final int[] typesArray;

	private PreparedStatement upload;
	private int batchCount = 0;
	private List<Row> backupRows;

	public JDBCOutputFormat(String username, String password, String drivername,
			String dbURL, String query, int batchInterval, int[] typesArray,
			boolean useBytedanceMysql, String consul, String psm, String dbname, String initSql) {
		super(username, password, drivername, dbURL, useBytedanceMysql, consul, psm, dbname, initSql);
		this.query = query;
		this.batchInterval = batchInterval;
		this.typesArray = typesArray;
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an
	 * I/O problem.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			this.taskNumber = taskNumber;
			establishConnection();
			upload = connection.prepareStatement(query);
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	@Override
	public void writeRecord(Row row) throws IOException {
		try {
			backupRows.add(row);
			setRecordToStatement(upload, typesArray, row);
			upload.addBatch();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}

		batchCount++;

		if (batchCount >= batchInterval) {
			// execute batch
			flush();
		}
	}

	void flush() {
		try {
			if (!connection.isValid(VALID_CONNECTION_TIMEOUT_SEC)) {
				LOG.warn("Jdbc connection for subTask: {} is invalid, reconnecting ...", this.taskNumber);
				updatePreparedStatement();
				batchCount = 0;
				for (Row row: backupRows) {
					setRecordToStatement(upload, typesArray, row);
					upload.addBatch();
					batchCount++;
				}
			}

			upload.executeBatch();
			batchCount = 0;
			backupRows.clear();
		} catch (SQLException e) {
			throw new RuntimeException("Execution of JDBC statement failed.", e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to updatePreparedStatement");
		}
	}

	int[] getTypesArray() {
		return typesArray;
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	@Override
	public void close() throws IOException {
		if (upload != null) {
			flush();
			try {
				upload.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				upload = null;
			}
		}

		closeDbConnection();
	}

	/**
	 * Reconnection db and get new prepared statement.
	 * */
	private void updatePreparedStatement() throws SQLException, ClassNotFoundException {
		establishConnection();
		upload = connection.prepareStatement(query);
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

	/**
	 * Builder for a {@link JDBCOutputFormat}.
	 */
	public static class JDBCOutputFormatBuilder {
		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String query;
		private boolean useBytedanceMysql;
		private String consul;
		private String psm;
		private String dbname;
		private String initSql;
		private int batchInterval = DEFAULT_FLUSH_MAX_SIZE;
		private int[] typesArray;

		protected JDBCOutputFormatBuilder() {}

		public JDBCOutputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JDBCOutputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JDBCOutputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JDBCOutputFormatBuilder setQuery(String query) {
			this.query = query;
			return this;
		}

		public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
			this.batchInterval = batchInterval;
			return this;
		}

		public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
			this.typesArray = typesArray;
			return this;
		}

		public JDBCOutputFormatBuilder setUseBytedanceMysql(boolean useBytedanceMysql) {
			this.useBytedanceMysql = useBytedanceMysql;
			return this;
		}

		public JDBCOutputFormatBuilder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public JDBCOutputFormatBuilder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public JDBCOutputFormatBuilder setDbname(String dbname) {
			this.dbname = dbname;
			return this;
		}

		public JDBCOutputFormatBuilder setInitSql(String initSql) {
			this.initSql = initSql;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCOutputFormat
		 */
		public JDBCOutputFormat finish() {
			if (this.username == null) {
				LOG.info("Username was not supplied.");
			}
			if (this.password == null) {
				LOG.info("Password was not supplied.");
			}
			if (this.query == null) {
				throw new IllegalArgumentException("No query supplied.");
			}
			if (this.drivername == null) {
				throw new IllegalArgumentException("No driver supplied.");
			}

			return new JDBCOutputFormat(
					username, password, drivername, dbURL, query, batchInterval, typesArray,
				useBytedanceMysql, consul, psm, dbname, initSql);
		}
	}

}
