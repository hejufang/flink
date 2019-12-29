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

import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * JDBC Connection Pool for each TM. All subtask of the same task will share a connection pool.
 * We choose to implement our own pool instead of other third party pools for the following reasons:
 * 1. Keep the jdbc-connector dependency light-weight.
 * 2. Connections creation is not standard.
 * 3. We can implement a bigger pool for different tasks.
 */
public class JDBCConnectionPool {
	private static final Logger LOG = LoggerFactory.getLogger(JDBCConnectionPool.class);
	private static final ConcurrentHashMap<Integer, JDBCConnectionPool> POOLs = new ConcurrentHashMap<>();

	private final String driverName;
	private final String dbURL;
	private final String username;
	private final String password;
	private final boolean useByteDanceMysql;
	private final String initSql;
	private final int maxConnection;

	private final Semaphore semaphore;
	private final Deque<Connection> connections = new LinkedList<>();

	private volatile boolean closed = false;

	private JDBCConnectionPool(
			String driverName,
			String dbURL,
			String username,
			String password,
			boolean useByteDanceMysql,
			String initSql,
			int maxConnection) {
		this.driverName = driverName;
		this.dbURL = dbURL;
		this.username = username;
		this.password = password;
		this.useByteDanceMysql = useByteDanceMysql;
		this.initSql = initSql;

		checkArgument(maxConnection > 0, "maxConnection must be > 0");
		this.maxConnection = maxConnection;
		this.semaphore = new Semaphore(maxConnection);
	}

	public static JDBCConnectionPool getInstance(JDBCOptions options) {
		return POOLs.computeIfAbsent(options.hashCode(), key -> new JDBCConnectionPool(
			options.getDriverName(),
			options.getDbURL(),
			options.getUsername(),
			options.getPassword(),
			options.getUseBytedanceMysql(),
			options.getInitSql(),
			options.getConnectionPoolSize()));
	}

	public ConnectionWrapper getConnection() throws InterruptedException, ClassNotFoundException, SQLException {
		if (closed) {
			throw new FlinkRuntimeException("getting connection from closed JDBC connection pool.");
		}

		semaphore.acquire();

		Connection connection = null;
		synchronized (connections) {
			if (connections.size() > 0) {
				connection = connections.poll();
			}
		}
		if (connection == null) {
			connection = JDBCUtils.establishConnection(driverName, dbURL, username, password,
				useByteDanceMysql, initSql);
		}

		return new ConnectionWrapper(connection);
	}

	public void close() {
		this.closed = true;
		synchronized (connections) {
			while (connections.size() > 0) {
				try {
					connections.poll().close();
				} catch (SQLException e) {
					LOG.error("closing connection error.", e);
				}
			}
		}
	}

	/**
	 * Wrapper for Connection, for try-with-resource.
	 */
	public class ConnectionWrapper implements AutoCloseable {
		private Connection connection;

		private ConnectionWrapper(Connection connection) {
			this.connection = connection;
		}

		public Connection getConnection() {
			return connection;
		}

		@Override
		public void close() {
			if (closed) {
				try {
					connection.close();
				} catch (SQLException e) {
					LOG.error("closing connection error.", e);
				}
			} else {
				synchronized (connections) {
					connections.addLast(connection);
				}
			}
			semaphore.release();
		}
	}
}
