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

package org.apache.flink.connector.jdbc.internal.connection;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import com.bytedance.mysql.MysqlDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

	private final JdbcConnectionOptions jdbcOptions;

	private transient volatile Connection connection;

	public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
		this.jdbcOptions = jdbcOptions;
	}

	@Override
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		if (connection == null) {
			synchronized (this) {
				if (connection == null) {
					try {
						Class.forName(
							jdbcOptions.getDriverName(),
							true,
							Thread.currentThread().getContextClassLoader());
						if (jdbcOptions.getUseBytedanceMysql()) {
							if (jdbcOptions.getUsername().isPresent()) {
								connection = MysqlDriverManager.getConnection(jdbcOptions.getDbURL(),
									jdbcOptions.getUsername().get(), jdbcOptions.getPassword().get());
							} else {
								connection = ByteDanceMysqlConnectionProvider.getConnection(jdbcOptions.getDbURL());
							}
						} else {
							if (jdbcOptions.getUsername().isPresent()) {
								connection = DriverManager.getConnection(jdbcOptions.getDbURL(),
									jdbcOptions.getUsername().get(), jdbcOptions.getPassword().get());
							} else {
								connection = DriverManager.getConnection(jdbcOptions.getDbURL());
							}
						}
					} catch (SQLException e) {
						throw new RuntimeException("An error occurs when connect to mysql, this is usually because you wrote a " +
							"wrong db name or table name, or do not have permission for this db, please check it!", e);
					}

					if (connection == null) {
						String errMsg;
						if (jdbcOptions.getUsername().isPresent() && jdbcOptions.getPassword().isPresent()) {
							errMsg = String.format("can't get connection, dbUrl = %s, username = %s, " +
									"password = %s", jdbcOptions.getDbURL(), jdbcOptions.getUsername().get(),
								jdbcOptions.getPassword().get());
						} else {
							errMsg = String.format("can't get connection, dbUrl = %s", jdbcOptions.getDbURL());
						}
						throw new RuntimeException(errMsg);
					}

					if (jdbcOptions.getInitSql() != null) {
						try (Statement statement = connection.createStatement()){
							LOG.info("Execute init sql: {}", jdbcOptions.getInitSql());
							statement.execute(jdbcOptions.getInitSql());
						}
					}
				}
			}
		}
		return connection;
	}

	@Override
	public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
		try {
			connection.close();
		} catch (SQLException e) {
			LOG.info("JDBC connection close failed.", e);
		} finally {
			connection = null;
		}
		connection = getConnection();
		return connection;
	}
}
