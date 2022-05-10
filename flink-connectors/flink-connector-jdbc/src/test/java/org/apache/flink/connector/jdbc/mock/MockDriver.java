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

package org.apache.flink.connector.jdbc.mock;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Mocking {@link java.sql.Driver}.
 */
public class MockDriver implements Driver {

	public static final String PREFIX = "jdbc:mock:memory";

	static {
		try {
			DriverManager.registerDriver(new MockDriver());
		} catch (SQLException e) {
			throw new RuntimeException("Can't register driver!");
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (url.startsWith(PREFIX)) {
			// jdbc:mock:memory:data-id
			String[] sp = url.split(":");
			String dataId = sp[sp.length - 1];
			return new MockConnection(dataId);
		}
		return null;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(PREFIX);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[0];
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
}
