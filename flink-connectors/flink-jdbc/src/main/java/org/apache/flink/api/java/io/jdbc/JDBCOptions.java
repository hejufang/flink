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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common options of {@link JDBCScanOptions} and {@link JDBCLookupOptions} for the JDBC connector.
 */
public class JDBCOptions {
	private static final String BYTEDANCE_MYSQL_URL_TEMPLATE = "jdbc:mysql:///%s?db_consul=%s" +
		"&psm=%s&useUnicode=true&characterEncoding=utf-8&auth_enable=true";

	private String dbURL;
	private String tableName;
	private String driverName;
	private String username;
	private String password;
	private String consul;
	private String psm;
	private String dbname;
	private boolean useBytedanceMysql;
	private JDBCDialect dialect;

	private JDBCOptions(String dbURL, String tableName, String driverName, String username,
			String password, JDBCDialect dialect, boolean useBytedanceMysql, String consul,
			String psm, String dbname) {
		this.dbURL = dbURL;
		this.tableName = tableName;
		this.driverName = driverName;
		this.username = username;
		this.password = password;
		this.dialect = dialect;
		this.useBytedanceMysql = useBytedanceMysql;
		this.consul = consul;
		this.psm = psm;
		this.dbname = dbname;
	}

	public String getDbURL() {
		return dbURL;
	}

	public String getTableName() {
		return tableName;
	}

	public String getDriverName() {
		return driverName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public JDBCDialect getDialect() {
		return dialect;
	}

	public boolean getUseBytedanceMysql() {
		return useBytedanceMysql;
	}

	public String getConsul() {
		return consul;
	}

	public String getPsm() {
		return psm;
	}

	public String getDbname() {
		return dbname;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JDBCOptions) {
			JDBCOptions options = (JDBCOptions) o;
			return Objects.equals(dbURL, options.dbURL) &&
				Objects.equals(tableName, options.tableName) &&
				Objects.equals(driverName, options.driverName) &&
				Objects.equals(username, options.username) &&
				Objects.equals(password, options.password) &&
				Objects.equals(dialect.getClass().getName(), options.dialect.getClass().getName()) &&
				Objects.equals(consul, options.consul) &&
				Objects.equals(psm, options.psm) &&
				useBytedanceMysql == options.useBytedanceMysql &&
				Objects.equals(dbname, options.dbname);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JDBCOptions}.
	 */
	public static class Builder {
		private String dbURL;
		private String tableName;
		private String driverName;
		private String username;
		private String password;
		private String consul;
		private String psm;
		private boolean useBytedanceMysql;
		private String dbname;
		private JDBCDialect dialect;

		/**
		 * required, table name.
		 */
		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * optional, user name.
		 */
		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * optional, password.
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * optional, driver name, dialect has a default driver name,
		 * See {@link JDBCDialect#defaultDriverName}.
		 */
		public Builder setDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		/**
		 * required, JDBC DB url.
		 */
		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		/**
		 * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by
		 * {@link JDBCDialects#get} from DB url.
		 */
		public Builder setDialect(JDBCDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		public Builder setUseBytedanceMysql(boolean useBytedanceMysql) {
			this.useBytedanceMysql = useBytedanceMysql;
			return this;
		}

		public Builder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public Builder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public Builder setDbname(String dbname) {
			this.dbname = dbname;
			return this;
		}

		public JDBCOptions build() {
			checkNotNull(tableName, "No tableName supplied.");
			if (this.dialect == null) {
				Optional<JDBCDialect> optional = JDBCDialects.get(dbURL);
				this.dialect = optional.orElseGet(() -> {
					throw new NullPointerException("No dialect supplied.");
				});
			}
			if (this.driverName == null) {
				Optional<String> optional = dialect.defaultDriverName();
				this.driverName = optional.orElseGet(() -> {
					throw new NullPointerException("No driverName supplied.");
				});
			}

			if (dbURL == null) {
				if (useBytedanceMysql) {
					this.dbURL = String.format(BYTEDANCE_MYSQL_URL_TEMPLATE, dbname, consul, psm);
				} else {
					throw new RuntimeException("Can't init db url, because " +
						"dbUrl == null & useBytedanceMysql is false");
				}
			}
			return new JDBCOptions(dbURL, tableName, driverName, username, password,
				dialect, useBytedanceMysql, consul, psm, dbname);
		}
	}
}
