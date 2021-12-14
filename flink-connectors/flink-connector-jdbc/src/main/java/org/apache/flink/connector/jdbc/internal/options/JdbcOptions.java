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

package org.apache.flink.connector.jdbc.internal.options;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Options for the JDBC connector.
 */
public class JdbcOptions extends JdbcConnectionOptions {

	private static final long serialVersionUID = 1L;
	private static final String BYTEDANCE_MYSQL_URL_TEMPLATE = "jdbc:mysql:///%s?db_consul=%s" +
		"&psm=%s&useUnicode=true&characterEncoding=utf-8&auth_enable=true&serverTimezone=%s" +
		"&zeroDateTimeBehavior=CONVERT_TO_NULL";

	public static final int CONNECTION_CHECK_TIMEOUT_SECONDS = 60;
	private static final boolean DEFAULT_COMPATIBLE_MODE = true;

	private String tableName;
	private JdbcDialect dialect;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final boolean compatibleMode;

	private JdbcOptions(
			String dbURL,
			String tableName,
			String driverName,
			String username,
			String password,
			JdbcDialect dialect,
			boolean useBytedanceMysql,
			String consul,
			String psm,
			String dbname,
			String initSql,
			FlinkConnectorRateLimiter rateLimiter,
			boolean compatibleMode) {
		super(dbURL, driverName, username, password, useBytedanceMysql, consul, psm, dbname, initSql);
		this.tableName = tableName;
		this.dialect = dialect;
		this.rateLimiter = rateLimiter;
		this.compatibleMode = compatibleMode;
	}

	public String getTableName() {
		return tableName;
	}

	public JdbcDialect getDialect() {
		return dialect;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public boolean isCompatibleMode() {
		return compatibleMode;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof JdbcOptions) {
			JdbcOptions options = (JdbcOptions) o;
			return Objects.equals(url, options.url) &&
				Objects.equals(tableName, options.tableName) &&
				Objects.equals(driverName, options.driverName) &&
				Objects.equals(username, options.username) &&
				Objects.equals(password, options.password) &&
				Objects.equals(dialect.getClass().getName(), options.dialect.getClass().getName()) &&
				Objects.equals(useBytedanceMysql, options.useBytedanceMysql) &&
				Objects.equals(consul, options.consul) &&
				Objects.equals(psm, options.psm) &&
				Objects.equals(dbname, options.dbname) &&
				Objects.equals(initSql, options.initSql) &&
				Objects.equals(rateLimiter, options.rateLimiter) &&
				Objects.equals(compatibleMode, options.compatibleMode);
		} else {
			return false;
		}
	}

	/**
	 * Builder of {@link JdbcOptions}.
	 */
	public static class Builder {
		private String dbURL;
		private String tableName;
		private String driverName;
		private String username;
		private String password;
		private JdbcDialect dialect;
		private boolean useBytedanceMysql;
		private String consul;
		private String psm;
		private String dbname;
		private String initSql;
		private FlinkConnectorRateLimiter rateLimiter;
		private boolean compatibleMode = DEFAULT_COMPATIBLE_MODE;

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
		 * See {@link JdbcDialect#defaultDriverName}.
		 */
		public Builder setDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		/**
		 * optional, JDBC DB url.
		 */
		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		/**
		 * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by
		 * {@link JdbcDialects#get} from DB url.
		 */
		public Builder setDialect(JdbcDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		/**
		 * optional, whether use bytedance mysql.
		 */
		public Builder setUseBytedanceMysql(boolean useBytedanceMysql) {
			this.useBytedanceMysql = useBytedanceMysql;
			return this;
		}

		/**
		 * optional, consul, if url is not set, then it is required.
		 */
		public Builder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		/**
		 * optional, psm for non-authentication, if url is not set, then it is required.
		 */
		public Builder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		/**
		 * optional, name of database, if url is not set, then it is required.
		 */
		public Builder setDbname(String dbname) {
			this.dbname = dbname;
			return this;
		}

		/**
		 * optional, init sql which will be executed when create a db connection.
		 */
		public Builder setInitSql(String initSql) {
			this.initSql = initSql;
			return this;
		}

		/**
		 * optional, set the rate limiter.
		 */
		public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public Builder setCompatibleMode(boolean compatibleMode) {
			this.compatibleMode = compatibleMode;
			return this;
		}

		public JdbcOptions build() {
			checkNotNull(tableName, "No tableName supplied.");
			if (this.dialect == null) {
				Optional<JdbcDialect> optional = JdbcDialects.get(dbURL);
				this.dialect = optional.orElseGet(() -> {
					throw new NullPointerException("Unknown dbURL,can not find proper dialect.");
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
					this.dbURL = String.format(BYTEDANCE_MYSQL_URL_TEMPLATE, dbname, consul, psm, TimeZone.getDefault().getID());
				} else {
					throw new FlinkRuntimeException("Can't init db url, because " +
						"dbUrl == null & useBytedanceMysql is false");
				}
			}

			return new JdbcOptions(
				dbURL,
				tableName,
				driverName,
				username,
				password,
				dialect,
				useBytedanceMysql,
				consul,
				psm,
				dbname,
				initSql,
				rateLimiter,
				compatibleMode);
		}
	}
}
