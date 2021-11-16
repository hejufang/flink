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

package org.apache.flink.connector.bytesql.table.descriptors;

import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.connector.bytesql.table.descriptors.ByteSQLConfigs.DB_CLASS;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common options for ByteSQL connector.
 */
public class ByteSQLOptions implements Serializable {
	private static final long serialVersionUID = 1L;
	private final String consul;
	private final String databaseName;
	private final String tableName;
	private final String username;
	private final String password;
	private final String dbClassName;
	private final long connectionTimeout;
	private final FlinkConnectorRateLimiter rateLimiter;

	private ByteSQLOptions(
			String consul,
			String databaseName,
			String tableName,
			String username,
			String password,
			String dbClassName,
			long connectionTimeout,
			FlinkConnectorRateLimiter rateLimiter) {
		this.consul = consul;
		this.databaseName = databaseName;
		this.tableName = tableName;
		this.username = username;
		this.password = password;
		this.dbClassName = dbClassName;
		this.connectionTimeout = connectionTimeout;
		this.rateLimiter = rateLimiter;
	}

	public String getConsul() {
		return consul;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getDbClassName() {
		return dbClassName;
	}

	public long getConnectionTimeout() {
		return connectionTimeout;
	}

	public FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ByteSQLOptions) {
			ByteSQLOptions options = (ByteSQLOptions) o;
			return Objects.equals(consul, options.consul) &&
				Objects.equals(databaseName, options.databaseName) &&
				Objects.equals(tableName, options.tableName) &&
				Objects.equals(username, options.username) &&
				Objects.equals(password, options.password) &&
				Objects.equals(dbClassName, options.dbClassName) &&
				connectionTimeout == options.connectionTimeout &&
				Objects.equals(rateLimiter, options.rateLimiter);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			consul,
			databaseName,
			tableName,
			username,
			password,
			dbClassName,
			connectionTimeout,
			rateLimiter
		);
	}

	/**
	 * Builder of {@link ByteSQLOptions}.
	 */
	public static class Builder {
		private String consul;
		private String databaseName;
		private String tableName;
		private String username;
		private String password;
		private String dbClassName = DB_CLASS.defaultValue();
		private long connectionTimeout = 2000;
		private FlinkConnectorRateLimiter rateLimiter;

		/**
		 * required, consul name.
		 */
		public Builder setConsul(String consul) {
			this.consul = consul;
			return this;
		}

		/**
		 * required, database name.
		 */
		public Builder setDatabaseName(String databaseName) {
			this.databaseName = databaseName;
			return this;
		}

		/**
		 * required, table name.
		 */
		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * required, user name.
		 */
		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * required, password.
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		public Builder setDbClassName(String dbClassName) {
			this.dbClassName = dbClassName;
			return this;
		}

		/**
		 * optional, set connection timeout, default 2000ms.
		 */
		public Builder setConnectionTimeout(long connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
			return this;
		}

		/**
		 * optional, set rate limiter.
		 */
		public Builder setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
			this.rateLimiter = rateLimiter;
			return this;
		}

		public ByteSQLOptions build() {
			checkNotNull(consul, "No consul supplied.");
			checkNotNull(databaseName, "No databaseName supplied.");
			checkNotNull(tableName, "No tableName supplied.");
			checkNotNull(username, "No username supplied.");
			checkNotNull(password, "No password supplied.");
			return new ByteSQLOptions(
				consul,
				databaseName,
				tableName,
				username,
				password,
				dbClassName,
				connectionTimeout,
				rateLimiter);
		}
	}
}
