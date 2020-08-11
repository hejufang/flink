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

package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * JDBC connection options.
 */
@PublicEvolving
public class JdbcConnectionOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	protected final String url;
	protected final String driverName;
	@Nullable
	protected final String username;
	@Nullable
	protected final String password;
	protected final boolean useBytedanceMysql;
	protected final String consul;
	protected final String psm;
	protected final String dbname;
	protected final String initSql;

	protected JdbcConnectionOptions(
			String url,
			String driverName,
			String username,
			String password,
			boolean useBytedanceMysql,
			String consul,
			String psm,
			String dbname,
			String initSql) {
		this.url = Preconditions.checkNotNull(url, "jdbc url is empty");
		this.driverName = Preconditions.checkNotNull(driverName, "driver name is empty");
		this.username = username;
		this.password = password;
		this.useBytedanceMysql = useBytedanceMysql;
		this.consul = consul;
		this.psm = psm;
		this.dbname = dbname;
		this.initSql = initSql;
	}

	public String getDbURL() {
		return url;
	}

	public String getDriverName() {
		return driverName;
	}

	public Optional<String> getUsername() {
		return Optional.ofNullable(username);
	}

	public Optional<String> getPassword() {
		return Optional.ofNullable(password);
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

	public String getInitSql() {
		return initSql;
	}

	/**
	 * Builder for {@link JdbcConnectionOptions}.
	 */
	public static class JdbcConnectionOptionsBuilder {
		private String url;
		private String driverName;
		private String username;
		private String password;
		private boolean useBytedanceMysql;
		private String consul;
		private String psm;
		private String dbname;
		private String initSql;

		public JdbcConnectionOptionsBuilder withUrl(String url) {
			this.url = url;
			return this;
		}

		public JdbcConnectionOptionsBuilder withDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		public JdbcConnectionOptionsBuilder withUsername(String username) {
			this.username = username;
			return this;
		}

		public JdbcConnectionOptionsBuilder withPassword(String password) {
			this.password = password;
			return this;
		}

		public JdbcConnectionOptionsBuilder withUseBytedanceMysql(boolean useBytedanceMysql) {
			this.useBytedanceMysql = useBytedanceMysql;
			return this;
		}

		public JdbcConnectionOptionsBuilder withConsul(String consul) {
			this.consul = consul;
			return this;
		}

		public JdbcConnectionOptionsBuilder withPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public JdbcConnectionOptionsBuilder withDbname(String dbname) {
			this.dbname = dbname;
			return this;
		}

		public JdbcConnectionOptionsBuilder withInitSql(String initSql) {
			this.initSql = initSql;
			return this;
		}

		public JdbcConnectionOptions build() {
			return new JdbcConnectionOptions(
				url,
				driverName,
				username,
				password,
				useBytedanceMysql,
				consul,
				psm,
				dbname,
				initSql);
		}
	}
}
