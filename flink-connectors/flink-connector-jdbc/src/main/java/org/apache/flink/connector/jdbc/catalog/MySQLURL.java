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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.byted.infsec.client.InfSecException;
import org.byted.infsec.client.SecTokenC;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Parse URL for MySQL.
 * For the purpose of easily validate and configure base-url, database-name and params for {@link MySQLCatalog}.
 * Meet the {@link AbstractJdbcCatalog}'s url validation requirement and pass db params in the meanwhile.
 * Besides, it is compatible with both common MySQL and bytedance MySQL.
 */
public class MySQLURL {

	private static final String JDBC_PREFIX = "jdbc:";
	private static final int JDBC_PREFIX_LEN = JDBC_PREFIX.length();
	private static final String MYSQL_PREFIX = "mysql:";
	private static final int MYSQL_PREFIX_LEN = MYSQL_PREFIX.length();
	private static final String PROTOCOL_SEP = "//";
	private static final String PATH_SEP = "/";
	private static final String PARAM_SEP = "?";
	private static final int PROTOCOL_LEN = JDBC_PREFIX_LEN + MYSQL_PREFIX_LEN + PROTOCOL_SEP.length();

	private final String baseUrl;
	private final String defaultDbName;
	private final String params;

	private MySQLURL(String baseUrl, String defaultDbName, String params) {
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(baseUrl));

		this.baseUrl = baseUrl;
		this.defaultDbName = defaultDbName;
		this.params = params;
	}

	public String getUrl(String databaseName) {
		if (databaseName == null) {
			return this.baseUrl + this.params;
		}

		return this.baseUrl + databaseName + this.params;
	}

	public String getDefaultUrl() {
		return this.getUrl(this.defaultDbName);
	}

	public String getBaseUrl() {
		return this.baseUrl;
	}

	@Override
	public String toString() {
		return this.getDefaultUrl();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof MySQLURL)) {
			return false;
		}

		MySQLURL that = (MySQLURL) o;
		return Objects.equals(this.baseUrl, that.baseUrl)
			&& Objects.equals(this.defaultDbName, that.defaultDbName)
			&& Objects.equals(this.params, that.params);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.baseUrl, this.defaultDbName, this.params);
	}

	private static final String PARAM_CATALOG_CONF_KEY = "catalog_conf";
	// use ',' to separate different configurations
	private static final String PARAM_CATALOG_CONF_SEP = ",";
	private static final String PARAM_CATALOG_CONF_READ_SOURCE = "READ_SOURCE";

	/**
	 * In aPaaS federated query, use SQL hint to realize the capability that SQL cannot achieve.
	 * The flink-sql-gateway will parse SQL hint, then pass it by
	 * {@link TableConfig#addJobParameter} in {@link TableEnvironment}.
	 * The {@link JdbcDynamicTableFactory#createDynamicTableSource}
	 * will take out the SQL hint and act on the URL by this function.
	 *
	 * <p>The jobParameters as a map contains a key called "catalog_conf" which
	 * is a SQL Hint used to affect the catalog behavior.
	 * the value of "catalog_conf" is a JSON string. It contains the catalog name and
	 * corresponding multi configuration items separated by ','.
	 * Here is an example as follow:
	 * '"catalog_conf": "{\"{catalog_1}\":\"{conf}\",\"{catalog_2}\":\"{conf_1},{...},{conf_n}\"}"'
	 *
	 * <p>For now, only support "READ_MASTER" or "READ_REPLICA" for every single catalog.
	 * "READ_SOURCE" helps read the RDS source database by modifying the url.
	 * "READ_REPLICA" is the default behavior. Read the RDS replica database.
	 *
	 * @param catalogName Catalog name for current {@link CatalogBaseTable}.
	 * @param url Comply with the MySQL connection protocol.
	 * @param jobParameters A map with various parameters, here contains SQL hint K-Vs.
	 * @return New URL decorated with jobParameters.
	 *
	 * @see <a href="https://bytedance.feishu.cn/docx/doxcnO5LsrYIqvorlRxw1vsAGQE">
	 * 	     More about aPaaS federated query and SQL hint</a>
	 * @see <a href="https://bytedance.feishu.cn/docx/doxcn2KPHPxKYgXO0gkRdiqI68g">
	 * 	     More about READ_SOURCE/READ_REPLICA Feature (see 3.3.1) </a>
	 */
	public static String fromJobParameters(String catalogName, String url, Optional<Map<String, String>> jobParameters) {

		if (!url.startsWith(JDBC_PREFIX + MYSQL_PREFIX + PROTOCOL_SEP)) {
			return url;
		}

		if (jobParameters != null && jobParameters.isPresent()) {
			String configs = jobParameters.get().get(PARAM_CATALOG_CONF_KEY);

			if (!StringUtils.isNullOrWhitespaceOnly(configs)) {
				Map<String, String> confMap = null;
				try {
					confMap = new ObjectMapper()
						.readValue(configs, new TypeReference<Map<String, String>>(){});
				} catch (JsonProcessingException e) {
					throw new RuntimeException(
						String.format("Failed to unmarshal json %s to map", configs), e);
				}

				if (confMap != null && confMap.containsKey(catalogName)) {
					Set<String> confSet = Arrays.stream(confMap.get(catalogName)
						.split(PARAM_CATALOG_CONF_SEP))
						.collect(Collectors.toSet());

					// use READ_SOURCE to connect to the bytedance source consul
					if (confSet.contains(PARAM_CATALOG_CONF_READ_SOURCE)) {
						return new MySQLURL.Builder(url)
							.setReadSource()
							.build()
							.getDefaultUrl();
					}
				}
			}
		}

		// by default, connect to the bytedance replica consul
		return new MySQLURL.Builder(url)
			.setReadReplica()
			.build()
			.getDefaultUrl();
	}

	/**
	 * Builder of {@link MySQLURL}.
	 */
	public static class Builder {
		// rawUrl ex: jdbc:mysql://localhost:3306/db?k1=v1&k2=v2
		private String baseUrl; // ex: jdbc:mysql://localhost:3306/
		private String defaultDbName; // ex: db
		private HashMap<String, String> paramMap = new HashMap<>(); // ex: k1: v1 k2: v2

		private String toParamString(HashMap<String, String> paramMap) {
			if (paramMap == null || paramMap.isEmpty()) {
				return "";
			}

			StringJoiner joiner = new StringJoiner("&");
			paramMap.forEach(
				(k, v) -> joiner.add(k + "=" + v)
			);

			return PARAM_SEP + joiner.toString();
		}

		private void parseUrl(String rawUrl) {
			String trimmedUrl = rawUrl.trim();
			if (trimmedUrl.length() >= PROTOCOL_LEN
				&& trimmedUrl.substring(0, PROTOCOL_LEN).startsWith(JDBC_PREFIX + MYSQL_PREFIX + PROTOCOL_SEP)) {

				int paramsIndex = trimmedUrl.indexOf(PARAM_SEP);

				if (trimmedUrl.substring(PROTOCOL_LEN).contains(PATH_SEP)) {
					int dbIndex = trimmedUrl.substring(PROTOCOL_LEN).indexOf(PATH_SEP) + PROTOCOL_LEN;

					this.baseUrl = trimmedUrl.substring(0, dbIndex);

					if (dbIndex < paramsIndex) {
						this.defaultDbName = trimmedUrl.substring(dbIndex + 1, paramsIndex);
					} else {
						this.defaultDbName = trimmedUrl.substring(dbIndex + 1);
					}
				} else if (paramsIndex != -1) {
					this.baseUrl = trimmedUrl.substring(0, paramsIndex);
					this.defaultDbName = "";
				} else {
					this.baseUrl = rawUrl;
					this.defaultDbName = "";
				}
				this.baseUrl = this.baseUrl.endsWith(PATH_SEP) && !this.baseUrl.endsWith(PROTOCOL_SEP)
					? this.baseUrl
					: this.baseUrl + PATH_SEP;

				HashMap<String, String> params = new HashMap<>();

				if (paramsIndex != -1) {
					int i = paramsIndex + 1;

					while (i < trimmedUrl.length()) {
						int keyStart;
						for (keyStart = i; i < trimmedUrl.length() && trimmedUrl.charAt(i) != '='; ++i) {}

						String key = trimmedUrl.substring(keyStart, i);
						++i;

						int valueStart;
						for (valueStart = i; i < trimmedUrl.length() && trimmedUrl.charAt(i) != '&'; ++i) {}

						String value = trimmedUrl.substring(valueStart, i);
						++i;

						params.put(key, value);
					}
				}

				this.paramMap = params;
			}
		}

		public Builder(String rawUrl) {
			checkArgument(!StringUtils.isNullOrWhitespaceOnly(rawUrl));

			parseUrl(rawUrl);

			validateUrl();
			setDefaultParams();
		}

		public Builder(String rawUrl, Boolean useBytedanceMysql) {
			this(rawUrl);

			if (useBytedanceMysql) {
				validateBytedanceUrl();
				setBytedanceParams();
			}
		}

		private void setDefaultParams() {
			// Enable cursor-based streaming by default
			setUseCursorFetch();
		}

		private void validateUrl() {
			checkArgument(!StringUtils.isNullOrWhitespaceOnly(this.baseUrl));
		}

		private void setBytedanceParams() {
			setAuthInfo();
			setReadReplica();
		}

		private void validateBytedanceUrl() {
			checkArgument(
				this.paramMap.containsKey("db_consul_w")
				&& this.paramMap.containsKey("db_consul_r"));
		}

		public Builder setDefaultDb(String dbName) {
			this.defaultDbName = dbName;
			return this;
		}

		/**
		 * Enable cursor-based streaming to retrieve a set number of rows each time.
		 *
		 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-implementation-notes.html">
		 * 	     More information please see this link</a>
		 *
		 * @return The builder itself.
		 */
		public Builder setUseCursorFetch() {
			this.paramMap.put("useCursorFetch", "true");
			return this;
		}

		public Builder setReadSource() {
			if (this.paramMap.containsKey("db_consul_w")) {
				this.paramMap.put("db_consul", this.paramMap.get("db_consul_w"));
			}
			return this;
		}

		public Builder setReadReplica() {
			if (this.paramMap.containsKey("db_consul_r")) {
				this.paramMap.put("db_consul", this.paramMap.get("db_consul_r"));
			}
			return this;
		}

		public Builder setAuthInfo() {
			try {
				if (System.getenv("TCE_PSM") != null) {
					this.paramMap.put("token", SecTokenC.getToken(true));
					this.paramMap.put("psm", System.getenv("TCE_PSM"));
					this.paramMap.put("auth_enable", "true");
				}
			} catch (InfSecException e) {
				throw new CatalogException(
					String.format("Failed to get token in service %s", System.getenv("TCE_PSM")), e);
			}

			return this;
		}

		public MySQLURL build() {
			return new MySQLURL(
				this.baseUrl,
				this.defaultDbName,
				this.toParamString(paramMap)
			);
		}
	}
}
