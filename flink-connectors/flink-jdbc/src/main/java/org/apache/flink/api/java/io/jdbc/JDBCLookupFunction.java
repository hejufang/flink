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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.metric.LookupMetricUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.io.jdbc.JDBCOptions.VALID_CONNECTION_TIMEOUT_SEC;
import static org.apache.flink.api.java.io.jdbc.JDBCUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JDBCLookupFunction extends TableFunction<Row> {

	private static final Logger LOG = LoggerFactory.getLogger(JDBCLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private final String query;
	private final String drivername;
	private final String dbURL;
	private final String username;
	private final String password;
	private final boolean useBytedanceMysql;
	private final String initSql;
	private final TypeInformation[] keyTypes;
	private final int[] keySqlTypes;
	private final String[] fieldNames;
	private final TypeInformation[] fieldTypes;
	private final int[] outputSqlTypes;
	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final int maxRetryTimes;
	private final JDBCOptions options;
	private final boolean cacheNullValue;

	private transient JDBCConnectionPool connectionPool;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient Cache<Row, List<Row>> cache;
	private transient Meter lookupRequestPerSecond;
	private transient Meter lookupFailurePerSecond;
	private transient Histogram requestDelayMs;

	public JDBCLookupFunction(
			JDBCOptions options, JDBCLookupOptions lookupOptions,
			String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {
		this.options = options;
		this.drivername = options.getDriverName();
		this.dbURL = options.getDbURL();
		this.username = options.getUsername();
		this.password = options.getPassword();
		this.useBytedanceMysql = options.getUseBytedanceMysql();
		this.initSql = options.getInitSql();
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		List<String> nameList = Arrays.asList(fieldNames);
		this.keyTypes = Arrays.stream(keyNames)
				.map(s -> {
					checkArgument(nameList.contains(s),
							"keyName %s can't find in fieldNames %s.", s, nameList);
					return fieldTypes[nameList.indexOf(s)];
				})
				.toArray(TypeInformation[]::new);
		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();
		this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
		this.cacheNullValue = lookupOptions.isCacheNullValue();
		this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();
		this.query = options.getDialect().getSelectFromStatement(
				options.getTableName(), fieldNames, keyNames);
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		try {
			if (options.getConnectionPoolSize() > 0) {
				connectionPool = JDBCConnectionPool.getInstance(options);
			} else {
				dbConn = JDBCUtils.establishConnection(drivername, dbURL, username, password,
					useBytedanceMysql, initSql);
				statement = dbConn.prepareStatement(query);
			}
			this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
					.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
					.maximumSize(cacheMaxSize)
					.recordStats()
					.build();
			if (cache != null) {
				context.getMetricGroup().gauge("hitRate", (Gauge<Double>) () -> cache.stats().hitRate());
			}
			lookupRequestPerSecond = LookupMetricUtils.registerRequestsPerSecond(context.getMetricGroup());
			lookupFailurePerSecond = LookupMetricUtils.registerFailurePerSecond(context.getMetricGroup());
			requestDelayMs = LookupMetricUtils.registerRequestDelayMs(context.getMetricGroup());
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}
	}

	public void eval(Object... keys) {
		Row keyRow = Row.of(keys);
		if (cache != null) {
			List<Row> cachedRows = cache.getIfPresent(keyRow);
			if (cachedRows != null) {
				for (Row cachedRow : cachedRows) {
					collect(cachedRow);
				}
				return;
			}
		}

		if (connectionPool != null) {
			try (JDBCConnectionPool.ConnectionWrapper wrapper = connectionPool.getConnection();
				PreparedStatement tmpStatement = wrapper.getConnection().prepareStatement(query)) {
				doLookup(tmpStatement, keys);
			} catch (InterruptedException | SQLException | ClassNotFoundException e) {
				throw new FlinkRuntimeException("get connection from pool failed.", e);
			}
		} else {
			doLookup(statement, keys);
		}

	}

	private void doLookup(PreparedStatement statement, Object... keys) {
		Row keyRow = Row.of(keys);
		ArrayList<Row> rows = new ArrayList<>();
		for (int retry = 1; retry <= maxRetryTimes; retry++) {
			lookupRequestPerSecond.markEvent();

			try {
				statement.clearParameters();
				for (int i = 0; i < keys.length; i++) {
					JDBCUtils.setField(statement, keySqlTypes[i], keys[i], i);
				}
				long startRequest = System.currentTimeMillis();
				try (ResultSet resultSet = statement.executeQuery()) {
					long requestDelay = System.currentTimeMillis() - startRequest;
					requestDelayMs.update(requestDelay);

					while (resultSet.next()) {
						Row row = convertToRowFromResultSet(resultSet);
						rows.add(row);
					}
					rows.trimToSize();

					if (cache != null) {
						if (!rows.isEmpty() || cacheNullValue) {
							cache.put(keyRow, rows);
						}
					}
				}
				// break instead of return to make sure the result is collected outside this loop
				break;
			} catch (SQLException e) {
				lookupFailurePerSecond.markEvent();

				LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
				if (retry >= maxRetryTimes) {
					throw new RuntimeException("Execution of JDBC statement failed.", e);
				}

				try {
					if (!dbConn.isValid(VALID_CONNECTION_TIMEOUT_SEC)) {
						statement.close();
						dbConn.close();
						dbConn = JDBCUtils.establishConnection(drivername, dbURL, username, password,
							useBytedanceMysql, initSql);
						statement = dbConn.prepareStatement(query);
					}
				} catch (SQLException | ClassNotFoundException exception) {
					LOG.error("JDBC connection is not valid, and reestablish connection failed", exception);
					throw new RuntimeException("Reestablish JDBC connection failed", exception);
				}

				try {
					Thread.sleep(1000 * retry);
				} catch (InterruptedException e1) {
					throw new RuntimeException(e1);
				}
			}
		}

		for (Row row : rows) {
			// should be outside of retry loop.
			// else the chained downstream exception will be caught.
			collect(row);
		}
	}

	private Row convertToRowFromResultSet(ResultSet resultSet) throws SQLException {
		Row row = new Row(outputSqlTypes.length);
		for (int i = 0; i < outputSqlTypes.length; i++) {
			row.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet));
		}
		return row;
	}

	@Override
	public void close() throws IOException {
		if (cache != null) {
			cache.cleanUp();
			cache = null;
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				statement = null;
			}
		}

		if (dbConn != null) {
			try {
				dbConn.close();
			} catch (SQLException se) {
				LOG.info("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				dbConn = null;
			}
		}

		if (connectionPool != null) {
			connectionPool.close();
			connectionPool = null;
		}
	}

	@Override
	public TypeInformation<Row> getResultType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		return keyTypes;
	}

	/**
	 * Builder for a {@link JDBCLookupFunction}.
	 */
	public static class Builder {
		private JDBCOptions options;
		private JDBCLookupOptions lookupOptions;
		private String[] fieldNames;
		private TypeInformation[] fieldTypes;
		private String[] keyNames;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, lookup related options.
		 */
		public Builder setLookupOptions(JDBCLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, field names of this jdbc table.
		 */
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, field types of this jdbc table.
		 */
		public Builder setFieldTypes(TypeInformation[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * required, key names to query this jdbc table.
		 */
		public Builder setKeyNames(String[] keyNames) {
			this.keyNames = keyNames;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCLookupFunction
		 */
		public JDBCLookupFunction build() {
			checkNotNull(options, "No JDBCOptions supplied.");
			if (lookupOptions == null) {
				lookupOptions = JDBCLookupOptions.builder().build();
			}
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");
			checkNotNull(keyNames, "No keyNames supplied.");

			return new JDBCLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
		}
	}
}
