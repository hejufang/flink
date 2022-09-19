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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import com.bytedance.mysql.MysqlDriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.CATALOG_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USE_BYTEDANCE_MYSQL;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Catalog for MySQL.
 */
public class MySQLCatalog extends AbstractJdbcCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

	private final String username;
	private final String pwd;
	private final String fetchSize;
	private final MySQLURL myUrl;
	private final Boolean useBytedanceMysql;

	protected MySQLCatalog(
			String catalogName,
			String defaultDatabase,
			String username,
			String pwd,
			String fetchSize,
			MySQLURL myUrl) {
		super(catalogName, defaultDatabase, username, pwd, myUrl.getBaseUrl());

		this.username = username;
		this.pwd = pwd;
		this.fetchSize = fetchSize;
		this.myUrl = myUrl;
		this.useBytedanceMysql = (username == null && pwd == null);
	}

	private static final Set<String> BUILTIN_DATABASES = new HashSet<String>() {{
		add("information_schema");
		add("mysql");
		add("performance_schema");
		add("sys");
	}};

	private Connection getConnection(String databaseName) throws SQLException {
		if (this.useBytedanceMysql) {
			return MysqlDriverManager.getConnection(this.myUrl.getUrl(databaseName));
		}

		return MysqlDriverManager.getConnection(this.myUrl.getUrl(databaseName), this.username, this.pwd);
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConnection("");
			ps = conn.prepareStatement("SHOW DATABASES;");
			rs = ps.executeQuery();
			List<String> databases = new ArrayList<>();
			while (rs.next()) {
				String dbName = rs.getString(1);
				if (!BUILTIN_DATABASES.contains(dbName)) {
					databases.add(dbName);
				}
			}
			return databases;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing database in catalog %s", getName()), e);
		} finally {
			close(conn, ps, rs);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (listDatabases().contains(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConnection(databaseName);
			ps = conn.prepareStatement("SHOW TABLES;");
			rs = ps.executeQuery();
			List<String> tables = new ArrayList<>();
			while (rs.next()) {
				tables.add(rs.getString(1));
			}
			return tables;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing table in catalog %s", getName()), e);
		} finally {
			close(conn, ps, rs);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		Connection conn = null;
		PreparedStatement ps = null;

		try {
			conn = getConnection(tablePath.getDatabaseName());

			DatabaseMetaData metaData = conn.getMetaData();
			Optional<UniqueConstraint> primaryKey = getPrimaryKey(
				metaData,
				"",
				tablePath.getObjectName());

			ps = conn.prepareStatement(
				String.format("SELECT * FROM %s;", tablePath.getObjectName()));
			ResultSetMetaData rsmd = ps.getMetaData();

			String[] names = new String[rsmd.getColumnCount()];
			DataType[] types = new DataType[rsmd.getColumnCount()];
			// in order to extend DESC TABLE statement, add the original datasource type
			String[] rawTypes = new String[rsmd.getColumnCount()];
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				names[i - 1] = rsmd.getColumnName(i);
				types[i - 1] = fromJdbcType(rsmd, i);
				rawTypes[i - 1] = rsmd.getColumnTypeName(i);
				if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
					types[i - 1] = types[i - 1].notNull();
				}
			}

			TableSchema.Builder tableBuilder = new TableSchema.Builder()
				.fields(names, types, rawTypes);
			primaryKey.ifPresent(pk ->
				tableBuilder.primaryKey(pk.getName(), pk.getColumns().toArray(new String[0]))
			);
			TableSchema tableSchema = tableBuilder.build();

			Map<String, String> props = new HashMap<>();
			props.put(CONNECTOR.key(), IDENTIFIER);
			props.put(CATALOG_NAME.key(), this.getName());
			props.put(USE_BYTEDANCE_MYSQL.key(), this.useBytedanceMysql.toString());
			props.put(URL.key(), myUrl.getUrl(tablePath.getDatabaseName()));
			props.put(TABLE_NAME.key(), tablePath.getObjectName());
			// properties can be null here, so check it
			if (this.fetchSize != null) {
				props.put(SCAN_FETCH_SIZE.key(), this.fetchSize);
			}
			if (this.username != null) {
				props.put(USERNAME.key(), this.username);
			}
			if (this.pwd != null) {
				props.put(PASSWORD.key(), this.pwd);
			}

			return new CatalogTableImpl(tableSchema, props, "");
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed getting table %s", tablePath.getFullName()), e);
		} finally {
			close(conn, ps, null);
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = getConnection(tablePath.getDatabaseName());
			ps = conn.prepareStatement(
				String.format("SHOW TABLES LIKE '%s';", tablePath.getObjectName()));
			rs = ps.executeQuery();
			return rs.next() && Objects.equals(rs.getString(1), tablePath.getObjectName());
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed fetching the existence of table %s", tablePath.getFullName()), e);
		} finally {
			close(conn, ps, rs);
		}
	}

	private void close(Connection conn, PreparedStatement ps, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			}
			catch (SQLException e) {
				LOG.error("Failed to close ResultSet", e);
			}
		}

		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				LOG.error("Failed to close PreparedStatement", e);
			}
		}

		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				LOG.error("Failed to close Connection", e);
			}
		}
	}

	// MySQL int types
	public static final String MYSQL_BIT = "BIT";
	public static final String MYSQL_TINYINT = "TINYINT";
	public static final String MYSQL_SMALLINT = "SMALLINT";
	public static final String MYSQL_TINYINT_UNSIGNED = "TINYINT UNSIGNED";
	public static final String MYSQL_INT = "INT";
	public static final String MYSQL_MEDIUMINT = "MEDIUMINT";
	public static final String MYSQL_SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
	public static final String MYSQL_BIGINT = "BIGINT";
	public static final String MYSQL_INT_UNSIGNED = "INT UNSIGNED";
	public static final String MYSQL_BIGINT_UNSIGNED = "BIGINT UNSIGNED";

	// MySQL float types
	public static final String MYSQL_FLOAT = "FLOAT";
	public static final String MYSQL_DOUBLE = "DOUBLE";
	public static final String MYSQL_DECIMAL = "DECIMAL";

	// MySQL time types
	public static final String MYSQL_DATE = "DATE";
	public static final String MYSQL_TIME = "TIME";
	public static final String MYSQL_DATETIME = "DATETIME";
	public static final String MYSQL_TIMESTAMP = "TIMESTAMP";

	// MySQL character types
	public static final String MYSQL_CHAR = "CHAR";
	public static final String MYSQL_VARCHAR = "VARCHAR";
	public static final String MYSQL_TINYTEXT = "TINYTEXT";
	public static final String MYSQL_TEXT = "TEXT";
	public static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
	public static final String MYSQL_LONGTEXT = "LONGTEXT";

	private DataType fromJdbcType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String mysqlType = metadata.getColumnTypeName(colIndex);

		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);

		switch (mysqlType) {
			case MYSQL_BIT:
				if (precision == 1) {
					return DataTypes.TINYINT();
				}
				throw new UnsupportedOperationException("Only support single precision MySQL type 'BIT' now");
			case MYSQL_TINYINT:
				return DataTypes.TINYINT();
			case MYSQL_SMALLINT:
			case MYSQL_TINYINT_UNSIGNED:
				return DataTypes.SMALLINT();
			case MYSQL_INT:
			case MYSQL_MEDIUMINT:
			case MYSQL_SMALLINT_UNSIGNED:
				return DataTypes.INT();
			case MYSQL_BIGINT:
			case MYSQL_INT_UNSIGNED:
				return DataTypes.BIGINT();
			case MYSQL_BIGINT_UNSIGNED:
				return DataTypes.DECIMAL(20, 0);
			case MYSQL_FLOAT:
				if (scale > 0) {
					return DataTypes.DECIMAL(precision, scale);
				}
				return DataTypes.FLOAT();
			case MYSQL_DOUBLE:
				return DataTypes.DOUBLE();
			case MYSQL_DECIMAL:
				return DataTypes.DECIMAL(precision, scale);
			case MYSQL_DATE:
				return DataTypes.DATE();
			case MYSQL_TIME:
				return new AtomicDataType(new TimeType(), LocalTime.class);
			case MYSQL_DATETIME:
				return new AtomicDataType(
					new TimestampType(TimestampType.MAX_PRECISION), LocalDateTime.class);
			case MYSQL_TIMESTAMP:
				return new AtomicDataType(
					new TimestampType(TimestampType.MAX_PRECISION), Timestamp.class);
			case MYSQL_CHAR:
			case MYSQL_VARCHAR:
				return DataTypes.VARCHAR(precision);
			case MYSQL_TINYTEXT:
			case MYSQL_TEXT:
			case MYSQL_MEDIUMTEXT:
			case MYSQL_LONGTEXT:
				return DataTypes.STRING();

			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support MySQL type '%s' yet", mysqlType));
		}
	}

	@Override
	public void open() throws CatalogException {
		String defaultUrl = this.myUrl.getDefaultUrl();

		// test connection, fail early if we cannot connect to database
		try (Connection conn = getConnection(this.getDefaultDatabase())) {

		} catch (SQLException e) {
			throw new ValidationException(
				String.format("Failed connecting to %s via JDBC.", defaultUrl, e));
		}

		LOG.info("Catalog {} established connection to {}", getName(), defaultUrl);
	}
}
