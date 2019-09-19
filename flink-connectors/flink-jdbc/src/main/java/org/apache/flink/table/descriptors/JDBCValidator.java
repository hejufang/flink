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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * The validator for JDBC.
 */
@Internal
public class JDBCValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_JDBC = "jdbc";

	public static final String CONNECTOR_URL = "connector.url";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_DRIVER = "connector.driver";
	public static final String CONNECTOR_DRIVER_DEFAULT = "com.mysql.jdbc.Driver";
	public static final String CONNECTOR_USERNAME = "connector.username";
	public static final String CONNECTOR_PASSWORD = "connector.password";
	public static final String CONNECTOR_USE_BYTEDANCE_MYSQL = "connector.use-bytedance-mysql";
	public static final boolean CONNECTOR_USE_BYTEDANCE_MYSQL_DEFAULT = true;
	public static final String CONNECTOR_CONSUL = "connector.consul";
	public static final String CONNECTOR_PSM = "connector.psm";
	public static final String CONNECTOR_DBNAME = "connector.dbname";
	public static final String CONNECTOR_INIT_SQL = "connector.init-sql";

	public static final String CONNECTOR_READ_PARTITION_COLUMN = "connector.read.partition.column";
	public static final String CONNECTOR_READ_PARTITION_LOWER_BOUND = "connector.read.partition.lower-bound";
	public static final String CONNECTOR_READ_PARTITION_UPPER_BOUND = "connector.read.partition.upper-bound";
	public static final String CONNECTOR_READ_PARTITION_NUM = "connector.read.partition.num";
	public static final String CONNECTOR_READ_FETCH_SIZE = "connector.read.fetch-size";

	public static final String CONNECTOR_LOOKUP_CACHE_MAX_ROWS = "connector.lookup.cache.max-rows";
	public static final String CONNECTOR_LOOKUP_CACHE_TTL = "connector.lookup.cache.ttl";
	public static final String CONNECTOR_LOOKUP_MAX_RETRIES = "connector.lookup.max-retries";

	public static final String CONNECTOR_WRITE_FLUSH_MAX_ROWS = "connector.write.flush.max-rows";
	public static final String CONNECTOR_WRITE_FLUSH_INTERVAL = "connector.write.flush.interval";
	public static final String CONNECTOR_WRITE_MAX_RETRIES = "connector.write.max-retries";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		validateCommonProperties(properties);
		validateReadProperties(properties);
		validateLookupProperties(properties);
		validateSinkProperties(properties);
	}

	private void validateCommonProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_URL, true);
		properties.validateString(CONNECTOR_TABLE, false, 1);
		properties.validateString(CONNECTOR_DRIVER, true);
		properties.validateString(CONNECTOR_USERNAME, true);
		properties.validateString(CONNECTOR_PASSWORD, true);
		properties.validateBoolean(CONNECTOR_USE_BYTEDANCE_MYSQL, true);
		properties.validateString(CONNECTOR_CONSUL, true);
		properties.validateString(CONNECTOR_PSM, true);
		properties.validateString(CONNECTOR_DBNAME, true);
		properties.validateString(CONNECTOR_INIT_SQL, true);

		final Optional<String> url = properties.getOptionalString(CONNECTOR_URL);
		Boolean useBytedanceMySQL =
			properties.getOptionalBoolean(CONNECTOR_USE_BYTEDANCE_MYSQL)
				.orElse(CONNECTOR_USE_BYTEDANCE_MYSQL_DEFAULT);

		if (url.isPresent()) {
			final Optional<JDBCDialect> dialect = JDBCDialects.get(url.get());
			Preconditions.checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + url.get());
		} else {
			Preconditions.checkState(useBytedanceMySQL,
				"connector.use_bytedance_mysql must be true when url is null");
			Preconditions.checkState(properties.getOptionalString(CONNECTOR_CONSUL).isPresent(),
				"connector.consul must be provided when url is null");
			Preconditions.checkState(properties.getOptionalString(CONNECTOR_PSM).isPresent(),
				"connector.psm must be provided when url is null");
			Preconditions.checkState(properties.getOptionalString(CONNECTOR_DBNAME).isPresent(),
				"connector.dbname must be provided when url is null");
			final Optional<JDBCDialect> dialect = JDBCDialects.get("jdbc:mysql:");
			Preconditions.checkState(dialect.isPresent(), "Cannot handle such jdbc url: jdbc:mysql:");
		}

		Optional<String> password = properties.getOptionalString(CONNECTOR_PASSWORD);
		if (password.isPresent()) {
			Preconditions.checkArgument(
				properties.getOptionalString(CONNECTOR_USERNAME).isPresent(),
				"Database username must be provided when database password is provided");
		}
	}

	private void validateReadProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_READ_PARTITION_COLUMN, true);
		properties.validateLong(CONNECTOR_READ_PARTITION_LOWER_BOUND, true);
		properties.validateLong(CONNECTOR_READ_PARTITION_UPPER_BOUND, true);
		properties.validateInt(CONNECTOR_READ_PARTITION_NUM, true);
		properties.validateInt(CONNECTOR_READ_FETCH_SIZE, true);

		Optional<Long> lowerBound = properties.getOptionalLong(CONNECTOR_READ_PARTITION_LOWER_BOUND);
		Optional<Long> upperBound = properties.getOptionalLong(CONNECTOR_READ_PARTITION_UPPER_BOUND);
		if (lowerBound.isPresent() && upperBound.isPresent()) {
			Preconditions.checkArgument(lowerBound.get() <= upperBound.get(),
				CONNECTOR_READ_PARTITION_LOWER_BOUND + " must not be larger than " + CONNECTOR_READ_PARTITION_UPPER_BOUND);
		}

		checkAllOrNone(properties, new String[]{
			CONNECTOR_READ_PARTITION_COLUMN,
			CONNECTOR_READ_PARTITION_LOWER_BOUND,
			CONNECTOR_READ_PARTITION_UPPER_BOUND,
			CONNECTOR_READ_PARTITION_NUM
		});
	}

	private void validateLookupProperties(DescriptorProperties properties) {
		properties.validateLong(CONNECTOR_LOOKUP_CACHE_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_LOOKUP_CACHE_TTL, true, 1);
		properties.validateInt(CONNECTOR_LOOKUP_MAX_RETRIES, true);

		checkAllOrNone(properties, new String[]{
			CONNECTOR_LOOKUP_CACHE_MAX_ROWS,
			CONNECTOR_LOOKUP_CACHE_TTL
		});
	}

	private void validateSinkProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_WRITE_FLUSH_INTERVAL, true, 1);
		properties.validateInt(CONNECTOR_WRITE_MAX_RETRIES, true);
	}

	private void checkAllOrNone(DescriptorProperties properties, String[] propertyNames) {
		int presentCount = 0;
		for (String name : propertyNames) {
			if (properties.getOptionalString(name).isPresent()) {
				presentCount++;
			}
		}
		Preconditions.checkArgument(presentCount == 0 || presentCount == propertyNames.length,
			"Either all or none of the following properties should be provided:\n" + String.join("\n", propertyNames));
	}
}
