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

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.dialect.MySQLDialect;

/**
 * Validator for {@link JdbcCatalog}.
 */
public class JdbcCatalogValidator extends CatalogDescriptorValidator {

	public static final String CATALOG_TYPE_VALUE_JDBC = "jdbc";

	public static final String CATALOG_JDBC_USERNAME = "username";
	public static final String CATALOG_JDBC_PASSWORD = "password";
	public static final String CATALOG_JDBC_BASE_URL = "base-url";
	public static final String CATALOG_JDBC_FETCH_SIZE = "fetch-size";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC, false);
		properties.validateString(CATALOG_DEFAULT_DATABASE, false, 1);
		properties.validateInt(CATALOG_JDBC_FETCH_SIZE, true);
		properties.validateString(CATALOG_JDBC_BASE_URL, false, 1);

		JdbcDialect dialect = null;
		if (JdbcDialects.get(properties.getString(CATALOG_JDBC_BASE_URL)).isPresent()) {
			dialect = JdbcDialects.get(properties.getString(CATALOG_JDBC_BASE_URL)).get();
		}
		if (dialect instanceof MySQLDialect) {
			properties.validateString(CATALOG_JDBC_USERNAME, true, 1);
			properties.validateString(CATALOG_JDBC_PASSWORD, true, 1);
		} else {
			properties.validateString(CATALOG_JDBC_USERNAME, false, 1);
			properties.validateString(CATALOG_JDBC_PASSWORD, false, 1);
		}
	}
}
