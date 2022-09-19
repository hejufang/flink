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

import org.apache.flink.table.api.ValidationException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_USERNAME;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC;

/**
 * Test for {@link JdbcCatalogValidator}.
 */
public class JdbcCatalogValidatorTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testValidate() {
		Map<String, String> properties = new HashMap<String, String>(){{
			put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC);
			put(CATALOG_JDBC_BASE_URL, "jdbc:mysql://localhost/");
			put(CATALOG_JDBC_USERNAME, "root");
			put(CATALOG_JDBC_PASSWORD, "root");
			put(CATALOG_DEFAULT_DATABASE, "db");
		}};

		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new JdbcCatalogValidator().validate(descriptorProperties);
	}

	@Test
	public void testValidateValidationException() {
		exception.expect(ValidationException.class);

		Map<String, String> properties = new HashMap<String, String>(){{
			put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC);
			put(CATALOG_JDBC_BASE_URL, "jdbc:mysql://localhost/");
			put(CATALOG_JDBC_USERNAME, "");
			put(CATALOG_JDBC_PASSWORD, "");
			put(CATALOG_DEFAULT_DATABASE, "db");
		}};

		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);

		new JdbcCatalogValidator().validate(descriptorProperties);
	}
}
