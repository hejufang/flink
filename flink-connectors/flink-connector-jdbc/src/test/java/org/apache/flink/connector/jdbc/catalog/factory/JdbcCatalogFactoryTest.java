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

package org.apache.flink.connector.jdbc.catalog.factory;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.MySQLCatalog;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericCachedCatalog;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.JdbcCatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;

import ch.vorburger.exec.ManagedProcessException;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ASYNC_RELOAD;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_ENABLE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_EXECUTOR_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_MAXIMUM_SIZE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_REFRESH_INTERVAL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_CACHE_TTL;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_BASE_URL;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_FETCH_SIZE;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_PASSWORD;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_JDBC_USERNAME;
import static org.apache.flink.table.descriptors.JdbcCatalogValidator.CATALOG_TYPE_VALUE_JDBC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link JdbcCatalogFactory}.
 */
@Ignore("MariaDB requires libssl 1.0.0 in the env")
public class JdbcCatalogFactoryTest {
	@ClassRule
	public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

	protected static String baseUrl;
	protected static JdbcCatalog catalog;

	protected static final String TEST_CATALOG_NAME = "mypg";
	protected static final String TEST_USERNAME = "postgres";
	protected static final String TEST_PWD = "postgres";

	protected static JdbcCatalog mysqlCatalog;

	protected static final String TEST_MYSQL_CATALOG_NAME = "mysql";
	protected static final String TEST_MYSQL_USERNAME = "root";
	protected static final String TEST_MYSQL_PWD = "root";
	protected static final String DEFAULT_MYSQL_DB = "test";
	protected static final String TEST_MYSQL_BASE_URL = "jdbc:mysql://localhost:33306/";
	protected static final String TEST_FETCH_SIZE = "10000";

	@BeforeClass
	public static void setup() throws SQLException, ManagedProcessException {
		// jdbc:postgresql://localhost:50807/postgres?user=postgres
		String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
		// jdbc:postgresql://localhost:50807/
		baseUrl = embeddedJdbcUrl.substring(0, embeddedJdbcUrl.lastIndexOf("/") + 1);

		catalog = new JdbcCatalog(
			TEST_CATALOG_NAME, PostgresCatalog.DEFAULT_DATABASE, TEST_USERNAME, TEST_PWD, baseUrl, TEST_FETCH_SIZE);

		mysqlCatalog = new JdbcCatalog(
			TEST_MYSQL_CATALOG_NAME,
			DEFAULT_MYSQL_DB,
			TEST_MYSQL_USERNAME,
			TEST_MYSQL_PWD,
			TEST_MYSQL_BASE_URL,
			TEST_FETCH_SIZE
		);
	}

	@Test
	public void testCache() {
		final Map<String, String> mysqlProperties = new HashMap<>();

		mysqlProperties.put(CATALOG_TYPE, "jdbc");
		mysqlProperties.put(CATALOG_PROPERTY_VERSION, "1");

		mysqlProperties.put(CATALOG_DEFAULT_DATABASE, DEFAULT_MYSQL_DB);
		mysqlProperties.put(CATALOG_JDBC_USERNAME, TEST_MYSQL_USERNAME);
		mysqlProperties.put(CATALOG_JDBC_PASSWORD, TEST_MYSQL_PWD);
		mysqlProperties.put(CATALOG_JDBC_BASE_URL, TEST_MYSQL_BASE_URL);

		mysqlProperties.put(CATALOG_CACHE_ENABLE, "true");
		mysqlProperties.put(CATALOG_CACHE_EXECUTOR_SIZE, "2");
		mysqlProperties.put(CATALOG_CACHE_TTL, "10000");
		mysqlProperties.put(CATALOG_CACHE_REFRESH_INTERVAL, "5000");
		mysqlProperties.put(CATALOG_CACHE_ASYNC_RELOAD, "true");
		mysqlProperties.put(CATALOG_CACHE_MAXIMUM_SIZE, "10000");

		final Catalog actualMysqlCatalog = TableFactoryService.find(CatalogFactory.class, mysqlProperties)
			.createCatalog(TEST_MYSQL_CATALOG_NAME, mysqlProperties);

		assertTrue(actualMysqlCatalog instanceof GenericCachedCatalog);
		assertTrue(((GenericCachedCatalog) actualMysqlCatalog).getDelegate() instanceof JdbcCatalog);
	}

	@Test
	public void testPostgres() {
		final CatalogDescriptor catalogDescriptor =
			new JdbcCatalogDescriptor(PostgresCatalog.DEFAULT_DATABASE, TEST_USERNAME, TEST_PWD, baseUrl);

		final Map<String, String> properties = catalogDescriptor.toProperties();

		final Catalog actualCatalog = TableFactoryService.find(CatalogFactory.class, properties)
			.createCatalog(TEST_CATALOG_NAME, properties);

		checkEquals(catalog, (JdbcCatalog) actualCatalog);

		assertTrue(((JdbcCatalog) actualCatalog).getInternal() instanceof PostgresCatalog);
	}

	@Test
	public void testMySQL() {
		final CatalogDescriptor mysqlCatalogDescriptor =
			new JdbcCatalogDescriptor(DEFAULT_MYSQL_DB, TEST_MYSQL_USERNAME, TEST_MYSQL_PWD, TEST_MYSQL_BASE_URL);

		final Map<String, String> mysqlProperties = mysqlCatalogDescriptor.toProperties();

		final Catalog actualMysqlCatalog = TableFactoryService.find(CatalogFactory.class, mysqlProperties)
			.createCatalog(TEST_MYSQL_CATALOG_NAME, mysqlProperties);

		checkEquals(mysqlCatalog, (JdbcCatalog) actualMysqlCatalog);

		assertTrue(((JdbcCatalog) actualMysqlCatalog).getInternal() instanceof MySQLCatalog);
	}

	private static void checkEquals(JdbcCatalog c1, JdbcCatalog c2) {
		assertEquals(c1.getName(), c2.getName());
		assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
		assertEquals(c1.getUsername(), c2.getUsername());
		assertEquals(c1.getPassword(), c2.getPassword());
		assertEquals(c1.getBaseUrl(), c2.getBaseUrl());
	}

	@Test
	public void testRequiredContext() {
		assertEquals(new JdbcCatalogFactory().requiredContext(), new HashMap<String, String>() {{
			put(CATALOG_TYPE, CATALOG_TYPE_VALUE_JDBC);
			put(CATALOG_PROPERTY_VERSION, "1");
		}});
	}

	@Test
	public void testSupportedProperties() {
		assertEquals(new JdbcCatalogFactory().supportedProperties(), Arrays.asList(
			CATALOG_DEFAULT_DATABASE, CATALOG_JDBC_BASE_URL, CATALOG_JDBC_USERNAME, CATALOG_JDBC_PASSWORD,
			CATALOG_JDBC_FETCH_SIZE, CATALOG_CACHE_ENABLE, CATALOG_CACHE_ASYNC_RELOAD, CATALOG_CACHE_EXECUTOR_SIZE,
			CATALOG_CACHE_TTL, CATALOG_CACHE_REFRESH_INTERVAL, CATALOG_CACHE_MAXIMUM_SIZE
			)
		);
	}
}
