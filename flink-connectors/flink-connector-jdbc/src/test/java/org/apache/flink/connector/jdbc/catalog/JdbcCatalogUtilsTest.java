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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link JdbcCatalogUtils}.
 */
public class JdbcCatalogUtilsTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testJdbcUrl() {
		JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432/");

		JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432");

		JdbcCatalogUtils.validateJdbcUrl("jdbc:mysql://localhost:3306/");

		JdbcCatalogUtils.validateJdbcUrl("jdbc:mysql://localhost:3306");

		JdbcCatalogUtils.validateJdbcUrl("jdbc:mysql://localhost:3306" +
			"?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true");
	}

	@Test
	public void testInvalidJdbcUrl() {
		exception.expect(IllegalArgumentException.class);
		JdbcCatalogUtils.validateJdbcUrl("jdbc:postgresql://localhost:5432/db");

		exception.expect(IllegalArgumentException.class);
		JdbcCatalogUtils.validateJdbcUrl("jdbc:mysql://localhost:3306/db");

		exception.expect(IllegalArgumentException.class);
		JdbcCatalogUtils.validateJdbcUrl("jdbc:mysql://localhost:3306/?useSSL=false");
	}

	@Test
	public void testCreateCatalog() {
		AbstractJdbcCatalog mysqlCatalog = JdbcCatalogUtils.createCatalog(
			"mysql",
			"test",
			"root",
			"root",
			"jdbc:mysql://localhost:3306/",
			"10000");
		AbstractJdbcCatalog postgresCatalog = JdbcCatalogUtils.createCatalog(
			"pg",
			"test",
			"pg",
			"pg",
			"jdbc:postgresql://localhost:5432/",
			"10000");

		assertEquals(mysqlCatalog.getClass(), MySQLCatalog.class);
		assertEquals(postgresCatalog.getClass(), PostgresCatalog.class);
	}

	@Test
	public void testCreateCatalogUnsupportedOperationException() {
		exception.expect(UnsupportedOperationException.class);
		JdbcCatalogUtils.createCatalog(
			"derby",
			"test",
			"test",
			"root",
			"jdbc:derby://localhost",
			"10000");
	}
}
