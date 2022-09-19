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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link MySQLCatalog}.
 */
public class MySQLCatalogTest extends MySQLCatalogTestBase {
	@Test
	public void testGetDatabase() throws DatabaseNotExistException {
		assertNotEquals(catalog.getDatabase(TEST_DB), null);
		assertNotEquals(catalog.getDatabase(DEFAULT_DB), null);
	}

	@Test
	public void testGetDatabaseDatabaseNotExistException() throws Exception {
		exception.expect(DatabaseNotExistException.class);
		exception.expectMessage("Database nonexistent does not exist in Catalog");
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testOpen() {
		catalog.open();
	}

	@Test
	public void testOpenValidationException() {
		exception.expect(ValidationException.class);

		invalidCatalog.open();
	}

	@Test
	public void testListDatabases() {
		List<String> actual = catalog.listDatabases();

		assertEquals(
			Arrays.asList(DEFAULT_DB, TEST_DB),
			actual
		);
	}

	@Test
	public void testListDatabasesCatalogException() throws DatabaseNotExistException {
		exception.expect(CatalogException.class);

		invalidCatalog.listDatabases();
	}

	@Test
	public void testDatabaseExists() throws Exception {
		assertFalse(catalog.databaseExists("nonexistent"));

		assertTrue(catalog.databaseExists(DEFAULT_DB));
	}

	@Test
	public void testListTables() throws DatabaseNotExistException {
		List<String> actual = catalog.listTables(DEFAULT_DB);

		assertEquals(
			Arrays.asList(
				TEST_TABLE1, TEST_TABLE4, TEST_TABLE5),
			actual);
	}

	@Test
	public void testListTablesDatabaseNotExistException() throws DatabaseNotExistException {
		exception.expect(DatabaseNotExistException.class);
		catalog.listTables("t10");
	}

	@Test
	public void testTableExists() {
		assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, "nonexistent")));

		assertTrue(catalog.tableExists(new ObjectPath(DEFAULT_DB, TEST_TABLE1)));
		assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, TEST_TABLE2)));
	}

	@Test
	public void testGetTablesTableNotExistException() throws TableNotExistException {
		exception.expect(TableNotExistException.class);
		catalog.getTable(new ObjectPath(TEST_DB, "nonexistent"));
	}

	@Test
	public void testGetTablesCatalogException() throws TableNotExistException {
		exception.expect(CatalogException.class);
		catalog.getTable(new ObjectPath("nonexistdb", "anytable"));
	}

	@Test
	public void testGetTable() throws org.apache.flink.table.catalog.exceptions.TableNotExistException {
		TableSchema simpleSchema = getSimpleTable().schema;
		CatalogBaseTable simpleTable = catalog.getTable(new ObjectPath(DEFAULT_DB, TEST_TABLE1));
		assertEquals(simpleSchema, simpleTable.getSchema());

		TableSchema numericSchema = getNumericTable().schema;
		CatalogBaseTable numericTable = catalog.getTable(new ObjectPath(TEST_DB, TEST_NUMERIC_TABLE));
		assertEquals(numericSchema, numericTable.getSchema());

		TableSchema charSchema = getCharTable().schema;
		CatalogBaseTable charTable = catalog.getTable(new ObjectPath(TEST_DB, TEST_CHAR_TABLE));
		assertEquals(charSchema, charTable.getSchema());

		TableSchema timeSchema = getTimeTable().schema;
		CatalogBaseTable timeTable = catalog.getTable(new ObjectPath(TEST_DB, TEST_TIME_TABLE));
		assertEquals(timeSchema, timeTable.getSchema());
	}

	@Test
	public void testGetTableCatalogException() throws TableNotExistException {
		exception.expect(CatalogException.class);

		catalog.getTable(new ObjectPath(TEST_DB, TEST_UNSUPPORTED_TABLE));
	}
}
