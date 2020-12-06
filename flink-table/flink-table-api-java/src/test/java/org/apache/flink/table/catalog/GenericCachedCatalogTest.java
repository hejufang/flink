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

package org.apache.flink.table.catalog;

import org.apache.flink.table.functions.TestGenericUDF;
import org.apache.flink.table.functions.TestSimpleUDF;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.table.catalog.GenericInMemoryCatalog.DEFAULT_DB;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link GenericCachedCatalog}, mainly reuse the cases in {@link CatalogTest}.
 */
public class GenericCachedCatalogTest extends CatalogTestBase {

	public static final String TEST_CACHED_CATALOG_NAME = "test-cached-catalog";

	private static final AbstractCatalog delegate = new GenericInMemoryCatalog(TEST_CATALOG_NAME);

	@BeforeClass
	public static void init() {
		delegate.open();
		catalog = new GenericCachedCatalog(delegate, TEST_CACHED_CATALOG_NAME, DEFAULT_DB,
			false, 0, Duration.ofSeconds(3), Duration.ofSeconds(2), 100);
		catalog.open();
	}

	@Test
	public void testCacheDataAutoReload() throws Exception {
		delegate.createDatabase(db1, createDb(), false);
		assertTrue(catalog.databaseExists(db1));
		// After we drop db from delegate directly, the cached catalog will still has data cached,
		// but after a refresh interval, the cached catalog will automatic reload this
		delegate.dropDatabase(db1, false);
		assertTrue(catalog.databaseExists(db1));
		Thread.sleep(3000);
		assertFalse(catalog.databaseExists(db1));
	}

	@Test
	public void testAlterCachedData() throws Exception {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		CatalogTestUtil.checkEquals(newDb, delegate.getDatabase(db1));
		CatalogTestUtil.checkEquals(newDb, catalog.getDatabase(db1));
	}

	@Override
	protected boolean isGeneric() {
		return true;
	}

	@Override
	protected CatalogFunction createFunction() {
		return new CatalogFunctionImpl(TestGenericUDF.class.getCanonicalName());
	}

	@Override
	protected CatalogFunction createPythonFunction() {
		return new CatalogFunctionImpl("test.func1", FunctionLanguage.PYTHON);
	}

	@Override
	protected CatalogFunction createAnotherFunction() {
		return new CatalogFunctionImpl(TestSimpleUDF.class.getCanonicalName(), FunctionLanguage.SCALA);
	}
}
