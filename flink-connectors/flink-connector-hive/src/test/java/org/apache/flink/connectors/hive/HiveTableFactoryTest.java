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

package org.apache.flink.connectors.hive;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.read.HiveContinuousMonitoringNewestPartitionFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link HiveTableFactory}.
 */
public class HiveTableFactoryTest {
	private static HiveCatalog catalog;

	@BeforeClass
	public static void init() {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	@AfterClass
	public static void close() {
		catalog.close();
	}

	@Test
	public void testGenericTable() throws Exception {
		TableSchema schema = TableSchema.builder()
			.field("name", DataTypes.STRING())
			.field("age", DataTypes.INT())
			.build();

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogConfig.IS_GENERIC, String.valueOf(true));
		properties.put("connector", "COLLECTION");

		catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
		ObjectPath path = new ObjectPath("mydb", "mytable");
		CatalogTable table = new CatalogTableImpl(schema, properties, "csv table");
		catalog.createTable(path, table, true);
		Optional<TableFactory> opt = catalog.getTableFactory();
		assertTrue(opt.isPresent());
		HiveTableFactory tableFactory = (HiveTableFactory) opt.get();
		TableSource tableSource = tableFactory.createTableSource(new TableSourceFactoryContextImpl(
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"), table, new Configuration()));
		assertTrue(tableSource instanceof StreamTableSource);
		TableSink tableSink = tableFactory.createTableSink(new TableSinkFactoryContextImpl(
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
				table,
				new Configuration(),
				true));
		assertTrue(tableSink instanceof StreamTableSink);
	}

	@Test
	public void testHiveTable() throws Exception {
		TableSchema schema = TableSchema.builder()
			.field("name", DataTypes.STRING())
			.field("age", DataTypes.INT())
			.build();

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogConfig.IS_GENERIC, String.valueOf(false));

		catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
		ObjectPath path = new ObjectPath("mydb", "mytable");
		CatalogTable table = new CatalogTableImpl(schema, properties, "hive table");
		catalog.createTable(path, table, true);
		Optional<TableFactory> opt = catalog.getTableFactory();
		assertTrue(opt.isPresent());
		HiveTableFactory tableFactory = (HiveTableFactory) opt.get();
		TableSink tableSink = tableFactory.createTableSink(new TableSinkFactoryContextImpl(
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
				table,
				new Configuration(),
				true));
		assertTrue(tableSink instanceof HiveTableSink);
		TableSource tableSource = tableFactory.createTableSource(new TableSourceFactoryContextImpl(
				ObjectIdentifier.of("mycatalog", "mydb", "mytable"), table, new Configuration()));
		assertTrue(tableSource instanceof HiveTableSource);
	}

	// test for obtaining correct hive partition filter string when continuously monitoring newest partition
	@Test(timeout = 30000)
	public void testGetPartitionFilterStr() throws Exception {
		Tuple2<String, String> datePartition = new Tuple2<>("p_date", "yyyyMMdd");
		Tuple2<String, String> hourPartition = new Tuple2<>("hour", "HH");
		String partitionFilter = "level='City' or level='Province'";
		Tuple2<Integer, Integer> partitionPendingRange = new Tuple2<>(3, 10);
		int lowerBounds = -partitionPendingRange.f1;
		int probeLevel = Calendar.HOUR;

		// use current date to construct partition filter string
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePartition.f1 + " " + hourPartition.f1);
		Date currentDate = new Date();

		// make sure the date format satisfy the specific format
		Date newestPartitionDate = dateFormat.parse(dateFormat.format(currentDate));

		// the return result would be like
		// (p_date='20221026' and hour>='00') or ('20221026'<p_date and p_date<'20221026') or (p_date='20221026' and hour<='13') and (level='City' or level='Province')
		String result = HiveContinuousMonitoringNewestPartitionFunction.getPartitionFilterStr(
			datePartition,
			hourPartition,
			partitionFilter,
			partitionPendingRange,
			lowerBounds,
			probeLevel,
			newestPartitionDate,
			cal,
			dateFormat);

		// construct the expected filter string
		String expected = "";

		int range = lowerBounds;
		cal.setTime(newestPartitionDate);
		cal.add(probeLevel, range);

		// lower bound filter condition
		String probePartitionDateStr = dateFormat.format(cal.getTime());
		expected += "(" + datePartition.f0 + "='" + probePartitionDateStr.split(" ")[0] + "'";
		expected += " and " + hourPartition.f0 + ">='" + probePartitionDateStr.split(" ")[1] + "'";
		expected += ") or ('" + probePartitionDateStr.split(" ")[0] + "'<" + datePartition.f0;

		range += partitionPendingRange.f0 - lowerBounds;
		cal.setTime(newestPartitionDate);
		cal.add(probeLevel, range);
		probePartitionDateStr = dateFormat.format(cal.getTime());

		// upper bound filter condition
		expected += " and " + datePartition.f0 + "<'" + probePartitionDateStr.split(" ")[0] + "')";
		expected += " or (" + datePartition.f0 + "='" + probePartitionDateStr.split(" ")[0] + "'";
		expected += " and " + hourPartition.f0 + "<='" + probePartitionDateStr.split(" ")[1] + "'";
		expected += ")";

		// other filter condition
		expected += " and (" + partitionFilter + ")";

		assertEquals(expected, result);
	}

}
