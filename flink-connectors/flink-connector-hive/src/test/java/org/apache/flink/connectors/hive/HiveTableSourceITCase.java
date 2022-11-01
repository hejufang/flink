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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.hive.read.HiveTableInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.runtime.utils.TestingAppendRowDataSink;
import org.apache.flink.table.planner.runtime.utils.TestingAppendSink;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.PipelineOptions.USE_MAX_SOURCE_PARALLELISM_AS_DEFAULT_PARALLELISM;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_EXEC_SUPPORT_HIVE_BUCKET;
import static org.apache.flink.table.catalog.hive.HiveTestUtils.createTableEnvWithHiveCatalog;
import static org.apache.flink.table.catalog.hive.HiveTestUtils.waitForJobFinish;
import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests {@link HiveTableSource}.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class HiveTableSourceITCase extends BatchAbstractTestBase {

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	private static HiveCatalog hiveCatalog;
	private static HiveConf hiveConf;

	@BeforeClass
	public static void createCatalog() throws IOException {
		hiveConf = hiveShell.getHiveConf();
		hiveCatalog = HiveTestUtils.createHiveCatalog(hiveConf);
		hiveCatalog.open();
	}

	@AfterClass
	public static void closeCatalog() {
		if (null != hiveCatalog) {
			hiveCatalog.close();
		}
	}

	@Before
	public void setupSourceDatabaseAndData() {
		hiveShell.execute("CREATE DATABASE IF NOT EXISTS source_db");
	}

	@Test
	public void testReadNonPartitionedTable() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test";
		TableEnvironment tEnv = createTableEnv();
		tEnv.executeSql("CREATE TABLE source_db.test ( a INT, b INT, c STRING, d BIGINT, e DOUBLE)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[] { 1, 1, "a", 1000L, 1.11 })
				.addRow(new Object[] { 2, 2, "b", 2000L, 2.22 })
				.addRow(new Object[] { 3, 3, "c", 3000L, 3.33 })
				.addRow(new Object[] { 4, 4, "d", 4000L, 4.44 })
				.commit();

		Table src = tEnv.sqlQuery("select * from hive.source_db.test");
		List<Row> rows = Lists.newArrayList(src.execute().collect());

		Assert.assertEquals(4, rows.size());
		Assert.assertEquals("1,1,a,1000,1.11", rows.get(0).toString());
		Assert.assertEquals("2,2,b,2000,2.22", rows.get(1).toString());
		Assert.assertEquals("3,3,c,3000,3.33", rows.get(2).toString());
		Assert.assertEquals("4,4,d,4000,4.44", rows.get(3).toString());
	}

	@Test
	public void testReadComplexDataType() throws Exception {
		final String dbName = "source_db";
		final String tblName = "complex_test";
		TableEnvironment tEnv = createTableEnv();
		tEnv.executeSql("create table source_db.complex_test(" +
						"a array<int>, m map<int,string>, s struct<f1:int,f2:bigint>)");
		Integer[] array = new Integer[]{1, 2, 3};
		Map<Integer, String> map = new LinkedHashMap<>();
		map.put(1, "a");
		map.put(2, "b");
		Object[] struct = new Object[]{3, 3L};
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{array, map, struct})
				.commit();
		Table src = tEnv.sqlQuery("select * from hive.source_db.complex_test");
		List<Row> rows = Lists.newArrayList(src.execute().collect());
		Assert.assertEquals(1, rows.size());
		assertArrayEquals(array, (Integer[]) rows.get(0).getField(0));
		assertEquals(map, rows.get(0).getField(1));
		assertEquals(Row.of(struct[0], struct[1]), rows.get(0).getField(2));
	}

	/**
	 * Test to read from partition table.
	 * @throws Exception
	 */
	@Test
	public void testReadPartitionTable() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test_table_pt";
		TableEnvironment tEnv = createTableEnv();
		tEnv.executeSql("CREATE TABLE source_db.test_table_pt " +
						"(`year` STRING, `value` INT) partitioned by (pt int)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2014", 3})
				.addRow(new Object[]{"2014", 4})
				.commit("pt=0");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2015", 2})
				.addRow(new Object[]{"2015", 5})
				.commit("pt=1");
		Table src = tEnv.sqlQuery("select * from hive.source_db.test_table_pt");
		List<Row> rows = Lists.newArrayList(src.execute().collect());

		assertEquals(4, rows.size());
		Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
		assertArrayEquals(new String[]{"2014,3,0", "2014,4,0", "2015,2,1", "2015,5,1"}, rowStrings);
	}

	@Test
	public void testReadPartitionBucketTable() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test_table_pt";
		TableEnvironment tEnv = createTableEnv();
		tEnv.getConfig().getConfiguration().setBoolean(USE_MAX_SOURCE_PARALLELISM_AS_DEFAULT_PARALLELISM, true);
		tEnv.getConfig().getConfiguration().setBoolean(TABLE_EXEC_SUPPORT_HIVE_BUCKET, true);
		List<String> cols = Arrays.asList("value");
		TestHiveCatalog testHiveCatalog = (TestHiveCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get();
		testHiveCatalog.addBucketInfo("source_db.test_table_pt", 1, cols, cols);
		tEnv.executeSql("CREATE TABLE source_db.test_table_pt " +
			"(`year` bigint, `value` INT) partitioned by (pt int)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
			.addRow(new Object[]{"2014", 3})
			.addRow(new Object[]{"2014", 4})
			.commit("pt=0");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
			.addRow(new Object[]{"2015", 2})
			.addRow(new Object[]{"2015", 5})
			.commit("pt=1");
		Table src = tEnv.sqlQuery("select * from hive.source_db.test_table_pt");
		List<Row> rows = Lists.newArrayList(src.execute().collect());

		assertEquals(4, rows.size());
		Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
		assertArrayEquals(new String[]{"2014,3,0", "2014,4,0", "2015,2,1", "2015,5,1"}, rowStrings);
	}

	@Test
	public void testPartitionPrunning() throws Exception {
		final String dbName = "source_db";
		final String tblName = "test_table_pt_1";
		TableEnvironment tEnv = createTableEnv();
		tEnv.executeSql("CREATE TABLE source_db.test_table_pt_1 " +
						"(`year` STRING, `value` INT) partitioned by (pt int)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2014", 3})
				.addRow(new Object[]{"2014", 4})
				.commit("pt=0");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2015", 2})
				.addRow(new Object[]{"2015", 5})
				.commit("pt=1");
		Table src = tEnv.sqlQuery("select * from hive.source_db.test_table_pt_1 where pt = 0");
		// first check execution plan to ensure partition prunning works
		String[] explain = src.explain().split("==.*==\n");
		assertEquals(4, explain.length);
		String optimizedLogicalPlan = explain[2];
		String physicalExecutionPlan = explain[3];
		assertTrue(optimizedLogicalPlan, optimizedLogicalPlan.contains(
				"HiveTableSource(year, value, pt) TablePath: source_db.test_table_pt_1, PartitionPruned: true, PartitionNums: 1, ProjectedFields: [0, 1], Predicates: [equals(pt, 0)]"));
		//This assert will failed because we prune the task name, so we ignore this assert for now.
		//assertTrue(physicalExecutionPlan, physicalExecutionPlan.contains(
		//		"HiveTableSource(year, value, pt) TablePath: source_db.test_table_pt_1, PartitionPruned: true, PartitionNums: 1"));
		// second check execute results
		List<Row> rows = Lists.newArrayList(src.execute().collect());
		assertEquals(2, rows.size());
		Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
		assertArrayEquals(new String[]{"2014,3,0", "2014,4,0"}, rowStrings);
	}

	@Test
	public void testPartitionFilter() throws Exception {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		TestPartitionFilterCatalog catalog = new TestPartitionFilterCatalog(
				hiveCatalog.getName(), hiveCatalog.getDefaultDatabase(), hiveCatalog.getHiveConf(), hiveCatalog.getHiveVersion());
		tableEnv.registerCatalog(catalog.getName(), catalog);
		tableEnv.useCatalog(catalog.getName());
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.part(x int) partitioned by (p1 int,p2 string)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{1}).commit("p1=1,p2='a'");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{2}).commit("p1=2,p2='b'");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{3}).commit("p1=3,p2='c'");
			// TODO: enable hive dialect with partition pruning
			tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
			// test string partition columns with special characters
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{4}).commit("p1=4,p2='c:2'");
			Table query = tableEnv.sqlQuery("select x from db1.part where p1>1 or p2<>'a' order by x");
			String[] explain = query.explain().split("==.*==\n");
			assertFalse(catalog.fallback);
			String optimizedPlan = explain[2];
			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 3"));
			List<Row> results = Lists.newArrayList(query.execute().collect());
			assertEquals("[2, 3, 4]", results.toString());

			query = tableEnv.sqlQuery("select x from db1.part where p1>2 and p2<='a' order by x");
			explain = query.explain().split("==.*==\n");
			assertFalse(catalog.fallback);
			optimizedPlan = explain[2];
			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 0"));
			results = Lists.newArrayList(query.execute().collect());
			assertEquals("[]", results.toString());

			query = tableEnv.sqlQuery("select x from db1.part where p2 = 'c:2' order by x");
			explain = query.explain().split("==.*==\n");
			assertFalse(catalog.fallback);
			optimizedPlan = explain[2];
			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 1"));
			results = Lists.newArrayList(query.execute().collect());
			assertEquals("[4]", results.toString());

			query = tableEnv.sqlQuery("select x from db1.part where '' = p2");
			explain = query.explain().split("==.*==\n");
			assertFalse(catalog.fallback);
			optimizedPlan = explain[2];
			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 0"));
			results = Lists.newArrayList(query.execute().collect());
			assertEquals("[]", results.toString());

			//This next two tests somehow failed after we implement FilterableTableSource for HiveTableSource.
			//We disable them for now.
//			query = tableEnv.sqlQuery("select x from db1.part where p1 in (1,3,5) order by x");
//			explain = query.explain().split("==.*==\n");
//			assertFalse(catalog.fallback);
//			optimizedPlan = explain[2];
//			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 2"));
//			results = Lists.newArrayList(query.execute().collect());
//			assertEquals("[1, 3]", results.toString());

//			query = tableEnv.sqlQuery("select x from db1.part where (p1=1 and p2='a') or ((p1=2 and p2='b') or p2='d') order by x");
//			explain = query.explain().split("==.*==\n");
//			assertFalse(catalog.fallback);
//			optimizedPlan = explain[2];
//			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 2"));
//			results = Lists.newArrayList(query.execute().collect());
//			assertEquals("[1, 2]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testPartitionFilterDateTimestamp() throws Exception {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		TestPartitionFilterCatalog catalog = new TestPartitionFilterCatalog(
				hiveCatalog.getName(), hiveCatalog.getDefaultDatabase(), hiveCatalog.getHiveConf(), hiveCatalog.getHiveVersion());
		tableEnv.registerCatalog(catalog.getName(), catalog);
		tableEnv.useCatalog(catalog.getName());
		tableEnv.executeSql("create database db1");
		try {
			tableEnv.executeSql("create table db1.part(x int) partitioned by (p1 date,p2 timestamp)");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{1}).commit("p1='2018-08-08',p2='2018-08-08 08:08:08'");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{2}).commit("p1='2018-08-09',p2='2018-08-08 08:08:09'");
			HiveTestUtils.createTextTableInserter(hiveShell, "db1", "part")
					.addRow(new Object[]{3}).commit("p1='2018-08-10',p2='2018-08-08 08:08:10'");
			// TODO: enable hive dialect with partition pruning
			tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

			Table query = tableEnv.sqlQuery(
					"select x from db1.part where p1>cast('2018-08-09' as date) and p2<>cast('2018-08-08 08:08:09' as timestamp)");
			String[] explain = query.explain().split("==.*==\n");
			assertTrue(catalog.fallback);
			String optimizedPlan = explain[2];
			assertTrue(optimizedPlan, optimizedPlan.contains("PartitionPruned: true, PartitionNums: 1"));
			List<Row> results = Lists.newArrayList(query.execute().collect());
			assertEquals("[3]", results.toString());

			// filter by timestamp partition
			query = tableEnv.sqlQuery("select x from db1.part where timestamp '2018-08-08 08:08:09' = p2");
			results = Lists.newArrayList(query.execute().collect());
			assertEquals("[2]", results.toString());
		} finally {
			tableEnv.executeSql("drop database db1 cascade");
		}
	}

	@Test
	public void testProjectionPushDown() throws Exception {
		TableEnvironment tableEnv = createTableEnv();
		tableEnv.executeSql("create table src(x int,y string) partitioned by (p1 bigint, p2 string)");
		try {
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "src")
					.addRow(new Object[]{1, "a"})
					.addRow(new Object[]{2, "b"})
					.commit("p1=2013, p2='2013'");
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "src")
					.addRow(new Object[]{3, "c"})
					.commit("p1=2014, p2='2014'");
			Table table = tableEnv.sqlQuery("select p1, count(y) from hive.`default`.src group by p1");
			String[] explain = table.explain().split("==.*==\n");
			assertEquals(4, explain.length);
			String logicalPlan = explain[2];
			String physicalPlan = explain[3];
			String expectedExplain =
					"HiveTableSource(x, y, p1, p2) TablePath: default.src, PartitionPruned: false, PartitionNums: null, ProjectedFields: [2, 1]";
			assertTrue(logicalPlan, logicalPlan.contains(expectedExplain));
			//This assert will failed because we prune the task name, so we ignore this assert for now.
			//assertTrue(physicalPlan, physicalPlan.contains(expectedExplain));

			List<Row> rows = Lists.newArrayList(table.execute().collect());
			assertEquals(2, rows.size());
			Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
			assertArrayEquals(new String[]{"2013,2", "2014,1"}, rowStrings);
		} finally {
			tableEnv.executeSql("drop table src");
		}
	}

	@Test
	public void testLimitPushDown() throws Exception {
		TableEnvironment tableEnv = createTableEnv();
		tableEnv.executeSql("create table src (a string)");
		try {
			HiveTestUtils.createTextTableInserter(hiveShell, "default", "src")
						.addRow(new Object[]{"a"})
						.addRow(new Object[]{"b"})
						.addRow(new Object[]{"c"})
						.addRow(new Object[]{"d"})
						.commit();
			//Add this to obtain correct stats of table to avoid FLINK-14965 problem
			hiveShell.execute("analyze table src COMPUTE STATISTICS");
			Table table = tableEnv.sqlQuery("select * from hive.`default`.src limit 1");
			String[] explain = table.explain().split("==.*==\n");
			assertEquals(4, explain.length);
			String logicalPlan = explain[2];
			String physicalPlan = explain[3];
			String expectedExplain = "HiveTableSource(a) TablePath: default.src, PartitionPruned: false, " +
									"PartitionNums: null, LimitPushDown: true, Limit: 1";
			assertTrue(logicalPlan.contains(expectedExplain));
			//This assert will failed because we prune the task name, so we ignore this assert for now.
			//assertTrue(physicalPlan.contains(expectedExplain));

			List<Row> rows = Lists.newArrayList(table.execute().collect());
			assertEquals(1, rows.size());
			Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
			assertArrayEquals(new String[]{"a"}, rowStrings);
		} finally {
			tableEnv.executeSql("drop table src");
		}
	}

	@Test
	public void testParallelismSetting() {
		final String dbName = "source_db";
		final String tblName = "test_parallelism";
		TableEnvironment tEnv = createTableEnv();
		tEnv.executeSql("CREATE TABLE source_db.test_parallelism " +
				"(`year` STRING, `value` INT) partitioned by (pt int)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2014", 3})
				.addRow(new Object[]{"2014", 4})
				.commit("pt=0");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{"2015", 2})
				.addRow(new Object[]{"2015", 5})
				.commit("pt=1");
		Table table = tEnv.sqlQuery("select * from hive.source_db.test_parallelism");
		PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
		RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
		ExecNode execNode = planner.translateToExecNodePlan(toScala(Collections.singletonList(relNode))).get(0);
		@SuppressWarnings("unchecked")
		Transformation transformation = execNode.translateToPlan(planner);
		Assert.assertEquals(2, transformation.getParallelism());
	}

	@Test
	public void testParallelismOnLimitPushDown() {
		final String dbName = "source_db";
		final String tblName = "test_parallelism_limit_pushdown";
		TableEnvironment tEnv = createTableEnv();
		tEnv.getConfig().getConfiguration().setBoolean(
				HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);
		tEnv.getConfig().getConfiguration().setInteger(
				ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
		tEnv.executeSql("CREATE TABLE source_db.test_parallelism_limit_pushdown " +
					"(`year` STRING, `value` INT) partitioned by (pt int)");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
					.addRow(new Object[]{"2014", 3})
					.addRow(new Object[]{"2014", 4})
					.commit("pt=0");
		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
					.addRow(new Object[]{"2015", 2})
					.addRow(new Object[]{"2015", 5})
					.commit("pt=1");
		Table table = tEnv.sqlQuery("select * from hive.source_db.test_parallelism_limit_pushdown limit 1");
		PlannerBase planner = (PlannerBase) ((TableEnvironmentImpl) tEnv).getPlanner();
		RelNode relNode = planner.optimize(TableTestUtil.toRelNode(table));
		ExecNode execNode = planner.translateToExecNodePlan(toScala(Collections.singletonList(relNode))).get(0);
		@SuppressWarnings("unchecked")
		Transformation transformation = execNode.translateToPlan(planner);
		Assert.assertEquals(1, ((PartitionTransformation) ((OneInputTransformation) transformation).getInput())
			.getInput().getParallelism());
	}

	@Test
	public void testSourceConfig() throws Exception {
		// vector reader not available for 1.x and we're not testing orc for 2.0.x
		Assume.assumeTrue(HiveVersionTestUtil.HIVE_210_OR_LATER);
		Map<String, String> env = System.getenv();
		hiveShell.execute("create database db1");
		try {
			hiveShell.execute("create table db1.src (x int,y string) stored as orc");
			hiveShell.execute("insert into db1.src values (1,'a'),(2,'b')");
			testSourceConfig(true, true);
			testSourceConfig(false, false);
		} finally {
			TestBaseUtils.setEnv(env);
			hiveShell.execute("drop database db1 cascade");
		}
	}

	@Test(timeout = 120000)
	public void testStreamPartitionRead() throws Exception {
		final String catalogName = "hive";
		final String dbName = "source_db";
		final String tblName = "stream_test";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(100);
		StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.HIVE);
		tEnv.registerCatalog(catalogName, hiveCatalog);
		tEnv.useCatalog(catalogName);
		tEnv.executeSql("CREATE TABLE source_db.stream_test (" +
				" a INT," +
				" b STRING" +
				") PARTITIONED BY (ts STRING) TBLPROPERTIES (" +
				"'streaming-source.enable'='true'," +
				"'streaming-source.monitor-interval'='1s'," +
				"'streaming-source.consume-order'='partition-time'" +
				")");

		HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{0, "0"})
				.commit("ts='2020-05-06 00:00:00'");

		Table src = tEnv.from("hive.source_db.stream_test");
		TestingAppendRowDataSink sink = new TestingAppendRowDataSink(new RowDataTypeInfo(
				DataTypes.INT().getLogicalType(),
				DataTypes.STRING().getLogicalType(),
				DataTypes.STRING().getLogicalType()));
		DataStream<RowData> out = tEnv.toAppendStream(src, RowData.class);
		out.print(); // add print to see streaming reading
		out.addSink(sink);

		JobClient job = env.executeAsync("job");

		Runnable runnable = () -> {
			for (int i = 1; i < 6; i++) {
				try {
					Thread.sleep(5_000);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
						.addRow(new Object[]{i, String.valueOf(i)})
						.addRow(new Object[]{i, i + "_copy"})
						.commit("ts='2020-05-06 00:" + i + "0:00'");
			}
		};
		Thread thread = new Thread(runnable);
		thread.setDaemon(true);
		thread.start();
		thread.join();
		Thread.sleep(5_000);

		List<String> expected = Arrays.asList(
				"+I(0,0,2020-05-06 00:00:00)",
				"+I(1,1,2020-05-06 00:10:00)",
				"+I(1,1_copy,2020-05-06 00:10:00)",
				"+I(2,2,2020-05-06 00:20:00)",
				"+I(2,2_copy,2020-05-06 00:20:00)",
				"+I(3,3,2020-05-06 00:30:00)",
				"+I(3,3_copy,2020-05-06 00:30:00)",
				"+I(4,4,2020-05-06 00:40:00)",
				"+I(4,4_copy,2020-05-06 00:40:00)",
				"+I(5,5,2020-05-06 00:50:00)",
				"+I(5,5_copy,2020-05-06 00:50:00)"
		);
		List<String> results = sink.getJavaAppendResults();
		results.sort(String::compareTo);
		assertEquals(expected, results);
		job.cancel();
		StreamTestSink.clear();
	}

	@Test(timeout = 120000)
	public void testStreamPartitionReadForBroadcastJoin() throws Exception {

		final String catalogName = "hive";
		final String dbName = "source_db";
		final String tblName = "stream_test";

		EnvironmentSettings streamEnvSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tsEnv = StreamTableEnvironment.create(senv, streamEnvSettings);

		tsEnv.getConfig().getConfiguration().setBoolean(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

		tsEnv.registerCatalog(catalogName, hiveCatalog);
		tsEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tsEnv.useCatalog(catalogName);
		tsEnv.executeSql(
			"CREATE TABLE source_db.stream_test (" +
				" a INT," +
				" b STRING" +
				") PARTITIONED BY (p_date STRING, hour STRING, level STRING) TBLPROPERTIES (" +
				"'streaming-source.enable'='true'," +
				"'scan.input-format-read-interval' = '5s'," +
				"'scan.count-of-scan-times' = '2'," +
				"'scan.hive.date-partition-pattern' = 'p_date=yyyyMMdd'," +
				"'scan.hive.hour-partition-pattern' = 'hour=HH'," +
				"'scan.hive.forward-partition-num' = '2'," +
				"'scan.hive.partition-filter' = \"level='City' or level='Province'\"" +
				")"
		);

		Calendar cal = Calendar.getInstance();
		Date currentDate = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH");
		dateFormat.setLenient(false);

		Date formattedCurrentDate = dateFormat.parse(dateFormat.format(currentDate));
		String formattedDate = null;
		for (int i = -1; i <= 1; i++) {
			cal.setTime(formattedCurrentDate);
			cal.add(Calendar.HOUR, i);
			formattedDate = dateFormat.format(cal.getTime());

			HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
			.addRow(new Object[]{0, "0"})
			.commit("p_date='" + formattedDate.split(" ")[0] + "',hour='" + formattedDate.split(" ")[1] + "',level='City'");

			HiveTestUtils.createTextTableInserter(hiveShell, dbName, tblName)
				.addRow(new Object[]{0, "0"})
				.commit("p_date='" + formattedDate.split(" ")[0] + "',hour='" + formattedDate.split(" ")[1] + "',level='Province'");
		}

		tsEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		tsEnv.useCatalog("default_catalog");
		// simulate the watermark in stream side is 5h ahead of hive side, use timeOffset to adjust the stream side
		long curTime = System.currentTimeMillis() + 18000000L;
		List<Row> testDataList = new ArrayList<>();
		testDataList.addAll(TestData.genIdAndDataListAsJava(0, 2, "A", curTime));
		String sourceId1 = TestValuesTableFactory.registerData(testDataList);

		tsEnv.executeSql(
			"CREATE TABLE A (\n" +
				"    a_id int,\n" +
				"    a_word varchar\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'data-id' = '" + sourceId1 + "',\n" +
				" 'table-source-class'='" + TestData.ScanTableSourceWithTimestamp.class.getName() + "'\n" +
				")"
		);

		Iterator<Row> collected = tsEnv.executeSql(
			"select\n" +
				"/*+ use_broadcast_join('table' = 'stream_test', 'allowLatency' = '1 min', 'maxBuildLatency' = '5 min', 'timeOffset' = '-5h') */\n" +
				"*\n" +
				"from A a left join hive.source_db.stream_test " +
				"h on a.a_id = h.a\n"
		).collect();

		List<String> results = Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = Arrays.asList(
				"0,A0,0,0," + formattedDate.split(" ")[0] + "," + formattedDate.split(" ")[1] + ",City",
				"0,A0,0,0," + formattedDate.split(" ")[0] + "," + formattedDate.split(" ")[1] + ",Province",
				"1,A1,null,null,null,null,null",
				"2,A2,null,null,null,null,null"
		);

		assertEquals(expected, results);
		StreamTestSink.clear();
	}

	@Test(timeout = 30000)
	public void testNonPartitionStreamingSourceWithMapredReader() throws Exception {
		testNonPartitionStreamingSource(true, "test_mapred_reader");
	}

	@Test(timeout = 30000)
	public void testNonPartitionStreamingSourceWithVectorizedReader() throws Exception {
		testNonPartitionStreamingSource(false, "test_vectorized_reader");
	}

	private void testNonPartitionStreamingSource(Boolean useMapredReader, String tblName) throws Exception {
		final String catalogName = "hive";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = HiveTestUtils.createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.HIVE);
		tEnv.getConfig().getConfiguration().setBoolean(
				HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, useMapredReader);
		tEnv.registerCatalog(catalogName, hiveCatalog);
		tEnv.useCatalog(catalogName);
		tEnv.executeSql("CREATE TABLE source_db." + tblName + " (" +
				"  a INT," +
				"  b CHAR(1) " +
				") stored as parquet TBLPROPERTIES (" +
				"  'streaming-source.enable'='true'," +
				"  'streaming-source.monitor-interval'='100ms'" +
				")");

		Table src = tEnv.sqlQuery("select * from hive.source_db." + tblName);

		TestingAppendSink sink = new TestingAppendSink();
		tEnv.toAppendStream(src, Row.class).addSink(sink);
		DataStream<RowData> out = tEnv.toAppendStream(src, RowData.class);
		out.print(); // add print to see streaming reading
		final JobClient jobClient = env.executeAsync();

		Runnable runnable = () -> {
			for (int i = 0; i < 3; ++i) {
				hiveShell.execute("insert into table source_db." + tblName + " values (1,'a'), (2,'b')");
				try {
					Thread.sleep(2_000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		};
		Thread thread = new Thread(runnable);
		thread.setDaemon(true);
		thread.start();
		// Waiting for writing test data to finish
		thread.join();
		// Wait up to 20 seconds for all data to be processed
		for (int i = 0; i < 20; ++i) {
			if (sink.getAppendResults().size() == 6) {
				break;
			} else {
				Thread.sleep(1000);
			}
		}

		// check the result
		List<String> actual = new ArrayList<>(JavaScalaConversionUtil.toJava(sink.getAppendResults()));
		actual.sort(String::compareTo);
		List<String> expected = Arrays.asList("1,a", "1,a", "1,a", "2,b", "2,b", "2,b");
		expected.sort(String::compareTo);
		assertEquals(expected, actual);
		// cancel the job
		jobClient.cancel();
	}

	private void testSourceConfig(boolean fallbackMR, boolean inferParallelism) throws Exception {
		HiveTableFactory tableFactorySpy = spy((HiveTableFactory) hiveCatalog.getTableFactory().get());

		doAnswer(invocation -> {
			TableSourceFactory.Context context = invocation.getArgument(0);
			return new TestConfigSource(
					new JobConf(hiveCatalog.getHiveConf()),
					context.getConfiguration(),
					context.getObjectIdentifier().toObjectPath(),
					context.getTable(),
					fallbackMR,
					inferParallelism);
		}).when(tableFactorySpy).createTableSource(any(TableSourceFactory.Context.class));

		HiveCatalog catalogSpy = spy(hiveCatalog);
		doReturn(Optional.of(tableFactorySpy)).when(catalogSpy).getTableFactory();

		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		tableEnv.getConfig().getConfiguration().setBoolean(
				HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, fallbackMR);
		tableEnv.getConfig().getConfiguration().setBoolean(
				HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, inferParallelism);
		tableEnv.getConfig().getConfiguration().setInteger(
				ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
		tableEnv.registerCatalog(catalogSpy.getName(), catalogSpy);
		tableEnv.useCatalog(catalogSpy.getName());

		List<Row> results = Lists.newArrayList(
				tableEnv.sqlQuery("select * from db1.src order by x").execute().collect());
		assertEquals("[1,a, 2,b]", results.toString());
	}

	@Test
	public void testParquetCaseInsensitive() throws Exception {
		testCaseInsensitive("parquet");
	}

	@Test
	public void testBucketJoin() throws Exception {
		System.out.println(this.getClass().getClassLoader()
			.getResource("qtest/data/bucket").toString());
	}

	private void testCaseInsensitive(String format) throws Exception {
		TableEnvironment tEnv = createTableEnvWithHiveCatalog(hiveCatalog);
		String folderURI = TEMPORARY_FOLDER.newFolder().toURI().toString();

		// Flink to write sensitive fields to parquet file
		tEnv.executeSql(String.format(
				"create table parquet_t (I int, J int) with (" +
						"'connector'='filesystem','format'='%s','path'='%s')",
				format,
				folderURI));
		waitForJobFinish(tEnv.executeSql("insert into parquet_t select 1, 2"));
		tEnv.executeSql("drop table parquet_t");

		// Hive to read parquet file
		tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tEnv.executeSql(String.format(
				"create external table parquet_t (i int, j int) stored as %s location '%s'",
				format,
				folderURI));
		Assert.assertEquals(
				Row.of(1, 2),
				tEnv.executeSql("select * from parquet_t").collect().next());
	}

	private static TableEnvironment createTableEnv() {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode(SqlDialect.HIVE);
		tableEnv.registerCatalog("hive", hiveCatalog);
		tableEnv.useCatalog("hive");
		return tableEnv;
	}

	/**
	 * A sub-class of HiveTableSource to test vector reader switch.
	 */
	private static class TestConfigSource extends HiveTableSource {
		private final boolean fallbackMR;
		private final boolean inferParallelism;

		TestConfigSource(
				JobConf jobConf,
				ReadableConfig flinkConf,
				ObjectPath tablePath,
				CatalogTable catalogTable,
				boolean fallbackMR,
				boolean inferParallelism) {
			super(jobConf, flinkConf, tablePath, catalogTable);
			this.fallbackMR = fallbackMR;
			this.inferParallelism = inferParallelism;
		}

		@Override
		public DataStream<RowData> getDataStream(StreamExecutionEnvironment execEnv) {
			DataStreamSource<RowData> dataStream = (DataStreamSource<RowData>) super.getDataStream(execEnv);
			int parallelism = dataStream.getTransformation().getParallelism();
			assertEquals(inferParallelism ? 1 : 2, parallelism);
			return dataStream;
		}

		@Override
		HiveTableInputFormat getInputFormat(
				List<HiveTablePartition> allHivePartitions,
				boolean useMapRedReader,
				boolean createSplitInParallel,
				boolean useFastGetSplits) {
			assertEquals(useMapRedReader, fallbackMR);
			return super.getInputFormat(allHivePartitions, useMapRedReader, createSplitInParallel, useFastGetSplits);
		}
	}

	// A sub-class of HiveCatalog to test list partitions by filter.
	private static class TestPartitionFilterCatalog extends HiveCatalog {

		private boolean fallback = false;

		TestPartitionFilterCatalog(String catalogName, String defaultDatabase,
				@Nullable HiveConf hiveConf, String hiveVersion) {
			super(catalogName, defaultDatabase, hiveConf, hiveVersion, true, true);
		}

		@Override
		public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
			fallback = true;
			return super.listPartitions(tablePath);
		}
	}
}
