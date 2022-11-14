/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed ON an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.RowDataSchemaCompatibilityResolveStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.CheckpointRecoveryUtils;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import scala.collection.JavaConverters;

import static org.apache.flink.configuration.PipelineOptions.ROW_DATA_SCHEMA_COMPATIBILITY_RESOLVE_STRATEGY;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.table.planner.runtime.utils.CheckpointRecoveryUtils.SOURCE_CREATION_WITHOUT_WATERMARK;
import static org.apache.flink.table.planner.runtime.utils.CheckpointRecoveryUtils.SOURCE_CREATION_WITH_WATERMARK;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

/**
 * Tests for checkpoint verification of SQL jobs.
 */
@RunWith(Parameterized.class)
public class StateSchemaEvolutionCheckpointRecoveryTests {

	private MiniClusterWithClientResource cluster;
	private File checkpointDir;
	private File savepointDir;
	@Parameterized.Parameter
	public String stateBackend;

	@Parameterized.Parameters(name = "stateBackend = {0}")
	public static Collection<String> getStateBackends() {
		return Arrays.asList("rocksdb", "filesystem");
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		TemporaryFolder folder = new TemporaryFolder();
		folder.create();
		final File testRoot = folder.newFolder();

		checkpointDir = new File(testRoot, "checkpoints");
		savepointDir = new File(testRoot, "savepoints");

		if (!checkpointDir.mkdir() || !savepointDir.mkdirs()) {
			fail("Test setup failed: failed to create temporary directories.");
		}
		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.STATE_BACKEND, stateBackend);
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(2)
				.build());
		cluster.before();
	}

	@After
	public void close() {
		if (cluster != null) {
			cluster.after();
		}
	}

	// ------------------------------------------------------------------------
	//  For Unbounded Aggregation
	// ------------------------------------------------------------------------

	private void testAggregate(TableConfig tableConfigs) throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal) as f1,\n" +
				"  MAX(longVal) as f2,\n" +
				"  MAX(stringVal) as f3,\n" + //Will be removed in afterSQL.
				"  COUNT(distinct stringVal) as f4\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  COUNT(distinct longVal) as f6,\n" + //Newly added distinct agg.
				"  COUNT(distinct stringVal) as f4,\n" + //Restored distinct agg.
				"  MAX(longVal) as f2,\n" + //Restored non-distinct agg with MapView.
				"  LAST_VALUE_IGNORE_RETRACT(stringVal) as f5,\n" + //Newly added non-distinct agg.
				"  SUM(doubleVal) as f1\n" + //Restored non-distinct agg with type of argument changed.
				"FROM\n" +
				"  T2\n" +
				"GROUP BY\n" +
				"  intVal";
		testWithSQL(beforeSQL, afterSQL, tableConfigs, "I,UA,UB,D", new String[]{
			"(false,-U(1,0,1,1,null,2.200000047683716))",
			"(false,-U(2,0,1,2,null,1.2000000476837158))",
			"(false,-U(3,0,1,2,null,1.2999999523162842))",
			"(false,-U(4,0,1,3,null,2.4000000953674316))",
			"(true,+U(1,1,1,1,Hi,4.400000047683716))",
			"(true,+U(2,1,1,2,Hello,2.300000047683716))",
			"(true,+U(3,1,1,2,Hello world,3.699999952316284))",
			"(true,+U(4,1,1,3,Hello world,5.7000000953674315))"
		});
	}

	@Test
	public void testNormalAggregateChangeAggregationFunction() throws Exception {
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		testAggregate(tableConfigs);
	}

	@Test
	public void testMiniBatchAggregate() throws Exception {
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		tableConfigs.getConfiguration()
			.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
		testAggregate(tableConfigs);
	}

	@Test
	public void testGlobalAggregate() throws Exception {
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testAggregate(tableConfigs);
	}

	// ------------------------------------------------------------------------
	//  For Window Aggregation
	// ------------------------------------------------------------------------

	@Test
	public void testWindowAggregateChangeAggregationFunction() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal) as f1,\n" +
				"  MAX(longVal) as f2,\n" +
				"  MAX(stringVal) as f3,\n" + //Will be removed in afterSQL.
				"  COUNT(distinct stringVal) as f4\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  COUNT(distinct longVal) as f6,\n" + //Newly added distinct agg.
				"  COUNT(distinct stringVal) as f4,\n" + //Restored distinct agg.
				"  MAX(longVal) as f2,\n" + //Restored non-distinct agg with MapView.
				"  LAST_VALUE_IGNORE_RETRACT(stringVal) as f5,\n" + //Newly added non-distinct agg.
				"  SUM(doubleVal) as f1\n" + //Restored non-distinct agg with type of argument changed.
				"FROM\n" +
				"  T2\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, null, new String[]{
			"(true,+I(1,1,1,1,Hi,4.400000047683716))",
			"(true,+I(2,1,1,2,Hello,2.300000047683716))",
			"(true,+I(3,1,1,2,Hello world,3.699999952316284))",
			"(true,+I(4,1,1,3,Hello world,5.7000000953674315))"
		});
	}

	// ------------------------------------------------------------------------
	//  For Lookup Join
	// ------------------------------------------------------------------------

	/**
	 * As we cannot control the time cost of each run, the output may be different.
	 * Most of the time, the output can be expected.
	 */
	@Ignore
	@Test
	public void testLookupJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  f1,\n" +
				"  f2,\n" +
				"  name\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    longVal as f1,\n" +
				"    longVal + intVal as f2,\n" +
				"    intVal,\n" +
				"    proc\n" +
				"  FROM T1\n" +
				"  ) T\n" +
				"  LEFT JOIN dim for system_time AS of proc AS T3\n" +
				"  ON T.intVal = T3.id";
		String afterSQL =
			"SELECT\n" +
				"  f2,\n" +
				"  f3,\n" +
				"  name\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    CONCAT(stringVal, '!') as f3,\n" +
				"    longVal + intVal as f2,\n" +
				"    intVal,\n" +
				"    proc\n" +
				"  FROM T2\n" +
				"  ) T\n" +
				"  LEFT JOIN dim for system_time AS of proc AS T3\n" +
				"  ON T.intVal = T3.id";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, null, new String[]{
			"(true,+I(2, ,null))",
			"(true,+I(4,  ,null))",
			"(true,+I(5,Hello world!,Test))",
			"(true,+I(7,Hello world!,Test))",
		});
	}

	// ------------------------------------------------------------------------
	//  For Interval Join
	// ------------------------------------------------------------------------

	@Test
	public void testIntervalJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  f1,\n" +
				"  T3.stringVal\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    longVal as f1,\n" +
				"    intVal,\n" +
				"    tsVal\n" +
				"  FROM T1\n" +
				"  ) T\n" +
				"  JOIN T1 AS T3 ON T.intVal = T3.intVal\n" +
				"  and T.tsVal between T3.tsVal - INTERVAL '5' SECOND and T3.tsVal + INTERVAL '5' SECOND";
		String afterSQL =
			"SELECT\n" +
				"  f1,\n" +
				"  f2,\n" +
				"  T3.stringVal\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    stringVal as f2,\n" +
				"    longVal as f1,\n" +
				"    intVal,\n" +
				"    tsVal\n" +
				"  FROM T2\n" +
				"  ) T\n" +
				"  JOIN T2 AS T3 ON T.intVal = T3.intVal\n" +
				"  and T.tsVal between T3.tsVal - INTERVAL '5' SECOND and T3.tsVal + INTERVAL '5' SECOND";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, null, new String[]{
			"(true,+I(1,Hi,Hi))",
			"(true,+I(1,Hi,Hi))",
			"(true,+I(1,null,Hi))",
			"(true,+I(2,Hello world,Hello world))",
			"(true,+I(2,Hello world,Hello world))",
			"(true,+I(2,Hello,Hello))",
			"(true,+I(2,Hello,Hello))",
			"(true,+I(2,null,Hello world))",
			"(true,+I(2,null,Hello))",
			"(true,+I(3,Hello world,Hello world))",
			"(true,+I(3,Hello world,Hello world))",
			"(true,+I(3,null,Hello world))"
		});
	}

	// ------------------------------------------------------------------------
	//  For Equi-Join
	// ------------------------------------------------------------------------

	@Test
	public void testEquiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  f1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    longVal as f1,\n" +
				"    intVal\n" +
				"  FROM T1\n" +
				"  ) T\n" +
				"JOIN T1 AS T2 ON T.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  f2,\n" +
				"  f1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    stringVal as f2,\n" +
				"    longVal as f1,\n" +
				"    intVal\n" +
				"  FROM T2\n" +
				"  ) T\n" +
				"JOIN T2 AS T3 ON T.intVal = T3.intVal\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		if (stateBackend.equals("rocksdb")) {
			thrown.expectCause(containsMessage("The new serializer for a MapState requires state migration " +
				"in order for the job to proceed, since the key schema has changed. " +
				"However, migration for MapState currently only allows value schema evolutions"));
		}
		testWithSQL(beforeSQL, afterSQL, tableConfigs, "I,UA,UB,D", new String[]{
			"(true,+I(Hello world,2))",
			"(true,+I(Hello world,2))",
			"(true,+I(Hello world,3))",
			"(true,+I(Hello world,3))",
			"(true,+I(Hello,2))",
			"(true,+I(Hello,2))",
			"(true,+I(Hi,1))",
			"(true,+I(Hi,1))",
			"(true,+I(null,1))",
			"(true,+I(null,2))",
			"(true,+I(null,2))",
			"(true,+I(null,3))"
		});
	}

	@Test
	public void testMiniBatchEquiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  f1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    longVal as f1,\n" +
				"    intVal\n" +
				"  FROM T1\n" +
				"  ) T\n" +
				"JOIN T1 AS T2 ON T.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  f2,\n" +
				"  f1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    stringVal as f2,\n" +
				"    longVal as f1,\n" +
				"    intVal\n" +
				"  FROM T2\n" +
				"  ) T\n" +
				"JOIN T2 AS T3 ON T.intVal = T3.intVal\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enable-regular-join", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		if (stateBackend.equals("rocksdb")) {
			thrown.expectCause(containsMessage("The new serializer for a MapState requires state migration " +
				"in order for the job to proceed, since the key schema has changed. " +
				"However, migration for MapState currently only allows value schema evolutions"));
		}
		testWithSQL(beforeSQL, afterSQL, tableConfigs, "I,UA,UB,D", new String[]{
			"(true,+I(Hello world,2))",
			"(true,+I(Hello world,2))",
			"(true,+I(Hello world,3))",
			"(true,+I(Hello world,3))",
			"(true,+I(Hello,2))",
			"(true,+I(Hello,2))",
			"(true,+I(Hi,1))",
			"(true,+I(Hi,1))",
			"(true,+I(null,1))",
			"(true,+I(null,2))",
			"(true,+I(null,2))",
			"(true,+I(null,3))"
		});
	}

	@Test
	public void testSemiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  f1 + 1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    longVal as f1\n" +
				"  FROM T1\n" +
				"  ) T\n" +
				"WHERE f1 IN\n" +
				"(\n" +
				"  SELECT\n" +
				"	 CAST(T1.intVal AS BIGINT) as f2\n" +
				"  FROM T1\n" +
				")";
		String afterSQL =
			"SELECT\n" +
				"  f2,\n" +
				"  f1 + 1\n" +
				"FROM\n" +
				"  (\n" +
				"  SELECT\n" +
				"    stringVal as f2,\n" +
				"    longVal as f1\n" +
				"  FROM T2\n" +
				"  ) T\n" +
				"WHERE f1 IN\n" +
				"(\n" +
				"  SELECT\n" +
				"	 CAST(T2.intVal AS BIGINT) as f2\n" +
				"  FROM T2\n" +
				")";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.involve-digest-in-state.enabled", "true");
		if (stateBackend.equals("rocksdb")) {
			thrown.expectCause(containsMessage("The new serializer for a MapState requires state migration " +
				"in order for the job to proceed, since the key schema has changed. " +
				"However, migration for MapState currently only allows value schema evolutions"));
		}
		testWithSQL(beforeSQL, afterSQL, tableConfigs, null, new String[]{
			"(true,+I(Hello world,3))",
			"(true,+I(Hello world,4))",
			"(true,+I(Hello,3))",
			"(true,+I(Hi,2))"
		});
	}

	void testWithSQL(
			String beforeSQL,
			String afterSQL,
			TableConfig tableConfigs,
			String sourceChangeMode,
			String[] expectedOutput) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Configuration configuration = new Configuration();
		configuration.set(ROW_DATA_SCHEMA_COMPATIBILITY_RESOLVE_STRATEGY,
			RowDataSchemaCompatibilityResolveStrategy.STRONG_RESTRICTIVE);
		env.configure(configuration, StateSchemaEvolutionCheckpointRecoveryTests.class.getClassLoader());
		StreamTableEnvironment tEnv;
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();
		tEnv = StreamTableEnvironmentImpl.create(env, settings, tableConfigs);
		// Source
		String id = TestValuesTableFactory.registerData(TestData.data4WithTimestamp());
		String sql = generateCreateTableSQL("T1", sourceChangeMode, id, false);
		tEnv.executeSql(sql);
		// Dim
		tEnv.executeSql("CREATE TABLE dim (\n" +
			"  `id` INT,\n" +
			"  `name` STRING,\n" +
			"  `age` INT,\n" +
			"  `ts` TIMESTAMP(3)\n" +
			") WITH (\n" +
			"  'connector' = 'values',\n" +
			"  'lookup.later-join-latency' = '1s',\n" +
			"  'lookup-function-class' = 'org.apache.flink.table.planner.runtime.utils.CheckpointRecoveryUtils$MockDimFunction'\n" +
			")");
		Table sinkTable = tEnv.sqlQuery(beforeSQL);
		tEnv.toRetractStream(sinkTable, RowData.class);
		JobGraph oldJob = env.getStreamGraph().getJobGraph();
		// Step 1: start a job and take a savepoint.
		ClusterClient<?> client = cluster.getClusterClient();
		CheckpointRecoveryUtils.setLatch(new CountDownLatch(TestData.data4WithTimestamp().size()));
		final String savepointPath = submitJobAndTakeSavepoint(client, oldJob, true);
		// Step 2: start a new job and recover from the savepoint took by the old job.
		sql = generateCreateTableSQL("T2", sourceChangeMode, id, true);
		tEnv.executeSql(sql);
		Table sinkTable2 = tEnv.sqlQuery(afterSQL);
		TestSink<Tuple2<Boolean, RowData>> testSink = new TestSink<>();
		TestSink.reset();
		tEnv.toRetractStream(sinkTable2, RowData.class).addSink(testSink);
		JobGraph newJob = env.getStreamGraph().getJobGraph();
		newJob.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));
		submitJobAndTakeSavepoint(client, newJob, false);
		// Step 3: check the equivalence of outputs.
		String[] actual = JavaConverters.asJavaCollectionConverter(TestSink.results()).asJavaCollection().toArray(new String[]{});
		Arrays.sort(actual);
		assertArrayEquals(expectedOutput, actual);
	}

	private String generateCreateTableSQL(
			String tableName,
			String sourceChangeMode,
			String dataId,
			boolean isBounded) {
		String sql;
		if (sourceChangeMode == null || sourceChangeMode.isEmpty() || sourceChangeMode.equals("I")) {
			sql = String.format(SOURCE_CREATION_WITH_WATERMARK, tableName, dataId,
				CheckpointRecoveryUtils.FiniteScanTableSource.class.getName(), isBounded);
		} else {
			sql = String.format(SOURCE_CREATION_WITHOUT_WATERMARK, tableName, dataId,
				CheckpointRecoveryUtils.FiniteScanTableSource.class.getName(), isBounded, sourceChangeMode);
		}
		return sql;
	}

	private String submitJobAndTakeSavepoint(
			ClusterClient<?> client,
			JobGraph jobGraph,
			boolean takeSavepoint) throws Exception {
		final JobID jobId = jobGraph.getJobID();
		if (takeSavepoint) {
			ClientUtils.submitJob(client, jobGraph);
			CheckpointRecoveryUtils.getLatch().await(60, TimeUnit.SECONDS);
			client.waitAllTaskRunningOrClusterFailed(jobId, 60000).get(10, TimeUnit.SECONDS);
			return client.triggerSavepoint(jobId, savepointDir.getAbsolutePath()).get(60, TimeUnit.SECONDS);
		} else {
			ClientUtils.submitJobAndWaitForResult(client, jobGraph,
				StateSchemaEvolutionCheckpointRecoveryTests.class.getClassLoader());
			return null;
		}
	}
}
