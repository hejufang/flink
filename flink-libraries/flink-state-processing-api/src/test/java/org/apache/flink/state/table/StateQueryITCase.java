/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.table.catalog.SavepointCatalog;
import org.apache.flink.state.table.catalog.resolver.LocationResolver;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.configuration.CheckpointingOptions.MAX_RETAINED_CHECKPOINTS;
import static org.junit.Assert.fail;

/**
 * Integration test for State Query.
 */
@RunWith(Parameterized.class)
public class StateQueryITCase {

	private static final String JOB_NAME = "savepoint-job";

	private TableEnvironment tEnv;

	private SavepointCatalog savepointCatalog;

	private LocationResolver resolver;

	@Parameterized.Parameter
	public String statebackend;

	@Parameterized.Parameters(name = "backend = {0}")
	public static Object[] data() {
		return new Object[]{"filesystem", "rocksdb_incremental", "rocksdb_full"};
	}

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private File savepointDir;

	@Before
	public void setUp() throws Exception {

		setUpSavepointDir();

		final MiniClusterResourceFactory clusterFactory = new MiniClusterResourceFactory(
			1,
			1,
			getFileBasedCheckpointsConfig());

		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, 1);

		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		this.tEnv = TableEnvironment.create(settings);

		resolver = new LocationResolver() {
			@Override
			public Tuple2<String, String> parseDatabase(String database) {
				return Tuple2.of("testJob", "testSavepointID");
			}

			@Override
			public Path getEffectiveSavepointPath(String database) throws IOException {
				return new Path(database);
			}
		};

		savepointCatalog = new SavepointCatalog("savepoint", savepointPath, resolver);
		// use savepoint catalog
		tEnv.registerCatalog("savepoint", savepointCatalog);
		tEnv.useCatalog("savepoint");
	}

	@Test
	public void testShowTables(){
		final TableResult result = tEnv.executeSql("show tables");

		List expect = new ArrayList();
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#all_keyed_states"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#all_operator_states"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#listState"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#mapState"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#unionState"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#valueState"));

		expect.add(Row.of("state_meta"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testShowViews(){
		final TableResult result = tEnv.executeSql("show views");
		List expect = new ArrayList();
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8#all_states"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testQueryStateMeta() {

		final TableResult result = tEnv.executeSql("select * from state_meta");
		List expect = new ArrayList();

		String stateBackendType = null;

		switch (statebackend) {
			case "filesystem":
				stateBackendType = "HEAP_STATE_BACKEND";
				break;
			case "rocksdb_incremental":
				stateBackendType = "INCREMENTAL_ROCKSDB_STATE_BACKEND";
				break;
			case "rocksdb_full":
				stateBackendType = "FULL_ROCKSDB_STATE_BACKEND";
				break;
			default:
				throw new IllegalStateException("Unsupported state backend selected.");
		}
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8", "Flat Map", "uid", true, "Integer", "valueState", "VALUE", stateBackendType, "Integer"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8", "Flat Map", "uid", true, "Integer", "listState", "LIST", stateBackendType, "List<String>"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8", "Flat Map", "uid", true, "Integer", "mapState", "MAP", stateBackendType, "Map<String, String>"));
		expect.add(Row.of("0e1febe90327d1cea326114660ec1de8", "Flat Map", "uid", false, null, "unionState", "LIST", "OPERATOR_STATE_BACKEND", "List<String>"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testKeyedListStateQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#listState`");
		List expect = new ArrayList();
		expect.add(Row.of("3", "VoidNamespace", "value_0"));
		expect.add(Row.of("3", "VoidNamespace", "value_1"));
		expect.add(Row.of("3", "VoidNamespace", "value_2"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testKeyedValueStateQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#valueState`");
		List expect = new ArrayList();
		expect.add(Row.of("3", "VoidNamespace", "3"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testKeyedMapStateQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#mapState`");
		List expect = new ArrayList();
		expect.add(Row.of("3", "VoidNamespace", "userKey_0=value_0"));
		expect.add(Row.of("3", "VoidNamespace", "userKey_1=value_1"));
		expect.add(Row.of("3", "VoidNamespace", "userKey_2=value_2"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testUnionStateQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#unionState`");
		List expect = new ArrayList();
		expect.add(Row.of("value_0"));
		expect.add(Row.of("value_1"));
		expect.add(Row.of("value_2"));
		List restList = Lists.newArrayList(result.collect());
		expect.sort(Comparator.comparing(rowData -> rowData.toString()));
		restList.sort(Comparator.comparing(rowData -> rowData.toString()));

		Assert.assertEquals(expect, restList);
	}

	@Test
	public void testAllKeyedQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#all_keyed_states`");
		List expect = new ArrayList();
		expect.add(Row.of("listState", "3", "VoidNamespace", "value_0"));
		expect.add(Row.of("listState", "3", "VoidNamespace", "value_1"));
		expect.add(Row.of("listState", "3", "VoidNamespace", "value_2"));
		expect.add(Row.of("mapState", "3", "VoidNamespace", "userKey_0=value_0"));
		expect.add(Row.of("mapState", "3", "VoidNamespace", "userKey_1=value_1"));
		expect.add(Row.of("mapState", "3", "VoidNamespace", "userKey_2=value_2"));
		expect.add(Row.of("valueState", "3", "VoidNamespace", "3"));

		List restList = Lists.newArrayList(result.collect());
		expect.sort(Comparator.comparing(rowData -> rowData.toString()));
		restList.sort(Comparator.comparing(rowData -> rowData.toString()));

		Assert.assertEquals(expect, restList);
	}

	@Test
	public void testAllOperatorStateQuery() {
		final TableResult result = tEnv.executeSql("select * from `0e1febe90327d1cea326114660ec1de8#all_operator_states`");
		List expect = new ArrayList();
		expect.add(Row.of("unionState", "value_0"));
		expect.add(Row.of("unionState", "value_1"));
		expect.add(Row.of("unionState", "value_2"));
		List restList = Lists.newArrayList(result.collect());
		expect.sort(Comparator.comparing(rowData -> rowData.toString()));
		restList.sort(Comparator.comparing(rowData -> rowData.toString()));

		Assert.assertEquals(expect, restList);
	}

	private String submitJobAndTakeSavepoint(MiniClusterResourceFactory clusterFactory, int parallelism) throws Exception {
		final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000, 10);
		final JobID jobId = jobGraph.getJobID();
		StatefulFunction.resetForTest(parallelism);

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);

			StatefulFunction.getProgressLatch().await();

			return client.cancelWithSavepoint(jobId, null).get();
		} finally {
			cluster.after();
			StatefulFunction.resetForTest(parallelism);
		}
	}

	// ------------------------------------------------------------------------
	// Test program
	// ------------------------------------------------------------------------

	private void setUpSavepointDir() throws IOException {
		final File testRoot = folder.newFolder();
		savepointDir = new File(testRoot, "savepoints");
		if (!savepointDir.mkdirs()) {
			fail("Test setup failed: failed to create temporary directories.");
		}
	}


	/**
	 * Creates a streaming JobGraph from the StreamEnvironment.
	 */
	private JobGraph createJobGraph(
		int parallelism,
		int numberOfRetries,
		long restartDelay,
		int maxRetainedCheckpoints) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(parallelism);
		env.disableOperatorChaining();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(numberOfRetries, restartDelay));
		Configuration conf = new Configuration();
		conf.set(MAX_RETAINED_CHECKPOINTS, maxRetainedCheckpoints);
		env.getCheckpointConfig().configure(conf);
		DataStream stream = env
			.addSource(new InfiniteTestSource())
			.keyBy(x -> x)
			.flatMap(new StatefulFunction()).uid("uid");

		stream.addSink(new DiscardingSink<>());

		return env.getStreamGraph(JOB_NAME).getJobGraph();
	}

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(3);
				}
				Thread.sleep(1);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	static class StatefulFunction extends RichFlatMapFunction<Integer, Void> implements CheckpointedFunction {

		private static volatile CountDownLatch progressLatch = new CountDownLatch(0);

		private ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Types.INT);
		private MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("mapState", Types.STRING, Types.STRING);
		private ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("listState", Types.STRING);
		private ListStateDescriptor<String> unionStateDescriptor = new ListStateDescriptor<>("unionState", Types.STRING);


		ValueState<Integer> valueState;
		MapState<String, String> mapState;
		ListState<String> listState;
		ListState<String> unionState;

		@Override
		public void open(Configuration parameters) {
			valueState = getRuntimeContext().getState(valueStateDescriptor);
			mapState = getRuntimeContext().getMapState(mapStateDescriptor);
			listState = getRuntimeContext().getListState(listStateDescriptor);
		}

		@Override
		public void flatMap(Integer value, Collector<Void> out) throws Exception {
			valueState.update(value);

			ArrayList<String> listValues = new ArrayList();

			for (Integer i = 0; i < value; i++) {
				String userKey = "userKey_" + i;
				String userValue = "value_" + i;
				mapState.put(userKey, userValue);
				listValues.add(userValue);
			}
			unionState.update(listValues);
			listState.update(listValues);
			progressLatch.countDown();
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			unionState = context.getOperatorStateStore().getUnionListState(unionStateDescriptor);
		}

		static CountDownLatch getProgressLatch() {
			return progressLatch;
		}

		static void resetForTest(int parallelism) {
			progressLatch = new CountDownLatch(parallelism);
		}
	}


	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private static class MiniClusterResourceFactory {
		private final int numTaskManagers;
		private final int numSlotsPerTaskManager;
		private final Configuration config;

		private MiniClusterResourceFactory(int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
			this.numTaskManagers = numTaskManagers;
			this.numSlotsPerTaskManager = numSlotsPerTaskManager;
			this.config = config;
		}

		MiniClusterWithClientResource get() {
			return new MiniClusterWithClientResource(
				new MiniClusterResourceConfiguration.Builder()
					.setConfiguration(config)
					.setNumberTaskManagers(numTaskManagers)
					.setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
					.build());
		}
	}

	private Configuration getFileBasedCheckpointsConfig(final String savepointDir) {
		final Configuration config = new Configuration();

		switch (statebackend) {
			case "filesystem":
				config.setString(CheckpointingOptions.STATE_BACKEND, statebackend);
				break;
			case "rocksdb_incremental":
				config.setString(CheckpointingOptions.STATE_BACKEND, "rocksdb");
				config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
				break;
			case "rocksdb_full":
				config.setString(CheckpointingOptions.STATE_BACKEND, "rocksdb");
				break;
			default:
				throw new IllegalStateException("Unsupported state backend selected.");
		}

		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, savepointDir);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		return config;
	}

	private Configuration getFileBasedCheckpointsConfig() {
		return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
	}
}
