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
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateRegistry;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CheckpointVerifier;
import org.apache.flink.client.cli.CheckpointVerifyResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorStateMeta;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * CheckpointVerifyITCase.
 */
public class CheckpointVerifyITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);
	private static final String JOB_NAME = "savepoint-job";

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private File checkpointDir;

	private File savepointDir;

	private Configuration config = new Configuration();

	private MiniClusterResourceFactory clusterFactory;

	private KeySelector defaultKeySelector = new KeySelector<Integer, String>() {
		@Override
		public String getKey(Integer value) throws Exception {
			return value.toString();
		}
	};

	private ValueStateDescriptor defaultValueStateDescriptor = new ValueStateDescriptor("state", StringSerializer.INSTANCE);

	@Before
	public void setUp() throws Exception {
		final File testRoot = folder.newFolder();

		checkpointDir = new File(testRoot, "checkpoints");
		savepointDir = new File(testRoot, "savepoints");

		if (!checkpointDir.mkdir() || !savepointDir.mkdirs()) {
			fail("Test setup failed: failed to create temporary directories.");
		}

		config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
		config.setBoolean(CheckpointingOptions.ALLOW_PERSIST_STATE_META, true);
		clusterFactory = new MiniClusterResourceFactory(1, 1, config);
	}

	@Test
	public void testStateTypeChangedIncompatible() throws Exception {
		ValueStateDescriptor after = new ValueStateDescriptor("state", IntSerializer.INSTANCE);
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, createJobGraph(defaultKeySelector, defaultValueStateDescriptor));
		CheckpointVerifyResult checkpointVerifyResult = verifyCheckpoint(savepointPath, createJobGraph(defaultKeySelector, after));
		Assert.assertEquals(checkpointVerifyResult, CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	@Test
	public void testKeyedSerializerIncompatible() throws Exception {
		KeySelector keySelector = new KeySelector<Integer, Integer>() {
			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, createJobGraph(defaultKeySelector, defaultValueStateDescriptor));
		CheckpointVerifyResult checkpointVerifyResult = verifyCheckpoint(savepointPath, createJobGraph(keySelector, defaultValueStateDescriptor));
		Assert.assertEquals(checkpointVerifyResult, CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE);
	}

	private JobGraph createJobGraph(KeySelector keySelector, ValueStateDescriptor stateDescriptor) {
		StatefulMap statefulMap = new StatefulMap(stateDescriptor);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
				env.setParallelism(1);
				env.addSource(new InfiniteTestSource()).uid("source")
					.keyBy(keySelector)
					.map(statefulMap).uid("map");
				return env.getStreamGraph(JOB_NAME).getJobGraph();

	}

	private String submitJobAndTakeSavepoint(
		MiniClusterResourceFactory clusterFactory,
		JobGraph jobGraph) throws Exception {
		final JobID jobId = jobGraph.getJobID();

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);
			client.waitAllTaskRunningOrClusterFailed(jobId, 60000).get(10, TimeUnit.SECONDS);
			return client.cancelWithSavepoint(jobId, savepointDir.getAbsolutePath()).get();
		} finally {
			cluster.after();
		}
	}

	private CheckpointVerifyResult verifyCheckpoint(
		String savepointPath,
		JobGraph jobGraph) throws Exception {

		Map<JobVertexID, JobVertex> tasks = new HashMap<>();

		jobGraph.getVertices().forEach(jobVertex -> {
			tasks.put(jobVertex.getID(), jobVertex);
		});
		CompletedCheckpointStorageLocation location = AbstractFsCheckpointStorage
			.resolveCheckpointPointer(savepointPath);

		Map<OperatorID, OperatorState> operatorIDAndOperatorState;
		try (DataInputStream stream = new DataInputStream(location.getMetadataHandle().openInputStream())) {
			operatorIDAndOperatorState = Checkpoints.loadCheckpointMetadata(stream, Thread.currentThread().getContextClassLoader(), savepointPath)
				.getOperatorStates().stream().collect(Collectors.toMap(OperatorState::getOperatorID, Function.identity()));
		}

		Map<OperatorID, OperatorStateMeta> operatorIDOperatorStateMetas = CheckpointVerifier.resolveStateMetaFromCheckpointPath(savepointPath, Thread.currentThread().getContextClassLoader());
		return CheckpointVerifier.getCheckpointVerifyResult(tasks, CheckpointVerifier.mergeLatestStateAndMetas(operatorIDAndOperatorState, operatorIDOperatorStateMetas));
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

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(1);
				}
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class StatefulMap extends RichMapFunction<Integer, Integer>
		implements CheckpointedFunction {

		private ValueStateDescriptor stateDescriptor;

		StatefulMap(ValueStateDescriptor stateDescriptor){
			this.stateDescriptor = stateDescriptor;
		}

		State state;

		@Override
		public void open(Configuration parameters) throws Exception {}

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}

		@Override
		public void registerState(StateRegistry stateRegistry) throws Exception {
			state = stateRegistry.getState(stateDescriptor);
		}
	}
}
