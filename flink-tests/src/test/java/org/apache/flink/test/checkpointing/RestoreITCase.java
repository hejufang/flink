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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * RestoreITCase.
 */
public class RestoreITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);
	private static final String JOB_NAME = "savepoint-job";

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private File checkpointDir;

	private File savepointDir;

	@Before
	public void setUp() throws Exception {
		final File testRoot = folder.newFolder();

		checkpointDir = new File(testRoot, "checkpoints");
		savepointDir = new File(testRoot, "savepoints");

		if (!checkpointDir.mkdir() || !savepointDir.mkdirs()) {
			fail("Test setup failed: failed to create temporary directories.");
		}
	}

	@Test
	public void testAllowNonRestoredState() throws Exception {
		final int numTaskManagers = 2;
		final int numSlotsPerTaskManager = 2;
		final int parallelism = numTaskManagers * numSlotsPerTaskManager;

		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
		config.setBoolean(CheckpointingOptions.ALLOW_NON_RESTORED_STATE, true);

		final MiniClusterResourceFactory clusterFactory = new MiniClusterResourceFactory(
			numTaskManagers,
			numSlotsPerTaskManager,
			config);

		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism, createJobGraph(false, "testAllowNonRestoredState"));
		restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, createJobGraph(true, "testAllowNonRestoredState"));
	}

	private JobGraph createJobGraph(boolean isRestored, String graph) {
		switch (graph) {
			case "testAllowNonRestoredState":
				if (isRestored) {
					StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
					env.setParallelism(1);
					env.addSource(new InfiniteTestSource())
						.addSink(new DiscardingSink<>());
					return env.getStreamGraph(JOB_NAME).getJobGraph();
				} else {
					StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
					env.setParallelism(1);
					env.disableOperatorChaining();
					env.addSource(new InfiniteTestSource())
						.map(new StatefulMap())
						.addSink(new DiscardingSink<>());
					return env.getStreamGraph(JOB_NAME).getJobGraph();
				}
			default:
				throw new UnsupportedOperationException(graph + " is unsupported.");
		}
	}

	private String submitJobAndTakeSavepoint(
		MiniClusterResourceFactory clusterFactory,
		int parallelism,
		JobGraph jobGraph) throws Exception {
		final JobID jobId = jobGraph.getJobID();

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);
			client.waitAllTaskRunningOrClusterFailed(jobId, 60000).get(10, TimeUnit.SECONDS);
			return client.cancelWithSavepoint(jobId, null).get();
		} finally {
			cluster.after();
		}
	}

	private void restoreJobAndVerifyState(
		String savepointPath,
		MiniClusterResourceFactory clusterFactory,
		int parallelism,
		JobGraph jobGraph) throws Exception {
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath, false));
		final JobID jobId = jobGraph.getJobID();

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);
			client.waitAllTaskRunningOrClusterFailed(jobId, 60000).get(10, TimeUnit.SECONDS);

			client.cancel(jobId).get();

			FutureUtils.retrySuccessfulWithDelay(
				() -> client.getJobStatus(jobId),
				Time.milliseconds(50),
				Deadline.now().plus(Duration.ofSeconds(30)),
				status -> status == JobStatus.CANCELED,
				TestingUtils.defaultScheduledExecutor()
			);

			client.disposeSavepoint(savepointPath)
				.get();

			assertFalse("Savepoint not properly cleaned up.", new File(savepointPath).exists());
		} finally {
			cluster.after();
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

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(1);
				}
				Thread.sleep(1);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class StatefulMap extends RichMapFunction<Integer, Integer>
		implements CheckpointedFunction {

		ListState<Integer> state;

		@Override
		public void open(Configuration parameters) throws Exception {}

		@Override
		public Integer map(Integer value) throws Exception {
			state.update(Collections.singletonList(value));
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("list-state", Integer.class));
		}
	}
}
