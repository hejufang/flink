/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Integration test for triggering and resuming from savepoints. */

@SuppressWarnings("serial")
public class CheckpointRestoreWithUidHashITCase extends TestLogger {
	private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);

	@Rule public final TemporaryFolder folder = new TemporaryFolder();
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
	public void testResumeWithTypoChanged() throws Exception {
		final int numTaskManagers = 2;
		final int numSlotsPerTaskManager = 2;
		final int parallelism = numTaskManagers * numSlotsPerTaskManager;

		final MiniClusterResourceFactory clusterFactory =
				new MiniClusterResourceFactory(
						numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

		JobGraph originJobGraph = createJobGraph(parallelism, 0, 1000, "origin");
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism, originJobGraph);
		verifySavepoint(parallelism, savepointPath);

		JobGraph disableChainJobGraph =  createJobGraph(parallelism, 0, 1000, "change");

		try {
			restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, disableChainJobGraph);
			fail("Expected exception not thrown.");
		} catch (Exception e){
			assertThat(e.getMessage(), containsString("Could not run job in detached mode"));
		}
	}

	@Test
	public void testResumeWithTypoChangedAndSetUidHash() throws Exception {
		final int numTaskManagers = 2;
		final int numSlotsPerTaskManager = 2;
		final int parallelism = numTaskManagers * numSlotsPerTaskManager;

		final MiniClusterResourceFactory clusterFactory =
				new MiniClusterResourceFactory(
						numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

		JobGraph originJobGraph = createJobGraph(parallelism, 0, 1000, "origin");
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism, originJobGraph);
		verifySavepoint(parallelism, savepointPath);

		JobGraph changeWithSetUidHash =  createJobGraph(parallelism, 0, 1000, "changeWithSetUidHash");
		restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, changeWithSetUidHash);
	}

	@Test
	public void testResumeWithTypoChangedAndSetBothUidAndUidHash() throws Exception {
		final int numTaskManagers = 2;
		final int numSlotsPerTaskManager = 2;
		final int parallelism = numTaskManagers * numSlotsPerTaskManager;

		final MiniClusterResourceFactory clusterFactory =
				new MiniClusterResourceFactory(
						numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

		JobGraph originJobGraph = createJobGraph(parallelism, 0, 1000, "origin");
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism, originJobGraph);
		verifySavepoint(parallelism, savepointPath);

		JobGraph changeWithBothSetUidHashAndUid =  createJobGraph(parallelism, 0, 1000, "changeWithBothSetUidHashAndUid");
		restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, changeWithBothSetUidHashAndUid);
	}

	@Test
	public void testResumeWithTypoChangedAndSetSameUid() throws Exception {
		final int numTaskManagers = 2;
		final int numSlotsPerTaskManager = 2;
		final int parallelism = numTaskManagers * numSlotsPerTaskManager;

		final MiniClusterResourceFactory clusterFactory =
				new MiniClusterResourceFactory(
						numTaskManagers, numSlotsPerTaskManager, getFileBasedCheckpointsConfig());

		JobGraph originJobGraph = createJobGraph(parallelism, 0, 1000, "changeWithBothSetUidHashAndUid");
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, parallelism, originJobGraph);
		verifySavepoint(parallelism, savepointPath);

		JobGraph originWithUid =  createJobGraph(parallelism, 0, 1000, "originWithUid");
		restoreJobAndVerifyState(savepointPath, clusterFactory, parallelism, originWithUid);
	}

	private String submitJobAndTakeSavepoint(
			MiniClusterResourceFactory clusterFactory, int parallelism, JobGraph  jobGraph) throws Exception {
		final JobID jobId = jobGraph.getJobID();
		StatefulCounter.resetForTest(parallelism);

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);

			StatefulCounter.getProgressLatch().await();

			return client.cancelWithSavepoint(jobId, null).get();
		} finally {
			cluster.after();
			StatefulCounter.resetForTest(parallelism);
		}
	}

	private void verifySavepoint(final int parallelism, final String savepointPath)
			throws URISyntaxException {
		// Only one savepoint should exist
		File savepointDir = new File(new URI(savepointPath));
		assertTrue("Savepoint directory does not exist.", savepointDir.exists());
		assertTrue(
				"Savepoint did not create self-contained directory.", savepointDir.isDirectory());

		File[] savepointFiles = savepointDir.listFiles();

		if (savepointFiles != null) {
			// Expect one metadata file and one checkpoint file per stateful
			// parallel subtask
			String errMsg =
					"Did not write expected number of savepoint/checkpoint files to directory: "
							+ Arrays.toString(savepointFiles);
			assertEquals(errMsg, 2 + parallelism, savepointFiles.length);
		} else {
			fail(String.format("Returned savepoint path (%s) is not valid.", savepointPath));
		}
	}

	private void restoreJobAndVerifyState(
			String savepointPath, MiniClusterResourceFactory clusterFactory, int parallelism, JobGraph jobGraph)
			throws Exception {
		jobGraph.setSavepointRestoreSettings(
				SavepointRestoreSettings.forPath(savepointPath, false));
		final JobID jobId = jobGraph.getJobID();
		StatefulCounter.resetForTest(parallelism);

		MiniClusterWithClientResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();

		try {
			ClientUtils.submitJob(client, jobGraph);

			// Await state is restored
			StatefulCounter.getRestoreLatch().await();

			// Await some progress after restore
			StatefulCounter.getProgressLatch().await();

			client.cancel(jobId).get();

			FutureUtils.retrySuccessfulWithDelay(
					() -> client.getJobStatus(jobId),
					Time.milliseconds(50),
					Deadline.now().plus(Duration.ofSeconds(30)),
					status -> status == JobStatus.CANCELED,
					TestingUtils.defaultScheduledExecutor());

			client.disposeSavepoint(savepointPath).get();

			assertFalse("Savepoint not properly cleaned up.", new File(savepointPath).exists());
		} finally {
			cluster.after();
			StatefulCounter.resetForTest(parallelism);
		}
	}


	// ------------------------------------------------------------------------
	// Test program
	// ------------------------------------------------------------------------

	/** Creates a streaming JobGraph from the StreamEnvironment. */

	private JobGraph createJobGraph(int parallelism, int numberOfRetries, long restartDelay, String graph) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(numberOfRetries, restartDelay));
		DataStream<Integer> source = env.addSource(new InfiniteTestSource());

		DataStream dataStream;
		switch (graph) {
			case "origin":
				dataStream = source.map(new StatefulCounter());
				break;
			case "originWithUid":
				dataStream = source.map(new StatefulCounter()).uid("20ba6b65f97481d5570070de90e4e791");
				break;
			case "change":
				env.disableOperatorChaining();
				dataStream = source.map(new StatefulCounter()).setParallelism(1);
				break;
			case "changeWithSetUidHash":
				env.disableOperatorChaining();
				dataStream = source.map(new StatefulCounter()).setUidHash("20ba6b65f97481d5570070de90e4e791");
				break;
			case "changeWithSetUid":
				env.disableOperatorChaining();
				dataStream = source.map(new StatefulCounter()).uid("20ba6b65f97481d5570070de90e4e791");
				break;
			case "changeWithBothSetUidHashAndUid":
				env.disableOperatorChaining();
				dataStream = source.map(new StatefulCounter()).uid("20ba6b65f97481d5570070de90e4e791").setUidHash("20ba6b65f97481d5570070de90e4e791");
				break;
			default:
				throw new UnsupportedOperationException(graph + " is unsupported.");
		}
		dataStream.addSink(new DiscardingSink<>());

		return env.getStreamGraph().getJobGraph();
	}

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;
		private volatile boolean suspended = false;
		private static final Collection<InfiniteTestSource> createdSources =
				new CopyOnWriteArrayList<>();
		private transient volatile CompletableFuture<Void> completeFuture;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			completeFuture = new CompletableFuture<>();
			createdSources.add(this);
			try {
				while (running) {
					if (!suspended) {
						synchronized (ctx.getCheckpointLock()) {
							ctx.collect(1);
						}
					}
					Thread.sleep(1);
				}
				completeFuture.complete(null);
			} catch (Exception e) {
				completeFuture.completeExceptionally(e);
				throw e;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		public void suspend() {
			suspended = true;
		}

		public static void resetForTest() {
			createdSources.clear();
		}

		public CompletableFuture<Void> getCompleteFuture() {
			return completeFuture;
		}

		public static void cancelAllAndAwait() throws ExecutionException, InterruptedException {
			createdSources.forEach(InfiniteTestSource::cancel);
			allOf(
							createdSources.stream()
									.map(InfiniteTestSource::getCompleteFuture)
									.toArray(CompletableFuture[]::new))
					.get();
		}

		public static void suspendAll() {
			createdSources.forEach(InfiniteTestSource::suspend);
		}
	}

	private static class StatefulCounter extends RichMapFunction<Integer, Integer>
			implements ListCheckpointed<byte[]> {

		private static volatile CountDownLatch progressLatch = new CountDownLatch(0);
		private static volatile CountDownLatch restoreLatch = new CountDownLatch(0);

		private int numCollectedElements = 0;

		private static final long serialVersionUID = 7317800376639115920L;
		private byte[] data;

		@Override
		public void open(Configuration parameters) throws Exception {
			if (data == null) {
				// We need this to be large, because we want to test with files
				Random rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
				data =
						new byte
								[(int)
												CheckpointingOptions.FS_SMALL_FILE_THRESHOLD
														.defaultValue()
														.getBytes()
										+ 1];
				rand.nextBytes(data);
			}
		}

		@Override
		public Integer map(Integer value) throws Exception {
			for (int i = 0; i < data.length; i++) {
				data[i] += 1;
			}

			if (numCollectedElements++ > 10) {
				progressLatch.countDown();
			}

			return value;
		}

		@Override
		public List<byte[]> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(data);
		}

		@Override
		public void restoreState(List<byte[]> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException(
						"Test failed due to unexpected recovered state size " + state.size());
			}
			this.data = state.get(0);

			restoreLatch.countDown();
		}

		// --------------------------------------------------------------------

		static CountDownLatch getProgressLatch() {
			return progressLatch;
		}

		static CountDownLatch getRestoreLatch() {
			return restoreLatch;
		}

		static void resetForTest(int parallelism) {
			progressLatch = new CountDownLatch(parallelism);
			restoreLatch = new CountDownLatch(parallelism);
		}
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	private static class MiniClusterResourceFactory {
		private final int numTaskManagers;
		private final int numSlotsPerTaskManager;
		private final Configuration config;

		private MiniClusterResourceFactory(
				int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
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
		config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		config.setString(
				CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		return config;
	}

	private Configuration getFileBasedCheckpointsConfig() {
		return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
	}
}
