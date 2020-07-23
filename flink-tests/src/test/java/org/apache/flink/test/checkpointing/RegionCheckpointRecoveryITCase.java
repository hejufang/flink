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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Testing for recovery.
 */
public class RegionCheckpointRecoveryITCase extends TestLogger implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(RegionCheckpointRecoveryITCase.class);

	private static final String STATE_BACKEND = "filesystem";
	private static final String JOB_NAME = "region-checkpoint-job";

	private static String checkpointPath;

	private static MiniClusterWithClientResource cluster;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		checkpointPath = checkpointDir.toURI().toString();

		Configuration config = new Configuration();
		config.setString(CheckpointingOptions.STATE_BACKEND, STATE_BACKEND);
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath);

		cluster = new MiniClusterWithClientResource(
				new MiniClusterResourceConfiguration.Builder()
						.setConfiguration(config)
						.setNumberTaskManagers(2)
						.setNumberSlotsPerTaskManager(2)
						.build());
		cluster.before();
	}

	@Test
	public void testRecovery() throws Exception {
		final ClusterClient<?> client = cluster.getClusterClient();

		final JobGraph job = createJobGraph();
		client.setDetached(true);
		client.submitJob(job, RegionCheckpointRecoveryITCase.class.getClassLoader());

		while (TestSource.numberOfCheckpoints < 3) {
			Thread.sleep(200);
		}
		// cancel job
		client.cancel(job.getJobID());

		while (getRunningJobs(client).size() > 0) {
			Thread.sleep(200);
		}

		// make sure the checkpoint is successful
		Assert.assertTrue(TestSource.numberOfSuccessfulCheckpoints > 0);

		// recover the job from checkpoint
		TestSource.numberOfCheckpoints = 0;
		TestSource.numberOfSuccessfulCheckpoints = 0;
		client.setDetached(true);
		final JobGraph recoveredJob = createJobGraph();

		// regard the checkpoint as a savepoint
		final String savepointPath = checkpointPath + "/" + JOB_NAME + "/default/chk-1";
		LOG.info("Using {} as the savepoint path.", savepointPath);
		recoveredJob.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));
		client.submitJob(recoveredJob, RegionCheckpointRecoveryITCase.class.getClassLoader());

		while (TestSource.numberOfSuccessfulCheckpoints == 0) {
			Thread.sleep(200);
		}

		client.cancel(recoveredJob.getJobID());
		Assert.assertTrue(StatefulAndExpireSink.stateRestored = true);
	}

	private JobGraph createJobGraph() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfiguration().setInteger("state.checkpoints.region.max-retained-snapshots", 1000);
		env.getConfiguration().setBoolean("state.checkpoints.region.enabled", true);
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
		env.getCheckpointConfig().setCheckpointInterval(2000);
		env.getCheckpointConfig().setCheckpointTimeout(3000);

		env.addSource(new TestSource()).addSink(new StatefulAndExpireSink());
		return env.getStreamGraph().getJobGraph();
	}

	private static class TestSource extends RichParallelSourceFunction<Integer> implements CheckpointedFunction, CheckpointListener {

		static int numberOfCheckpoints = 0;
		static int numberOfSuccessfulCheckpoints = 0;

		boolean loop = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (loop) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Random().nextInt(100));
				}
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				numberOfSuccessfulCheckpoints++;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				numberOfCheckpoints++;
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {}
	}

	private static class StatefulAndExpireSink extends RichSinkFunction<Integer> implements CheckpointedFunction {

		ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>("list_state", Integer.class);
		ListState<Integer> state;

		static boolean stateRestored = false;

		@Override
		public void invoke(Integer value, Context context) throws Exception {
			if (state != null) {
				state.add(value);
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (TestSource.numberOfSuccessfulCheckpoints >= 1 && getRuntimeContext().getIndexOfThisSubtask() == 0) {
				Thread.sleep(5000);
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			if (context.isRestored()) {
				state = context.getOperatorStateStore().getUnionListState(descriptor);
				if (state.get().iterator().hasNext()) {
					stateRestored = true;
				}
			}
		}
	}

	private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
		Collection<JobStatusMessage> statusMessages = client.listJobs().get();
		return statusMessages.stream()
				.filter(status -> !status.getJobState().isGloballyTerminalState())
				.map(JobStatusMessage::getJobId)
				.collect(Collectors.toList());
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}
}
