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
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

/**
 * Base class for CheckpointVerifyTest.
 */
public class CheckpointVerifyTestBase {

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	private File checkpointDir;

	private File savepointDir;

	private Configuration config = new Configuration();

	protected MiniClusterResourceFactory clusterFactory;

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
		clusterFactory = new MiniClusterResourceFactory(2, 2, config);
	}

	protected String submitJobAndTakeSavepoint(
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

	protected CheckpointVerifyResult verifyCheckpoint(
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
}
