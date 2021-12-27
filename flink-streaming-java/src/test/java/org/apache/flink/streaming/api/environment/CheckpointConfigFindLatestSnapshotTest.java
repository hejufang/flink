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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorage;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Tests for finding latest snapshot {@link CheckpointConfig}.
 */
public class CheckpointConfigFindLatestSnapshotTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testUserDefinedCheckpointsNamespace() throws IOException {
		JobGraph jobGraph = new JobGraph("test-job");
		Configuration configuration = new Configuration();
		configuration.set(CheckpointingOptions.CHECKPOINTS_NAMESPACE, "ns");
		configuration.set(CheckpointingOptions.SNAPSHOT_NAMESPACE, "sn");
		configuration.set(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, "latest");
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		Assert.assertEquals("ns", configuration.get(CheckpointingOptions.CHECKPOINTS_NAMESPACE));
		Assert.assertTrue(!configuration.contains(CheckpointingOptions.SNAPSHOT_NAMESPACE) && !configuration.contains(CheckpointingOptions.RESTORE_SAVEPOINT_PATH));
	}

	@Test
	public void testSetUpNewSnapshotFeature() throws IOException {
		JobGraph jobGraph = new JobGraph("test-job");
		Configuration configuration = new Configuration();
		configuration.set(CheckpointingOptions.SNAPSHOT_NAMESPACE, "snapshot_namespace");
		configuration.set(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, "latest");
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		Assert.assertEquals("snapshot_namespace", configuration.get(CheckpointingOptions.CHECKPOINTS_NAMESPACE));
	}

	@Test
	public void testReconfigureRestorFromSnapshotOnSameNamespace() throws IOException {
		String jobUID = "jname";
		boolean allowNonRestoredState = false;
		URI checkpointURI = tmp.newFolder().toURI();

		final JobGraph jobGraph = buildJobGraph(jobUID);
		final CheckpointStorageLocation location = buildCheckpointsInDir(checkpointURI.toString(), jobUID, "checkpoint-namespace");

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, "checkpoint-namespace");
		configuration.setString(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, "latest");
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointURI.toString());
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		assertEquals(SavepointRestoreSettings.none(), jobGraph.getSavepointRestoreSettings());
	}

	@Test
	public void testReconfigureRestorFromSnapshotOnDifferentNamespace() throws IOException {
		String jobUID = "jname";
		boolean allowNonRestoredState = true;
		URI checkpointURI = tmp.newFolder().toURI();

		final JobGraph jobGraph = buildJobGraph(jobUID);
		final CheckpointStorageLocation location = buildCheckpointsInDir(checkpointURI.toString(), jobUID, "checkpoint-namespace");

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, "snapshot-namespace");
		configuration.setString(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, "latest");
		configuration.setBoolean(CheckpointingOptions.ALLOW_NON_RESTORED_STATE, allowNonRestoredState);
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointURI.toString());
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		// 'latest' is replaced by the actual checkpoint directory path
		assertEquals(location.getMetadataFilePath().getParent().toString(), jobGraph.getSavepointRestoreSettings().getRestorePath());
		assertEquals(allowNonRestoredState, jobGraph.getSavepointRestoreSettings().allowNonRestoredState());
	}

	@Test
	public void testReconfigureRestorFromSnapshotOnNullRestorePath() throws IOException {
		String jobUID = "jname";
		URI checkpointURI = tmp.newFolder().toURI();

		final JobGraph jobGraph = buildJobGraph(jobUID);
		final CheckpointStorageLocation location = buildCheckpointsInDir(checkpointURI.toString(), jobUID, "checkpoint-namespace");

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, "snapshot-namespace");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, "checkpoint-namespace");
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointURI.toString());
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		// no restore-path settings
		assertEquals(SavepointRestoreSettings.none(), jobGraph.getSavepointRestoreSettings());
	}

	@Test
	public void testReconfigureRestorFromSnapshotOnSpecifiedSnapshotPath() throws IOException {
		String jobUID = "jname";
		URI checkpointURI = tmp.newFolder().toURI();
		URI savepointURI = tmp.newFolder().toURI();

		final JobGraph jobGraph = buildJobGraph(jobUID);
		final CheckpointStorageLocation location = buildCheckpointsInDir(checkpointURI.toString(), jobUID, "checkpoint-namespace");

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, "snapshot-namespace");
		configuration.setString(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, savepointURI.toString());
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointURI.toString());
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		// specified restore-path settings
		assertEquals(savepointURI.toString(), jobGraph.getSavepointRestoreSettings().getRestorePath());
		assertEquals(false, jobGraph.getSavepointRestoreSettings().allowNonRestoredState());
	}

	@Test
	public void testReconfigureRestorFromSnapshotOnDisableCheckpointing() throws IOException {
		String jobUID = "jname";

		final JobGraph jobGraph = buildJobGraphOnDisableCheckpointing(jobUID);

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, "snapshot-namespace");
		configuration.setString(CheckpointingOptions.RESTORE_SAVEPOINT_PATH, "latest");
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		CheckpointConfig.reconfigureRestoreFromSnapshot(jobGraph, configuration);

		// disable checkpionting
		assertEquals(SavepointRestoreSettings.none(), jobGraph.getSavepointRestoreSettings());
	}

	private JobGraph buildJobGraphOnDisableCheckpointing(String jobUID) {
		JobGraph jobGraph = new JobGraph(jobUID);
		jobGraph.setJobUID(jobUID);
		return jobGraph;
	}

	private JobGraph buildJobGraph(String jobUID) {
		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			10L,
			Long.MAX_VALUE,
			Long.MAX_VALUE,
			Integer.MAX_VALUE,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			false,
			0);
		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			Collections.emptyList(),
			Collections.emptyList(),
			Collections.emptyList(),
			checkpointCoordinatorConfiguration,
			null);
		JobGraph jobGraph = new JobGraph(jobUID);
		jobGraph.setJobUID(jobUID);
		jobGraph.setSnapshotSettings(settings);
		return jobGraph;
	}

	private CheckpointStorageLocation buildCheckpointsInDir(String checkpointsDir, String jobUID, String namespace) throws IOException {
		Configuration configuration = new Configuration();
		configuration.set(CheckpointingOptions.CHECKPOINTS_NAMESPACE, namespace);
		configuration.set(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointsDir);

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, classLoader, null);
		FsCheckpointStorage storage = (FsCheckpointStorage) stateBackend.createCheckpointStorage(new JobID(), jobUID);

		CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(1L);
		try (CheckpointMetadataOutputStream out = location.createMetadataOutputStream()) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}
		return location;
	}
}
