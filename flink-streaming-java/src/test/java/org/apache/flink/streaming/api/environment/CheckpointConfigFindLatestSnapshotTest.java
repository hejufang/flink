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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.RESTORE_FROM_LATEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for finding latest snapshot {@link CheckpointConfig}.
 */
public class CheckpointConfigFindLatestSnapshotTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testReconfigureLatestSnapshot() throws IOException {
		String jobName = "jname";
		String jobUID = jobName;
		long checkpointInterval = 30000L;
		String namespace = "ns";
		String statebackend = "filesystem";
		String emptyNamespace = "emptyns";
		boolean allowNonRestoredState = false;
		URI checkpointURI = tmp.newFolder().toURI();

		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			checkpointInterval,
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
		JobGraph jobGraph = new JobGraph(jobName);
		jobGraph.setJobUID(jobUID);
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(RESTORE_FROM_LATEST, allowNonRestoredState));
		jobGraph.setSnapshotSettings(settings);
		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, namespace);
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, namespace);
		configuration.setString(CheckpointingOptions.STATE_BACKEND, statebackend);
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointURI.toString());
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, classLoader, null);
		FsCheckpointStorage storageOld = (FsCheckpointStorage) stateBackend.createCheckpointStorage(new JobID(), jobUID);
		// create chk-1 in checkpoint dir
		CheckpointStorageLocation location = storageOld.initializeLocationForCheckpoint(1L);

		try (CheckpointMetadataOutputStream out = location.createMetadataOutputStream()) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, emptyNamespace);
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, emptyNamespace);

		CheckpointConfig.reconfigureLatestSnapshot(jobGraph, configuration);

		// 'latest' is replaced by the actual checkpoint directory path
		assertFalse(RESTORE_FROM_LATEST.equals(jobGraph.getSavepointRestoreSettings().getRestorePath()));
		assertEquals(location.getMetadataFilePath().getParent().toString(), jobGraph.getSavepointRestoreSettings().getRestorePath());
		assertEquals(allowNonRestoredState, jobGraph.getSavepointRestoreSettings().allowNonRestoredState());
	}
}
