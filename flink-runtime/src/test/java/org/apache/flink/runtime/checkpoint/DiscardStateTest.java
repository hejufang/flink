/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.mockExecutionVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for discarding state.
 */
public class DiscardStateTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

	@Before
	public void setUp() throws Exception {
		manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
	}

	@Test
	public void testDiscardHistoricalInvalidCheckpoint() throws Exception {
		String jobUID = "jobUID";
		String namespace = "ns";
		Path checkpointParentPath = new Path(tmp.newFolder(jobUID, "ns").getPath());
		Path chk1 = new Path(tmp.newFolder(jobUID, namespace, "chk-1").getPath());
		Path chk2 = new Path(tmp.newFolder(jobUID, namespace, "chk-2").getPath());
		Path chk4 = new Path(tmp.newFolder(jobUID, namespace, "chk-4").getPath());
		Path chk5 = new Path(tmp.newFolder(jobUID, namespace, "chk-5").getPath());
		Path chk6 = new Path(tmp.newFolder(jobUID, namespace, "chk-6").getPath());
		Path chk7 = new Path(tmp.newFolder(jobUID, namespace, "chk-7").getPath());
		StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(5);

		store.addCheckpoint(new CompletedCheckpoint(
			new JobID(),
			5,
			0,
			0,
			Collections.<OperatorID, OperatorState>emptyMap(),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation()));

		store.addCheckpoint(new CompletedCheckpoint(
			new JobID(),
			7,
			0,
			0,
			Collections.<OperatorID, OperatorState>emptyMap(),
			Collections.<MasterState>emptyList(),
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
			new TestCompletedCheckpointStorageLocation()));

		long currentTimestamp = System.currentTimeMillis();
		TreeMap<Long, Long> abortedPendingCheckpoints = new TreeMap<>();
		abortedPendingCheckpoints.put(1L, currentTimestamp - 10 * 60 * 1000);
		abortedPendingCheckpoints.put(2L, currentTimestamp - 9 * 60 * 1000);
		abortedPendingCheckpoints.put(3L, currentTimestamp - 8 * 60 * 1000);
		abortedPendingCheckpoints.put(4L, currentTimestamp - 7 * 60 * 1000);
		abortedPendingCheckpoints.put(6L, currentTimestamp - 2 * 60 * 1000);

		FileSystem fs = checkpointParentPath.getFileSystem();
		LocalDate currentDate = LocalDate.now();
		String dateSubDir = String.format("%04d%02d%02d", currentDate.getYear(), currentDate.getMonthValue(), currentDate.getDayOfMonth());
		Path expiredDirectory = new Path(tmp.newFolder("expire", dateSubDir, jobUID, namespace).getPath());

		StateUtil.discardHistoricalInvalidCheckpoint(
			checkpointParentPath,
			abortedPendingCheckpoints,
			3,
			300000,
			expiredDirectory.getPath(),
			new ArrayList<FileStatus>(),
			new AtomicInteger(0),
			Executors.directExecutor(),
			null
		);

		Path movedChk1 = new Path(expiredDirectory, "chk-1");
		Path movedChk2 = new Path(expiredDirectory, "chk-2");
		Path movedChk4 = new Path(expiredDirectory, "chk-4");
		Assert.assertFalse(fs.exists(chk1));
		Assert.assertFalse(fs.exists(chk2));
		Assert.assertFalse(fs.exists(chk4));
		Assert.assertTrue(fs.exists(chk5));
		Assert.assertTrue(fs.exists(chk6));
		Assert.assertTrue(fs.exists(chk7));
		Assert.assertTrue(fs.exists(movedChk1));
		Assert.assertTrue(fs.exists(movedChk2));
		Assert.assertTrue(fs.exists(movedChk4));
	}

	@Test
	public void testDiscardHistoricalInvalidChkDirectoriesAfterJobRestore() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path checkpointParentPath = new Path(checkpointBaseDir.getPath());
		FileSystem fs = checkpointParentPath.getFileSystem();
		LocalDate currentDate = LocalDate.now();
		String dateSubDir = String.format("%04d%02d%02d", currentDate.getYear(), currentDate.getMonthValue(), currentDate.getDayOfMonth());
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());
		Path expiredDirectory = new Path(expiredPrefix, dateSubDir);

		final JobID jid = new JobID();
		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptId);

		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);

		// trigger 6 checkpoints, these cps should all succeed
		for (long cpId = 1; cpId <= 6; cpId++) {
			triggerCpAndReceiveACK(coord, cpId, jid, attemptId);
		}

		// checkpoint 1~6 all succeed
		// when the job fails, the next expected checkpoint id should be 7 after recovery
		CheckpointCoordinator coord1 = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);
		coord1.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true, Thread.currentThread().getContextClassLoader());
		assertEquals(7, coord1.getCheckpointIdCounter().get());

		// trigger the cp 7, this should not succeed with dirty directory chk-7
		coord.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();

		// the latest recovered checkpoint id is 6
		// but the dirty directory chk-7 exists
		// so the next triggered checkpoint id should be 8 to prevent files from being deleted by mistake
		CheckpointCoordinator coord2 = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);
		coord2.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true, Thread.currentThread().getContextClassLoader());
		assertEquals(8, coord2.getCheckpointIdCounter().get());

		// trigger cp 8 and receive ack
		triggerCpAndReceiveACK(coord2, 8, jid, attemptId);
		// after cp-8 finished, the dirty directory chk-7 should be moved to the expired directory
		assertFalse(fs.exists(new Path(checkpointParentPath,  "/test/namespace/chk-7")));
		assertTrue(fs.exists(new Path(expiredDirectory, "/test/namespace/chk-7")));
		// the chk-8 should exist
		assertTrue(fs.exists(new Path(checkpointParentPath, "/test/namespace/chk-8")));

		// trigger 3 checkpoints, these cps should all succeed
		for (long cpId = 9; cpId <= 11; cpId++) {
			triggerCpAndReceiveACK(coord2, cpId, jid, attemptId);
		}
		// the chk-8 should NOT exist
		assertFalse(fs.exists(new Path(checkpointParentPath, "/test/namespace/chk-8")));
		// the chk-9 should exist
		assertTrue(fs.exists(new Path(checkpointParentPath, "/test/namespace/chk-9")));
		// the chk-10 should exist
		assertTrue(fs.exists(new Path(checkpointParentPath, "/test/namespace/chk-10")));
		// the chk-11 should exist
		assertTrue(fs.exists(new Path(checkpointParentPath, "/test/namespace/chk-11")));
	}

	// test case that checks the recovered id when
	// 1) id on store > id on storage
	// 2) id on store = id on storage
	// 3) id on store < id on storage
	@Test
	public void testRecoveredCheckpointIdWhenStorageHasNoDirtyChks() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());
		Path savepointPath = new Path(tmp.newFolder("savepoint").getPath());

		final JobID jid = new JobID();
		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptId);

		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);

		// HDFS and ZK both have no cps
		assertId(1, checkpointBaseDir, expiredPrefix);

		triggerCpAndReceiveACK(coord, 1, jid, attemptId);
		triggerSpAndReceiveACK(coord, 2, savepointPath.getPath(), jid, attemptId);
		triggerCpAndReceiveACK(coord, 3, jid, attemptId);
		triggerCpAndReceiveACK(coord, 4, jid, attemptId);

		// Case 1: CHECKPOINT is the last successful cp
		// id on store > id on storage (the latest used id)
		assertId(5, 5, checkpointBaseDir, expiredPrefix);
		assertId(100, 100, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(5, 2, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(5, 4, checkpointBaseDir, expiredPrefix);

		triggerSpAndReceiveACK(coord, 121, savepointPath.getPath(), jid, attemptId);

		// Case 2: SAVEPOINT is the last successful cp
		// id on store > id on storage (the latest used id)
		assertId(122, 122, checkpointBaseDir, expiredPrefix);
		assertId(1000, 1000, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(122, 1, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(122, 121, checkpointBaseDir, expiredPrefix);

		// Case 3: HDFS is empty, but zk counter is not reset
		// mock that the cp is failed all the time
		assertEquals(122, coord.getCheckpointIdCounter().get());
		// remove chks
		FileUtils.deleteFileOrDirectory(new File(checkpointBaseDir.getPath() + "/test/namespace"));
		coord.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true, Thread.currentThread().getContextClassLoader());
		assertEquals(122, coord.getCheckpointIdCounter().get());
	}

	@Test
	public void testRecoveredCheckpointIdWhenStorageHasSomeDirtyChks() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());
		Path savepointPath = new Path(tmp.newFolder("savepoint").getPath());

		final JobID jid = new JobID();
		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptId);

		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);
		triggerCpAndReceiveACK(coord, 1, jid, attemptId);
		// dirty sp-2
		tmp.newFolder("test", "namespace", "sp-2");
		triggerSpAndReceiveACK(coord, 3, savepointPath.getPath(), jid, attemptId);
		// dirty chk-4
		tmp.newFolder("test", "namespace", "chk-4");

		// Case 1: CHECKPOINT is the last DIRTY cp
		// id on store > id on storage (the latest used id)
		assertId(5, 5, checkpointBaseDir, expiredPrefix);
		assertId(100, 100, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(5, 2, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(5, 4, checkpointBaseDir, expiredPrefix);

		// dirty sp-5
		tmp.newFolder("test", "namespace", "sp-5");

		// Case 2: SAVEPOINT is the last DIRTY cp
		// id on store > id on storage (the latest used id)
		assertId(6, 6, checkpointBaseDir, expiredPrefix);
		assertId(100, 100, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(6, 2, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(6, 5, checkpointBaseDir, expiredPrefix);

		triggerCpAndReceiveACK(coord, 100, jid, attemptId);

		// Case 3: CHECKPOINT is the last SUCCESSFUL cp
		// id on store > id on storage (the latest used id)
		assertId(101, 101, checkpointBaseDir, expiredPrefix);
		assertId(200, 200, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(101, 20, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(101, 100, checkpointBaseDir, expiredPrefix);

		triggerSpAndReceiveACK(coord, 200, savepointPath.getPath(), jid, attemptId);

		// Case 4: SAVEPOINT is the last SUCCESSFUL cp
		// id on store > id on storage (the latest used id)
		assertId(201, 201, checkpointBaseDir, expiredPrefix);
		assertId(500, 500, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(201, 1, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(201, 200, checkpointBaseDir, expiredPrefix);
	}

	@Test
	public void testRecoveredCheckpointIdWhenStorageChksAreAllDirty() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());

		tmp.newFolder("test", "namespace", "chk-1");
		tmp.newFolder("test", "namespace", "sp-2");
		tmp.newFolder("test", "namespace", "sp-3");
		tmp.newFolder("test", "namespace", "chk-4");

		// Case 1: CHECKPOINT is the last dirty cp
		// id on store > id on storage (the latest used id)
		assertId(5, 5, checkpointBaseDir, expiredPrefix);
		assertId(100, 100, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(5, 2, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(5, 4, checkpointBaseDir, expiredPrefix);

		tmp.newFolder("test", "namespace", "sp-1234");

		// Case 2: SAVEPOINT is the last dirty cp
		// id on store > id on storage (the latest used id)
		assertId(1235, 1235, checkpointBaseDir, expiredPrefix);
		// id on store < id on storage (the latest used id)
		assertId(1235, 1000, checkpointBaseDir, expiredPrefix);
		// id on store = id on storage (the latest used id)
		assertId(1235, 1234, checkpointBaseDir, expiredPrefix);
	}

	// test the recovered id when
	// 1) job recovered from the same namespace
	// 2) job recovered from a new namespace
	// 3) job recovered from a new namespace and referred to a checkpoint
	@Test
	public void testRecoveredCheckpointIdWithNamespaceWhenStorageHasNoDirtyChks() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());
		Path savepointPath = new Path(tmp.newFolder("savepoint").getPath());

		final JobID jid = new JobID();
		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptId);

		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);
		triggerCpAndReceiveACK(coord, 101, jid, attemptId);
		triggerSpAndReceiveACK(coord, 102, savepointPath.getPath(), jid, attemptId);
		triggerCpAndReceiveACK(coord, 103, jid, attemptId);

		// Case 1: when the job restarts with the same namespace
		assertId(104, checkpointBaseDir, expiredPrefix);
		// Case 2: when the job restarts with a new namespace without a recovered checkpoints
		assertId(1, tmp.newFolder("new-namespace"), expiredPrefix);
		// Case 3: when the job restarts with a new namespace and refers a recovered checkpoints (chk-108),
		// the next expected checkpoint id should be 109 after recovery
		File ns1 = tmp.newFolder("new-namespace1");
		CheckpointCoordinator coord1 = getCheckpointCoordinator(jid, vertex, ns1, expiredPrefix);
		coord1.restoreSavepoint(checkpointBaseDir.getPath() + "/test/namespace/chk-101/_metadata", true, new TreeMap<>(), Thread.currentThread().getContextClassLoader());
		assertEquals(102, coord1.getCheckpointIdCounter().get());
	}

	@Test
	public void testRecoveredCheckpointIdWithNamespaceWhenStorageHasSomeDirtyChks() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());
		Path savepointPath = new Path(tmp.newFolder("savepoint").getPath());

		final JobID jid = new JobID();
		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		ExecutionVertex vertex = mockExecutionVertex(attemptId);

		CheckpointCoordinator coord = getCheckpointCoordinator(jid, vertex, checkpointBaseDir, expiredPrefix);
		triggerCpAndReceiveACK(coord, 1, jid, attemptId);
		triggerSpAndReceiveACK(coord, 2, savepointPath.getPath(), jid, attemptId);
		// dirty sp-3
		tmp.newFolder("test", "namespace", "sp-3");
		// dirty chk-4
		tmp.newFolder("test", "namespace", "chk-4");

		// Case 1: when the job restarts with the same namespace
		assertId(5, checkpointBaseDir, expiredPrefix);
		// Case 2: when the job restarts with a new namespace without a recovered checkpoints
		assertId(1, tmp.newFolder("new-namespace1"), expiredPrefix);
		// Case 3: when the job restarts with a new namespace and refers a recovered checkpoints (chk-1),
		// the next expected checkpoint id should be 2 after recovery
		File ns1 = tmp.newFolder("new-namespace2");
		CheckpointCoordinator coord1 = getCheckpointCoordinator(jid, vertex, ns1, expiredPrefix);
		coord1.restoreSavepoint(checkpointBaseDir.getPath() + "/test/namespace/chk-1/_metadata", true, new TreeMap<>(), Thread.currentThread().getContextClassLoader());
		assertEquals(2, coord1.getCheckpointIdCounter().get());
	}

	@Test
	public void testRecoveredCheckpointIdWithNamespaceWhenStorageChksAreAllDirty() throws Exception {
		File checkpointBaseDir = tmp.getRoot();
		Path expiredPrefix = new Path(tmp.newFolder("expire").getPath());

		tmp.newFolder("test", "namespace", "chk-101");
		tmp.newFolder("test", "namespace", "chk-102");
		tmp.newFolder("test", "namespace", "sp-103");
		tmp.newFolder("test", "namespace", "sp-104");
		tmp.newFolder("test", "namespace", "chk-105");

		// when the job restarts with the same namespace
		assertId(106, checkpointBaseDir, expiredPrefix);
		// when the job restarts with a new namespace without a recovered checkpoints
		assertId(1, tmp.newFolder("new-namespace"), expiredPrefix);

		tmp.newFolder("test", "namespace", "sp-200");
		// when the job restarts with the same namespace
		assertId(201, checkpointBaseDir, expiredPrefix);
		// when the job restarts with a new namespace without a recovered checkpoints
		assertId(1, tmp.newFolder("new-namespace1"), expiredPrefix);
	}

	private void assertId(long expectedId, File checkpointBaseDir, Path expiredPrefix) throws Exception {
		assertId(expectedId, -1, checkpointBaseDir, expiredPrefix);
	}

	private void assertId(long expectedId, long zkId, File checkpointBaseDir, Path expiredPrefix) throws Exception {
		CheckpointCoordinator coord =
			getCheckpointCoordinator(
				new JobID(),
				mockExecutionVertex(new ExecutionAttemptID()),
				checkpointBaseDir,
				expiredPrefix);

		// when zkId <= 0, we do not set the id counter manually
		if (zkId > 0) {
			coord.getCheckpointIdCounter().setCount(zkId);
		}
		coord.restoreLatestCheckpointedStateToAll(Collections.emptySet(), true, Thread.currentThread().getContextClassLoader());
		assertEquals(expectedId, coord.getCheckpointIdCounter().get());
	}

	private CheckpointCoordinator getCheckpointCoordinator(JobID jobID, ExecutionVertex vertex, File checkpointBaseDir, Path expiredDirectoryPrefix) {
		CheckpointCoordinatorConfiguration chkConfig = CheckpointCoordinatorConfiguration.builder()
			.setCheckpointInterval(1000L)
			.setCheckpointTimeout(Long.MAX_VALUE)
			.setMaxConcurrentCheckpoints(1)
			.setMinPauseBetweenCheckpoints(0)
			.setCheckpointRetentionPolicy(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)
			.setExactlyOnce(true)
			.setUnalignedCheckpointsEnabled(false)
			.setPreferCheckpointForRecovery(false)
			.setTolerableCheckpointFailureNumber(0)
			.build();

		chkConfig.setDiscardHistoricalCheckpointConfiguration(new DiscardHistoricalCheckpointConfiguration(
			3,
			300000,
			expiredDirectoryPrefix.getPath()
		));

		Configuration configuration = new Configuration();
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, "namespace");

		return new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
			.setTimer(manuallyTriggeredScheduledExecutor)
			.setJobId(jobID)
			.setCheckpointStateBackend(new FsStateBackend(checkpointBaseDir.toURI()).configure(configuration, null))
			.setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(3))
			.setCheckpointCoordinatorConfiguration(chkConfig)
			.setTasksToTrigger(new ExecutionVertex[]{vertex})
			.setTasksToWaitFor(new ExecutionVertex[]{vertex})
			.setTasksToCommitTo(new ExecutionVertex[]{vertex})
			.setIoExecutor(Executors.directExecutor())
			.build();
	}

	private void triggerCpAndReceiveACK(CheckpointCoordinator checkpointCoordinator, long checkpointId, JobID jid, ExecutionAttemptID attemptId) throws Exception {
		checkpointCoordinator.getCheckpointIdCounter().setCount(checkpointId);
		// trigger checkpoint
		final CompletableFuture<CompletedCheckpoint> checkpointFuture = checkpointCoordinator.triggerCheckpoint(false);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());
		// acknowledge from the task
		checkpointCoordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptId, checkpointId), "Unknown location");
	}

	private void triggerSpAndReceiveACK(CheckpointCoordinator checkpointCoordinator, long checkpointId, String savepointPath, JobID jid, ExecutionAttemptID attemptId) throws Exception {
		checkpointCoordinator.getCheckpointIdCounter().setCount(checkpointId);
		// trigger checkpoint
		final CompletableFuture<CompletedCheckpoint> checkpointFuture = checkpointCoordinator.triggerSavepoint(savepointPath);
		manuallyTriggeredScheduledExecutor.triggerAll();
		assertFalse(checkpointFuture.isCompletedExceptionally());
		// acknowledge from the task
		checkpointCoordinator.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, attemptId, checkpointId), "Unknown location");
	}
}
