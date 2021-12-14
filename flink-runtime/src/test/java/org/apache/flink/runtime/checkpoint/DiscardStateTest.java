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
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test for discarding state.
 */
public class DiscardStateTest {
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

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
			store,
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
}
