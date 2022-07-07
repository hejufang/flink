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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.BatchTaskExecutionState;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import org.junit.Test;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test case for {@link JobDeploymentManager}.
 */
public class JobDeploymentManagerTest {
	@Test
	public void testFinishInvalidJobTaskState() {
		final int taskCount = 100;
		JobDeploymentManager jobDeploymentManager = new JobDeploymentManager(new JobID(), JobMasterId.generate());
		jobDeploymentManager.addDeployTaskCount(taskCount);
		assertThrows("", IllegalArgumentException.class, () -> {
			jobDeploymentManager.finishJobTask(new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FINISHED));
			return null;
		});
	}

	@Test
	public void testFinishAllTasks() {
		final int taskCount = 100;
		final JobID jobId = new JobID();
		JobDeploymentManager jobDeploymentManager = new JobDeploymentManager(jobId, JobMasterId.generate());
		jobDeploymentManager.addDeployTaskCount(taskCount);
		for (int i = 0; i < taskCount - 1; i++) {
			assertFalse(jobDeploymentManager.finishJobTask(new TaskExecutionState(jobId, new ExecutionAttemptID(), ExecutionState.FINISHED)));
		}
		assertTrue(jobDeploymentManager.finishJobTask(new TaskExecutionState(jobId, new ExecutionAttemptID(), ExecutionState.FINISHED)));

		BatchTaskExecutionState batchTaskExecutionState = jobDeploymentManager.getBatchTaskExecutionState();
		assertEquals(jobId, batchTaskExecutionState.getJobId());
		assertEquals(taskCount, batchTaskExecutionState.getExecutionStateList().size());
	}

	@Test
	public void testFailTask() {
		final int taskCount = 100;
		final JobID jobId = new JobID();
		JobDeploymentManager jobDeploymentManager = new JobDeploymentManager(jobId, JobMasterId.generate());
		jobDeploymentManager.addDeployTaskCount(taskCount);
		for (int i = 0; i < taskCount; i++) {
			assertTrue(jobDeploymentManager.finishJobTask(new TaskExecutionState(jobId, new ExecutionAttemptID(), ExecutionState.FAILED)));
		}
	}

	@Test
	public void testCancelTask() {
		final int taskCount = 100;
		final JobID jobId = new JobID();
		JobDeploymentManager jobDeploymentManager = new JobDeploymentManager(jobId, JobMasterId.generate());
		jobDeploymentManager.addDeployTaskCount(taskCount);
		for (int i = 0; i < taskCount; i++) {
			assertTrue(jobDeploymentManager.finishJobTask(new TaskExecutionState(jobId, new ExecutionAttemptID(), ExecutionState.CANCELED)));
		}
	}
}
