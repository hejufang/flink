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

package org.apache.flink.runtime.deployment;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for the {@link JobTaskDeploymentDescriptor}.
 */
public class JobTaskDeploymentDescriptorTest extends TestLogger {
	private static final int subtaskIndex = 1;
	private static final ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
	private static final AllocationID allocationId = new AllocationID();
	private static final int attemptNumber = 0;
	private static final List<JobTaskInputGateDeploymentDescriptor> inputGates = new ArrayList<>(0);
	private static final List<JobTaskPartitionDescriptor> partitionDescriptors = new ArrayList<>(0);
	private static final int targetSlotNumber = 1234;
	private static final TaskStateSnapshot taskStateHandles = new TaskStateSnapshot();
	private static final JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(1L, taskStateHandles);

	@Test
	public void testSerialization() throws Exception {
		final JobTaskDeploymentDescriptor orig = createJobTaskDeploymentDescriptor();

		final JobTaskDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

		assertFalse(orig.getExecutionAttemptId() == copy.getExecutionAttemptId());
		assertFalse(orig.getTaskRestore() == copy.getTaskRestore());
		assertFalse(orig.getTaskPartitionDescriptorList() == copy.getTaskPartitionDescriptorList());
		assertFalse(orig.getInputGates() == copy.getInputGates());

		assertEquals(orig.getSubtaskIndex(), copy.getSubtaskIndex());
		assertEquals(orig.getExecutionAttemptId(), copy.getExecutionAttemptId());
		assertEquals(orig.getAllocationId(), copy.getAllocationId());
		assertEquals(orig.getAttemptNumber(), copy.getAttemptNumber());
		assertEquals(orig.getTargetSlotNumber(), copy.getTargetSlotNumber());
		assertEquals(orig.getTaskRestore().getRestoreCheckpointId(), copy.getTaskRestore().getRestoreCheckpointId());
		assertEquals(orig.getTaskRestore().getTaskStateSnapshot(), copy.getTaskRestore().getTaskStateSnapshot());
		assertEquals(orig.getTaskPartitionDescriptorList(), copy.getTaskPartitionDescriptorList());
		assertEquals(orig.getInputGates(), copy.getInputGates());
	}

	@Nonnull
	private JobTaskDeploymentDescriptor createJobTaskDeploymentDescriptor() {
		return new JobTaskDeploymentDescriptor(
			subtaskIndex,
			executionAttemptId,
			allocationId,
			attemptNumber,
			targetSlotNumber,
			taskRestore,
			partitionDescriptors,
			inputGates);
	}
}
