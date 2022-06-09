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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link JobVertexDeploymentDescriptor}.
 */
public class JobVertexDeploymentDescriptorTest extends TestLogger {
	private static final String taskName = "task name";
	private static final int currentNumberOfSubtasks = 1;
	private static final int numberOfKeyGroups = 1;
	private static final Class<? extends AbstractInvokable> invokableClass = NoOpInvokable.class;
	private static final Configuration taskConfiguration = new Configuration();
	private static final JobVertexID vertexID = new JobVertexID();
	private static final List<JobVertexResultPartitionDeploymentDescriptor> vertexResultPartitions = new ArrayList<>(0);
	private static final List<JobVertexInputGateDeploymentDescriptor> inputGates = new ArrayList<>(0);

	private final SerializedValue<TaskInformation> serializedJobVertexInformation = new SerializedValue<>(new TaskInformation(
		vertexID, taskName, currentNumberOfSubtasks, numberOfKeyGroups, invokableClass.getName(), taskConfiguration));

	public JobVertexDeploymentDescriptorTest() throws IOException {
	}

	@Test
	public void testSerialization() throws Exception {
		final JobVertexDeploymentDescriptor orig =
			createJobVertexDeploymentDescriptor(new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobVertexInformation));

		final JobVertexDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

		assertFalse(orig.getSerializedTaskInformation() == copy.getSerializedTaskInformation());
		assertFalse(orig.getAllToAllInputGates() == copy.getAllToAllInputGates());

		assertEquals(orig.getJobVertexId(), copy.getJobVertexId());
		assertEquals(orig.getSerializedTaskInformation(), copy.getSerializedTaskInformation());
		assertEquals(orig.getAllToAllInputGates(), copy.getAllToAllInputGates());
		assertEquals(orig.getTaskDeploymentDescriptorList(), copy.getTaskDeploymentDescriptorList());
	}

	@Test
	public void testOffLoadedPayload() {
		final JobVertexDeploymentDescriptor jobVertexDeploymentDescriptor =
			createJobVertexDeploymentDescriptor(new TaskDeploymentDescriptor.Offloaded<>(new PermanentBlobKey()));

		try {
			jobVertexDeploymentDescriptor.getSerializedTaskInformation();
			fail("Expected to fail since the task information should be offloaded.");
		} catch (IllegalStateException expected) {
			// expected
		}
	}

	@Nonnull
	private JobVertexDeploymentDescriptor createJobVertexDeploymentDescriptor(
			TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> taskInformation) {
		return new JobVertexDeploymentDescriptor(
			vertexID,
			taskInformation,
			vertexResultPartitions,
			inputGates);
	}
}
