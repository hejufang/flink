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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.ProgrammedSlotProvider;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.GatewayDeploymentManager;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link JobDeploymentDescriptor}.
 */
public class JobDeploymentDescriptorTest extends TestLogger {
	private static final String jobName = "job name";
	private static final Configuration jobConfiguration = new Configuration();
	private static final List<PermanentBlobKey> requiredJars = new ArrayList<>(0);
	private static final List<URL> requiredClasspaths = new ArrayList<>(0);
	private static final JobID jobId = new JobID();
	private final SerializedValue<ExecutionConfig> executionConfig = new SerializedValue<>(new ExecutionConfig());

	private final JobInformation jobInformation = new JobInformation(
		jobId, jobName, executionConfig, jobConfiguration, requiredJars, requiredClasspaths);

	private final SerializedValue<JobInformation> serializedJobInformation = new SerializedValue<>(jobInformation);

	public JobDeploymentDescriptorTest() throws IOException {
	}

	@Test
	public void testSerialization() throws Exception {
		final JobDeploymentDescriptor orig =
			createJobDeploymentDescriptor(new TaskDeploymentDescriptor.NonOffloaded<>(serializedJobInformation));

		final JobDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

		assertFalse(orig.getSerializedJobInformation() == copy.getSerializedJobInformation());

		assertEquals(orig.getJobId(), copy.getJobId());
		assertEquals(orig.getSerializedJobInformation(), copy.getSerializedJobInformation());
		assertEquals(orig.getVertexDeploymentDescriptorList(), copy.getVertexDeploymentDescriptorList());
	}

	@Test
	public void testOffLoadedPayload() {
		final JobDeploymentDescriptor jobDeploymentDescriptor =
			createJobDeploymentDescriptor(new TaskDeploymentDescriptor.Offloaded<>(new PermanentBlobKey()));

		try {
			jobDeploymentDescriptor.getSerializedJobInformation();
			fail("Expected to fail since the job information should be offloaded.");
		} catch (IllegalStateException expected) {
			// expected
		}
	}

	@Test
	public void testTaskDeploymentDescriptorEqual() throws Exception {
		Execution execution = createExecution();
		GatewayDeploymentManager gatewayDeploymentManager = new GatewayDeploymentManager(true);
		execution.deploy(true, gatewayDeploymentManager);

		execution.transitionState(ExecutionState.CREATED);
		GatewayDeploymentManager gatewayDeploymentManager1 = new GatewayDeploymentManager(false);
		execution.deploy(true, gatewayDeploymentManager1);

		assertEquals(1, gatewayDeploymentManager.getGatewayJobDeployment().size());
		assertEquals(0, gatewayDeploymentManager.getGatewayDeploymentList().size());

		assertEquals(0, gatewayDeploymentManager1.getGatewayJobDeployment().size());
		assertEquals(1, gatewayDeploymentManager1.getGatewayDeploymentList().size());

		JobDeploymentDescriptor jdd =
			gatewayDeploymentManager.getGatewayJobDeployment().entrySet().iterator().next().getValue().getJobDeploymentDescriptor();
		JobVertexDeploymentDescriptor jvdd = jdd.getVertexDeploymentDescriptorList().get(0);
		JobTaskDeploymentDescriptor jtdd = jvdd.getTaskDeploymentDescriptorList().get(0);
		TaskDeploymentDescriptor tdd =
			gatewayDeploymentManager1.getGatewayDeploymentList().entrySet().iterator().next().getValue().get(0).getTaskDeploymentDescriptor();

		assertEquals(tdd.getJobId(), jdd.getJobId());
		assertEquals(tdd.getSerializedJobInformation(), jdd.getSerializedJobInformation());

		assertEquals(tdd.getSerializedTaskInformation(), jvdd.getSerializedTaskInformation());

		assertEquals(tdd.getSubtaskIndex(), jtdd.getSubtaskIndex());
		assertEquals(tdd.getExecutionAttemptId(), jtdd.getExecutionAttemptId());
		assertEquals(tdd.getAllocationId(), jtdd.getAllocationId());
		assertEquals(tdd.getAttemptNumber(), jtdd.getAttemptNumber());
		assertEquals(tdd.getTaskRestore(), jtdd.getTaskRestore());
		assertEquals(tdd.getTargetSlotNumber(), jtdd.getTargetSlotNumber());
		List<ResultPartitionDeploymentDescriptor> producePartitions =
			JobDeploymentDescriptorHelper.buildResultPartitionDeploymentList(jvdd, jtdd);
		assertEquals(tdd.getProducedPartitions(), producePartitions);

		Set<InputGateDeploymentDescriptor> tddInputGates = new HashSet<>(tdd.getInputGates());
		Set<InputGateDeploymentDescriptor> jddInputGates = new HashSet<>(
			JobDeploymentDescriptorHelper.buildInputGateDeploymentList(jvdd, jtdd));
		assertEquals(tddInputGates, jddInputGates);
	}

	@Test
	public void testSerialize() throws Exception {
		Execution execution = createExecution();
		GatewayDeploymentManager gatewayDeploymentManager = new GatewayDeploymentManager(true);
		execution.deploy(true, gatewayDeploymentManager);
		JobDeploymentDescriptor jdd =
			gatewayDeploymentManager.getGatewayJobDeployment().entrySet().iterator().next().getValue().getJobDeploymentDescriptor();
		JobDeploymentITCases.testJddPerformance(jdd);
		JobDeploymentITCases.testJddCustomSerializePerformance(jdd);
	}

	private Execution createExecution() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final LogicalSlot slot = createTestingLogicalSlot(slotOwner);

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);
		final LogicalSlot logicalSlot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();
		execution.tryAssignResource(logicalSlot);

		return execution;
	}

	@Nonnull
	private JobVertex createNoOpJobVertex() {
		final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID(0, 0));
		jobVertex.setInvokableClass(NoOpInvokable.class);

		return jobVertex;
	}

	private TestingLogicalSlot createTestingLogicalSlot(SlotOwner slotOwner) {
		return new TestingLogicalSlotBuilder()
			.setSlotOwner(slotOwner)
			.createTestingLogicalSlot();
	}

	private static final class SingleSlotTestingSlotOwner implements SlotOwner {

		final CompletableFuture<LogicalSlot> returnedSlot = new CompletableFuture<>();

		public CompletableFuture<LogicalSlot> getReturnedSlotFuture() {
			return returnedSlot;
		}

		@Override
		public void returnLogicalSlot(LogicalSlot logicalSlot) {
			returnedSlot.complete(logicalSlot);
		}
	}

	private JobDeploymentDescriptor createJobDeploymentDescriptor(TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> jobInformation) {
		return new JobDeploymentDescriptor(
			jobId,
			jobInformation);
	}
}
