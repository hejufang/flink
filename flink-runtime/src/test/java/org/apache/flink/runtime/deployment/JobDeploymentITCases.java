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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * ITTests for the {@link JobDeploymentDescriptor}.
 */
public class JobDeploymentITCases {
	private static final int ROUND = 500;

	@Test
	public void testGenerateSourceJobDeployment() throws Exception {
		JobID jobId = JobID.generate();
		JobInformation jobInformation = new JobInformation(
			jobId,
			jobId.toHexString(),
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());
		JobDeploymentDescriptor jobDeploymentDescriptor = new JobDeploymentDescriptor(
			jobId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)));

		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateSourceVertex(12));

		List<TaskDeploymentDescriptor> tdds = generateSourceVertexTdd(jobId, jobInformation, 12);

		System.out.println("Source performance test: " + ROUND + " rounds");
		testTddPerformance(tdds);
		testJddPerformance(jobDeploymentDescriptor);
		testJddCustomSerializePerformance(jobDeploymentDescriptor);
	}

	@Test
	public void testGenerateWordCountJobDeployment() throws Exception {
		JobID jobId = JobID.generate();
		JobInformation jobInformation = new JobInformation(
			jobId, jobId.toHexString(),
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());
		JobDeploymentDescriptor jobDeploymentDescriptor = new JobDeploymentDescriptor(
			jobId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateSourceVertex(12));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateAggregateVertex(12));

		List<TaskDeploymentDescriptor> tdds = generateSourceVertexTdd(jobId, jobInformation, 12);
		tdds.addAll(generateAggregateVertexTdd(jobId, jobInformation, 12));

		System.out.println("WordCount performance test: " + ROUND + " rounds");
		testTddPerformance(tdds);
		testJddPerformance(jobDeploymentDescriptor);
		testJddCustomSerializePerformance(jobDeploymentDescriptor);
	}

	@Test
	public void testGenerateJoinJobDeployment() throws Exception {
		JobID jobId = JobID.generate();
		JobInformation jobInformation = new JobInformation(
			jobId, jobId.toHexString(),
			new SerializedValue<>(new ExecutionConfig()),
			new Configuration(),
			Collections.emptyList(),
			Collections.emptyList());
		JobDeploymentDescriptor jobDeploymentDescriptor = new JobDeploymentDescriptor(
			jobId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateSourceVertex(5));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateSourceVertex(5));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateSourceVertex(5));
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateJoinVertex());
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateJoinVertex());
		jobDeploymentDescriptor.addJobVertexDeploymentDescriptor(generateAggregateVertex(5));

		List<TaskDeploymentDescriptor> tdds = generateSourceVertexTdd(jobId, jobInformation, 5);
		tdds.addAll(generateSourceVertexTdd(jobId, jobInformation, 5));
		tdds.addAll(generateSourceVertexTdd(jobId, jobInformation, 5));
		tdds.addAll(generateJoinVertexTdd(jobId, jobInformation));
		tdds.addAll(generateJoinVertexTdd(jobId, jobInformation));
		tdds.addAll(generateAggregateVertexTdd(jobId, jobInformation, 5));

		System.out.println("Join performance test: " + ROUND + " rounds");
		testTddPerformance(tdds);
		testJddPerformance(jobDeploymentDescriptor);
		testJddCustomSerializePerformance(jobDeploymentDescriptor);
	}

	public static void testTddPerformance(List<TaskDeploymentDescriptor> taskDeploymentDescriptorList) throws Exception {
		int dataSize = 0;
		long start = System.currentTimeMillis();
		for (int i = 0; i < ROUND; i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			for (TaskDeploymentDescriptor tdd: taskDeploymentDescriptorList) {
				oos.writeObject(tdd);
			}
			oos.flush();

			if (dataSize == 0) {
				dataSize = baos.size();
			}
			oos.close();
		}
		System.out.println("TDD costs " + (System.currentTimeMillis() - start) + " ms, with size " + dataSize + " bytes");
	}

	public static void testJddPerformance(JobDeploymentDescriptor jobDeploymentDescriptor) throws Exception {
		int dataSize = 0;
		long start = System.currentTimeMillis();
		for (int i = 0; i < ROUND; i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(jobDeploymentDescriptor);
			oos.flush();

			if (dataSize == 0) {
				dataSize = baos.size();
			}
			oos.close();
		}
		System.out.println("JDD costs " + (System.currentTimeMillis() - start) + " ms, with size " + dataSize + " bytes");
	}

	public static void testJddCustomSerializePerformance(JobDeploymentDescriptor jobDeploymentDescriptor) throws Exception {
		int dataSize = 0;
		byte[] bytes = null;
		long start = System.currentTimeMillis();
		for (int i = 0; i < ROUND; i++) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
			jobDeploymentDescriptor.write(dataOutputView);
			baos.flush();
			if (dataSize == 0) {
				dataSize = baos.size();
				bytes = baos.toByteArray();
			}
			baos.close();
		}
		System.out.println("JDD custom costs " + (System.currentTimeMillis() - start) + " ms, with size " + dataSize + " bytes");

		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputView input = new DataInputViewStreamWrapper(bais);
		JobDeploymentDescriptor deserializedRequest = new JobDeploymentDescriptor();
		deserializedRequest.read(input);
		assertEquals(jobDeploymentDescriptor.getJobId(), deserializedRequest.getJobId());
		assertEquals(jobDeploymentDescriptor.getSerializedJobInformation(), deserializedRequest.getSerializedJobInformation());
	}

	private static JobVertexDeploymentDescriptor generateSourceVertex(int taskCount) throws IOException {
		List<JobVertexResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
		JobVertexResultPartitionDeploymentDescriptor vertexResultPartitionDeploymentRequest =
			new JobVertexResultPartitionDeploymentDescriptor(
				new JobVertexPartitionDescriptor(
					new IntermediateDataSetID(),
					128,
					ResultPartitionType.PIPELINED,
					128
				),
				128,
				false
			);
		resultPartitionList.add(vertexResultPartitionDeploymentRequest);

		List<JobVertexInputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
		JobVertexID jobVertexId = new JobVertexID();
		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			jobVertexId.toString(),
			128,
			128,
			DummyInvokable.class.getName(),
			new Configuration());

		JobVertexDeploymentDescriptor source = new JobVertexDeploymentDescriptor(
			jobVertexId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
			resultPartitionList,
			inputGateList);

		for (int i = 0; i < taskCount; i++) {
			List<JobTaskPartitionDescriptor> taskPartitionDescriptorList = new ArrayList<>();
			taskPartitionDescriptorList.add(new JobTaskPartitionDescriptor(
				new IntermediateDataSetID(),
				128));

			JobTaskDeploymentDescriptor taskDeploymentDescriptor = new JobTaskDeploymentDescriptor(
				i,
				new ExecutionAttemptID(),
				new AllocationID(),
				i,
				i,
				null,
				taskPartitionDescriptorList,
				new ArrayList<>());
			source.addJobTaskDeploymentDescriptor(taskDeploymentDescriptor);
		}

		return source;
	}

	private JobVertexDeploymentDescriptor generateAggregateVertex(int taskCount) throws java.io.IOException {
		List<JobVertexResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
		JobVertexResultPartitionDeploymentDescriptor vertexResultPartitionDeploymentRequest =
			new JobVertexResultPartitionDeploymentDescriptor(
				new JobVertexPartitionDescriptor(
					new IntermediateDataSetID(),
					128,
					ResultPartitionType.PIPELINED,
					128
				),
				128,
				false
			);
		resultPartitionList.add(vertexResultPartitionDeploymentRequest);

		List<JobVertexInputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
		List<ShuffleDescriptor> inputShuffleList = new ArrayList<>();
		for (int i = 0; i < 128; i++) {
			inputShuffleList.add(new NettyShuffleDescriptor(
				ResourceID.generate(),
				new NettyShuffleDescriptor.NetworkPartitionConnectionInfo("localhost", i, i),
				new ResultPartitionID(new IntermediateResultPartitionID(new IntermediateDataSetID(), i), new ExecutionAttemptID())));
		}
		inputGateList.add(new JobVertexInputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			inputShuffleList.toArray(new ShuffleDescriptor[0])));

		JobVertexID jobVertexId = new JobVertexID();
		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			jobVertexId.toString(),
			128,
			128,
			DummyInvokable.class.getName(),
			new Configuration());

		JobVertexDeploymentDescriptor vertex = new JobVertexDeploymentDescriptor(
			jobVertexId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
			resultPartitionList,
			inputGateList);

		for (int i = 0; i < taskCount; i++) {
			List<JobTaskPartitionDescriptor> taskPartitionDescriptorList = new ArrayList<>();
			taskPartitionDescriptorList.add(new JobTaskPartitionDescriptor(
				new IntermediateDataSetID(),
				128));

			JobTaskDeploymentDescriptor taskDeploymentDescriptor = new JobTaskDeploymentDescriptor(
				i,
				new ExecutionAttemptID(),
				new AllocationID(),
				i,
				i,
				null,
				taskPartitionDescriptorList,
				new ArrayList<>());
			vertex.addJobTaskDeploymentDescriptor(taskDeploymentDescriptor);
		}

		return vertex;
	}

	private JobVertexDeploymentDescriptor generateJoinVertex() throws Exception {
		List<JobVertexResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
		JobVertexResultPartitionDeploymentDescriptor vertexResultPartitionDeploymentRequest =
			new JobVertexResultPartitionDeploymentDescriptor(
				new JobVertexPartitionDescriptor(
					new IntermediateDataSetID(),
					128,
					ResultPartitionType.PIPELINED,
					128
				),
				128,
				false
			);
		resultPartitionList.add(vertexResultPartitionDeploymentRequest);

		List<JobVertexInputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
		List<ShuffleDescriptor> input1ShuffleList = new ArrayList<>();
		for (int i = 0; i < 128; i++) {
			input1ShuffleList.add(new NettyShuffleDescriptor(
				ResourceID.generate(),
				new NettyShuffleDescriptor.NetworkPartitionConnectionInfo("localhost", i, i),
				new ResultPartitionID(new IntermediateResultPartitionID(new IntermediateDataSetID(), i), new ExecutionAttemptID())));
		}
		List<ShuffleDescriptor> input2ShuffleList = new ArrayList<>();
		for (int i = 0; i < 128; i++) {
			input2ShuffleList.add(new NettyShuffleDescriptor(
				ResourceID.generate(),
				new NettyShuffleDescriptor.NetworkPartitionConnectionInfo("localhost", i, i),
				new ResultPartitionID(new IntermediateResultPartitionID(new IntermediateDataSetID(), i), new ExecutionAttemptID())));
		}
		inputGateList.add(new JobVertexInputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			input1ShuffleList.toArray(new ShuffleDescriptor[0])));
		inputGateList.add(new JobVertexInputGateDeploymentDescriptor(
			new IntermediateDataSetID(),
			ResultPartitionType.PIPELINED,
			input2ShuffleList.toArray(new ShuffleDescriptor[0])));

		JobVertexID jobVertexId = new JobVertexID();
		TaskInformation taskInformation = new TaskInformation(
			jobVertexId,
			jobVertexId.toString(),
			128,
			128,
			DummyInvokable.class.getName(),
			new Configuration());

		JobVertexDeploymentDescriptor vertex = new JobVertexDeploymentDescriptor(
			jobVertexId,
			new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
			resultPartitionList,
			inputGateList);

		for (int i = 0; i < 5; i++) {
			List<JobTaskPartitionDescriptor> taskPartitionDescriptorList = new ArrayList<>();
			taskPartitionDescriptorList.add(new JobTaskPartitionDescriptor(
				new IntermediateDataSetID(),
				128));

			JobTaskDeploymentDescriptor taskDeploymentDescriptor = new JobTaskDeploymentDescriptor(
				i,
				new ExecutionAttemptID(),
				new AllocationID(),
				i,
				i,
				null,
				taskPartitionDescriptorList,
				new ArrayList<>());
			vertex.addJobTaskDeploymentDescriptor(taskDeploymentDescriptor);
		}

		return vertex;
	}

	private static List<TaskDeploymentDescriptor> generateSourceVertexTdd(
			JobID jobId,
			JobInformation jobInformation,
			int taskCount) throws IOException {
		List<TaskDeploymentDescriptor> sources = new ArrayList<>();

		for (int i = 0; i < taskCount; i++) {
			List<ResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
			ResultPartitionDeploymentDescriptor producedPartition =
				new ResultPartitionDeploymentDescriptor(
					new PartitionDescriptor(
						new IntermediateDataSetID(),
						128,
						new IntermediateResultPartitionID(),
						ResultPartitionType.PIPELINED,
						128,
						128,
						null),
					new UnknownShuffleDescriptor(),
					128,
					false
				);
			resultPartitionList.add(producedPartition);

			List<InputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
			JobVertexID jobVertexId = new JobVertexID();
			TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				jobVertexId.toString(),
				128,
				128,
				DummyInvokable.class.getName(),
				new Configuration());

			TaskDeploymentDescriptor source = new TaskDeploymentDescriptor(
				jobId,
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)),
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
				new ExecutionAttemptID(),
				new AllocationID(),
				new ExecutionVertexID(jobVertexId, i),
				i,
				i,
				null,
				resultPartitionList,
				inputGateList);
			sources.add(source);
		}

		return sources;
	}

	private static List<TaskDeploymentDescriptor> generateAggregateVertexTdd(
			JobID jobId,
			JobInformation jobInformation,
			int taskCount) throws IOException {
		List<TaskDeploymentDescriptor> vertices = new ArrayList<>();

		for (int i = 0; i < taskCount; i++) {
			List<ResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
			ResultPartitionDeploymentDescriptor producedPartition =
				new ResultPartitionDeploymentDescriptor(
					new PartitionDescriptor(
						new IntermediateDataSetID(),
						128,
						new IntermediateResultPartitionID(),
						ResultPartitionType.PIPELINED,
						128,
						128,
						null),
					new UnknownShuffleDescriptor(),
					128,
					false
				);
			resultPartitionList.add(producedPartition);

			List<InputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
			List<ShuffleDescriptor> inputShuffleList = new ArrayList<>();
			for (int j = 0; j < 128; j++) {
				inputShuffleList.add(new NettyShuffleDescriptor(
					ResourceID.generate(),
					new NettyShuffleDescriptor.NetworkPartitionConnectionInfo("localhost", j, j),
					new ResultPartitionID(new IntermediateResultPartitionID(new IntermediateDataSetID(), j), new ExecutionAttemptID())));
			}
			inputGateList.add(new InputGateDeploymentDescriptor(
				new IntermediateDataSetID(),
				ResultPartitionType.PIPELINED,
				i,
				inputShuffleList.toArray(new ShuffleDescriptor[0])));

			JobVertexID jobVertexId = new JobVertexID();
			TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				jobVertexId.toString(),
				128,
				128,
				DummyInvokable.class.getName(),
				new Configuration());

			TaskDeploymentDescriptor vertex = new TaskDeploymentDescriptor(
				jobId,
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)),
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
				new ExecutionAttemptID(),
				new AllocationID(),
				new ExecutionVertexID(jobVertexId, i),
				i,
				i,
				null,
				resultPartitionList,
				inputGateList);
			vertices.add(vertex);
		}

		return vertices;
	}

	private static List<TaskDeploymentDescriptor> generateJoinVertexTdd(
			JobID jobId,
			JobInformation jobInformation) throws IOException {
		List<TaskDeploymentDescriptor> vertices = new ArrayList<>();

		for (int i = 0; i < 5; i++) {
			List<ResultPartitionDeploymentDescriptor> resultPartitionList = new ArrayList<>();
			ResultPartitionDeploymentDescriptor producedPartition =
				new ResultPartitionDeploymentDescriptor(
					new PartitionDescriptor(
						new IntermediateDataSetID(),
						128,
						new IntermediateResultPartitionID(),
						ResultPartitionType.PIPELINED,
						128,
						128,
						null),
					new UnknownShuffleDescriptor(),
					128,
					false
				);
			resultPartitionList.add(producedPartition);

			List<InputGateDeploymentDescriptor> inputGateList = new ArrayList<>();
			for (int k = 0; k < 2; k++) {
				List<ShuffleDescriptor> inputShuffleList = new ArrayList<>();
				for (int j = 0; j < 128; j++) {
					inputShuffleList.add(new NettyShuffleDescriptor(
						ResourceID.generate(),
						new NettyShuffleDescriptor.NetworkPartitionConnectionInfo("localhost", j, j),
						new ResultPartitionID(new IntermediateResultPartitionID(new IntermediateDataSetID(), j), new ExecutionAttemptID())));
				}
				inputGateList.add(new InputGateDeploymentDescriptor(
					new IntermediateDataSetID(),
					ResultPartitionType.PIPELINED,
					i,
					inputShuffleList.toArray(new ShuffleDescriptor[0])));
			}

			JobVertexID jobVertexId = new JobVertexID();
			TaskInformation taskInformation = new TaskInformation(
				jobVertexId,
				jobVertexId.toString(),
				128,
				128,
				DummyInvokable.class.getName(),
				new Configuration());

			TaskDeploymentDescriptor vertex = new TaskDeploymentDescriptor(
				jobId,
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(jobInformation)),
				new TaskDeploymentDescriptor.NonOffloaded<>(new SerializedValue<>(taskInformation)),
				new ExecutionAttemptID(),
				new AllocationID(),
				new ExecutionVertexID(jobVertexId, i),
				i,
				i,
				null,
				resultPartitionList,
				inputGateList);
			vertices.add(vertex);
		}

		return vertices;
	}
}
