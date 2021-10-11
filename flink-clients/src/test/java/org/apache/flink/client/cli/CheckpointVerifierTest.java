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

package org.apache.flink.client.cli;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

/**
 * unit test for ClientOptions.
 */
@RunWith(JUnit4.class)
public class CheckpointVerifierTest {
	public Map<OperatorID, OperatorState> operatorStates;
	public Map<JobVertexID, JobVertex> tasks;
	public Random random = new Random();

	@Test
	public void testVerifySuccess() {
		buildSuccessGraph();
		for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, CheckpointVerifyResult> strategy : CheckpointVerifier.getVerifyStrategies()) {
			assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.SUCCESS);
		}
	}

	@Test
	public void testVerifySuccessWithSetUserDefinedOperatorID() {
		buildSuccessGraphWithUserDefinedOperatorID();
		for (BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, CheckpointVerifyResult> strategy : CheckpointVerifier.getVerifyStrategies()) {
			assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.SUCCESS);
		}
	}

	@Test
	public void testVerifyFailWithMissOperatorID() {
		buildFailGraphWithMissOperatorID();
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.FAIL_MISS_OPERATOR_ID);
	}

	@Test
	public void testVerifyFailWithMismatchParallelism() {
		buildFailGraphWithMismatchParallelism(false);
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(1);
		assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.SUCCESS);

		buildFailGraphWithMismatchParallelism(true);
		assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.FAIL_MISMATCH_PARALLELISM);
	}

	@Test
	public void testVerifySuccessWithMissOperatorIDButEmptyOperatorState() {
		buildGraphWithMissOperatorIDWithEmptyOperatorState();
		BiFunction<Map<JobVertexID, JobVertex>, Map<OperatorID, OperatorState>, CheckpointVerifyResult> strategy;
		strategy = CheckpointVerifier.getVerifyStrategies().get(0);
		assertEquals(strategy.apply(tasks, operatorStates), CheckpointVerifyResult.SUCCESS);
	}

	void buildSuccessGraph() {
		tasks = new HashMap<>();
		operatorStates = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			List<OperatorIDPair> operatorIDs = new ArrayList<>();
			for (int j = 0; j < 3; j++) {
				operatorIDs.add(OperatorIDPair.generatedIDOnly(new OperatorID(100 * i, j)));
			}
			JobVertex jobVertex = new JobVertex("vertex-" + i, new JobVertexID(), operatorIDs);
			jobVertex.setParallelism(10);
			jobVertex.setMaxParallelism(100);
			tasks.put(new JobVertexID(), jobVertex);
		}

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 3; j++) {
				if (random.nextBoolean()) {

					OperatorState operatorState = new OperatorState(new OperatorID(100 * i, j), 100, 100);
					operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));

					operatorStates.put(
						new OperatorID(100 * i, j),
						operatorState);
				}
			}
		}
	}

	void buildSuccessGraphWithUserDefinedOperatorID() {
		tasks = new HashMap<>();
		operatorStates = new HashMap<>();
		for (int i = 0; i < 10; i++) {
			List<OperatorIDPair> operatorIDs = new ArrayList<>();
			for (int j = 0; j < 3; j++) {
				operatorIDs.add(OperatorIDPair.of(new OperatorID(20L + i, 20L + j), new OperatorID(100 * i, j)));
			}
			JobVertex jobVertex = new JobVertex("vertex-" + i, new JobVertexID(), operatorIDs);
			jobVertex.setParallelism(10);
			jobVertex.setMaxParallelism(100);
			tasks.put(new JobVertexID(), jobVertex);
		}

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 3; j++) {
				if (random.nextBoolean()) {

					OperatorState operatorState = new OperatorState(new OperatorID(100 * i, j), 100, 100);
					operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));

					operatorStates.put(
						new OperatorID(100 * i, j),
						operatorState);
				}
			}
		}
	}

	void buildFailGraphWithMissOperatorID() {
		buildSuccessGraph();
		// put a never exist OperatorID with coordinator state into operatorStates
		OperatorID operatorID = new OperatorID(10, 0);
		OperatorState operatorState = new OperatorState(operatorID, 100, 100);
		// put coordinator settings to operatorState
		operatorState.setCoordinatorState(new ByteStreamStateHandle("MockHandleName", new byte[5]));
		operatorStates.put(operatorID, operatorState);
	}

	void buildGraphWithMissOperatorIDWithEmptyOperatorState() {
		buildSuccessGraph();
		// put a never exist OperatorID with empty state into operatorStates
		OperatorID operatorID = new OperatorID(10, 0);
		OperatorState operatorState = new OperatorState(operatorID, 100, 100);
		operatorStates.put(operatorID, operatorState);
	}

	void buildFailGraphWithMismatchParallelism(boolean containKeyedState) {
		buildSuccessGraph();
		// put any OperatorState with a small maxParallelism
		OperatorState operatorState = new OperatorState(new OperatorID(100, 0), 1, 1);
		if (containKeyedState) {
			OperatorSubtaskState subtaskState = new OperatorSubtaskState(
				null,
				null,
				new KeyGroupsStateHandle(new KeyGroupRangeOffsets(0, 0), new ByteStreamStateHandle("test-handler", new byte[0])),
				null,
				null,
				null);
			operatorState.putState(0, subtaskState);
		}
		operatorStates.put(new OperatorID(100 * 0, 0), operatorState);
	}
}
