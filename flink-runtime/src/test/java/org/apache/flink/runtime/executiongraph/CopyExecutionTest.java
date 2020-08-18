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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Copy Execution Tests.
 */
public class CopyExecutionTest {

	@Test
	public void testIncompleteSplits() throws Exception {
		final JobVertex jobVertex = createJobVertexWithAssigner();
		final JobGraph jobGraph = new JobGraph();
		jobGraph.addVertex(jobVertex);
		final JobID jobId = new JobID();
		final ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder()
				.setJobGraph(jobGraph)
				.setFutureExecutor(TestingUtils.defaultExecutor())
				.setIoExecutor(TestingUtils.defaultExecutor())
				.setSlotProvider(new SimpleSlotProvider(2))
				.setRestartStrategy(new FixedDelayRestartStrategy(5, 100L))
				.setFailoverStrategyFactory(new MyFailoverStrategy.Factory())
				.build();
		executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		final ExecutionJobVertex ejv = executionGraph.getJobVertex(jobVertex.getID());
		final ExecutionVertex[] vertices = ejv.getTaskVertices();
		final ExecutionVertex vertex1 = vertices[0];
		final ExecutionVertex vertex2 = vertices[1];

		final Execution mainExecution = vertex1.getCurrentExecutionAttempt();
		vertex1.createCopyExecution();
		final Execution copyExecution = vertex1.getCopyExecution();

		// step1. attempt-1 get a new split
		mainExecution.getNextInputSplit();
		vertex2.getCurrentExecutionAttempt().getNextInputSplit();
		Assert.assertEquals(0, MyInputSplitAssigner.splits.size());

		// step2. another exeuction returns some splits
		vertex2.getCurrentExecutionAttempt().markFailed(new RuntimeException());
		Assert.assertEquals(1, MyInputSplitAssigner.splits.size());

		// step3. attempt-2 get two splits from vertex
		copyExecution.getNextInputSplit();
		copyExecution.getNextInputSplit();
		Assert.assertEquals(0, MyInputSplitAssigner.splits.size());

		// step4. attempt-1 finishes
		mainExecution.markFinished();
		Assert.assertSame(mainExecution.getState(), ExecutionState.FAILED);
		Assert.assertSame(copyExecution.getState(), ExecutionState.CANCELED);

		// step5. schedule a new execution
		Assert.assertSame(vertex1.getCurrentExecutionAttempt().getAttemptNumber(), copyExecution.getAttemptNumber() + 1);
		Assert.assertSame(vertex1.getCurrentExecutionAttempt().getState(), ExecutionState.CREATED);
		Assert.assertEquals(2, MyInputSplitAssigner.splits.size());
	}

	@Nonnull
	private JobVertex createJobVertexWithAssigner() {
		final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setInputSplitSource(new MyInputSplitSource());
		jobVertex.setParallelism(2);

		return jobVertex;
	}

	private static class MyInputSplitAssigner implements InputSplitAssigner {

		static Queue<MySplit> splits = new ArrayDeque<>();

		MyInputSplitAssigner() {
			splits.add(new MySplit());
			splits.add(new MySplit());
		}

		@Override
		public InputSplit getNextInputSplit(String host, int taskId) {
			if (splits.isEmpty()) {
				return null;
			}
			return splits.poll();
		}

		@Override
		public void returnInputSplit(List<InputSplit> extras, int taskId) {
			for (InputSplit split : extras) {
				splits.add((MySplit) split);
			}
		}
	}

	private static class MyInputSplitSource implements InputSplitSource<CopyExecutionTest.MySplit> {

		MyInputSplitAssigner assigner = new MyInputSplitAssigner();

		@Override
		public CopyExecutionTest.MySplit[] createInputSplits(int minNumSplits) throws Exception {
			return new CopyExecutionTest.MySplit[0];
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(CopyExecutionTest.MySplit[] inputSplits) {
			return assigner;
		}
	}

	private static class MySplit implements InputSplit {

		@Override
		public int getSplitNumber() {
			return 0;
		}
	}

	private static class MyFailoverStrategy extends FailoverStrategy {

		long version = 1L;

		@Override
		public void onTaskFailure(Execution taskExecution, Throwable cause) {
			taskExecution.cancel();
			taskExecution.completeCancelling();

			if (taskExecution.getVertex().getCopyExecutions().size() > 0) {
				taskExecution.getVertex().getCopyExecution().cancel();
				taskExecution.getVertex().getCopyExecution().completeCancelling();
			}
			try {
				taskExecution.getVertex().resetForNewExecution(1L, ++version);
			} catch (GlobalModVersionMismatch globalModVersionMismatch) {
				throw new RuntimeException(globalModVersionMismatch);
			}
		}

		@Override
		public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {}

		@Override
		public String getStrategyName() {
			return "My Failover Strategy";
		}

		public static class Factory implements FailoverStrategy.Factory {

			@Override
			public FailoverStrategy create(ExecutionGraph executionGraph) {
				return new MyFailoverStrategy();
			}
		}
	}
}
