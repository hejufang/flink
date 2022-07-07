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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedThrowable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Task execution state list with job id.
 */
public class BatchTaskExecutionState implements Serializable {
	private static final long serialVersionUID = 1L;

	private final JobID jobId;
	private final List<SingleTaskExecutionState> executionStateList;

	public BatchTaskExecutionState(JobID jobId) {
		this.jobId = jobId;
		this.executionStateList = new ArrayList<>(100);
	}

	public void addTaskExecutionState(TaskExecutionState taskExecutionState) {
		executionStateList.add(
			new SingleTaskExecutionState(taskExecutionState.getID(),
				taskExecutionState.getExecutionState(),
				taskExecutionState.getThrowable(),
				taskExecutionState.getAccumulators(),
				taskExecutionState.getIOMetrics()));
	}

	public JobID getJobId() {
		return jobId;
	}

	public List<TaskExecutionState> getExecutionStateList() {
		List<TaskExecutionState> taskExecutionStateList = new ArrayList<>(executionStateList.size());
		for (SingleTaskExecutionState singleTaskExecutionState : executionStateList) {
			taskExecutionStateList.add(new TaskExecutionState(
				jobId,
				singleTaskExecutionState.executionId,
				singleTaskExecutionState.executionState,
				singleTaskExecutionState.throwable,
				singleTaskExecutionState.accumulators,
				singleTaskExecutionState.ioMetrics));
		}
		return taskExecutionStateList;
	}

	private static class SingleTaskExecutionState implements Serializable {
		private static final long serialVersionUID = 1L;

		private final ExecutionAttemptID executionId;

		private final ExecutionState executionState;

		private final SerializedThrowable throwable;

		/** Serialized user-defined accumulators */
		private final AccumulatorSnapshot accumulators;

		private final IOMetrics ioMetrics;

		public SingleTaskExecutionState(
				ExecutionAttemptID executionId,
				ExecutionState executionState,
				SerializedThrowable throwable,
				AccumulatorSnapshot accumulators,
				IOMetrics ioMetrics) {
			this.executionId = executionId;
			this.executionState = executionState;
			this.throwable = throwable;
			this.accumulators = accumulators;
			this.ioMetrics = ioMetrics;
		}
	}

	public CompletableFuture<Acknowledge> batchUpdateTaskExecutionState(JobMasterGateway jobMasterGateway) {
		List<TaskExecutionState> taskExecutionStateList = this.getExecutionStateList();
		List<CompletableFuture<Acknowledge>> futureList = new ArrayList<>(taskExecutionStateList.size());
		for (TaskExecutionState taskExecutionState : taskExecutionStateList) {
			futureList.add(jobMasterGateway.updateTaskExecutionState(taskExecutionState));
		}
		return FutureUtils.completeAll(futureList).thenApply(v -> Acknowledge.get());
	}
}
