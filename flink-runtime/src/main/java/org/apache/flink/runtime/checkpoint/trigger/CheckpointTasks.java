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

package org.apache.flink.runtime.checkpoint.trigger;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * Checkpoint related tasks.
 */
public class CheckpointTasks {
	/** Tasks who need to be sent a message when a checkpoint is started. */
	private final CheckpointTask[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds. */
	private final CheckpointTask[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed. */
	private final CheckpointTask[] tasksToCommitTo;

	public CheckpointTasks(
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo) {

		this.tasksToTrigger = transToCheckpointTasks(tasksToTrigger, CheckpointPeriod.TRIGGER);
		this.tasksToWaitFor = transToCheckpointTasks(tasksToWaitFor, CheckpointPeriod.ACK);
		this.tasksToCommitTo = transToCheckpointTasks(tasksToCommitTo, CheckpointPeriod.CONFIRM);
	}

	public CheckpointTask[] getTasksToTrigger() {
		return tasksToTrigger;
	}

	public CheckpointTask[] getTasksToWaitFor() {
		return tasksToWaitFor;
	}

	public CheckpointTask[] getTasksToCommitTo() {
		return tasksToCommitTo;
	}

	public static CheckpointTask[] transToCheckpointTasks(ExecutionVertex[] tasks, CheckpointPeriod checkpointPeriod){
		CheckpointTask[] wrappedTasks = new CheckpointTask[tasks.length];
		for (int i = 0; i < tasks.length; i++) {
			wrappedTasks[i] = wrap(tasks[i], checkpointPeriod);
		}
		return wrappedTasks;
	}

	public static CheckpointTask wrap(ExecutionVertex executionVertex, CheckpointPeriod checkpointPeriod){
		// for bounded vertex, we only need assert whether it has finished, and it would not actually execute trigger、ack or confirm
		if (executionVertex.isBounded()){
			return new CheckpointTask(executionVertex, ExecutionState.FINISHED, false);
		}
		// for default stream vertex, it will really participate in checkpoint and need to confirm that the status is running.
		return new CheckpointTask(executionVertex, ExecutionState.RUNNING, true);
	}

	public void reverseTriggerTasks() {
		ArrayUtils.reverse(tasksToTrigger);
	}

	@Override
	public String toString() {
		return "CheckpointTasks{" +
			"tasksToTrigger=" + Arrays.toString(tasksToTrigger) +
			", tasksToWaitFor=" + Arrays.toString(tasksToWaitFor) +
			", tasksToCommitTo=" + Arrays.toString(tasksToCommitTo) +
			'}';
	}

	private enum CheckpointPeriod {
		TRIGGER,
		ACK,
		CONFIRM;
	}
}
