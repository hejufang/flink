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

import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;


/**
 * Checkpoint related tasks.
 */
public class CheckpointTasks {
	/** Tasks who need to be sent a message when a checkpoint is started. */
	private final ExecutionVertex[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds. */
	private final ExecutionVertex[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed. */
	private final ExecutionVertex[] tasksToCommitTo;

	public CheckpointTasks(
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo) {

		this.tasksToTrigger = tasksToTrigger;
		this.tasksToWaitFor = tasksToWaitFor;
		this.tasksToCommitTo = tasksToCommitTo;
	}

	public ExecutionVertex[] getTasksToTrigger() {
		return tasksToTrigger;
	}

	public ExecutionVertex[] getTasksToWaitFor() {
		return tasksToWaitFor;
	}

	public ExecutionVertex[] getTasksToCommitTo() {
		return tasksToCommitTo;
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
}
