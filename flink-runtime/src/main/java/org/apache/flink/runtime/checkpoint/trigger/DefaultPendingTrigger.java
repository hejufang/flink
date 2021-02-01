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

import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import java.util.Map;

/**
 * Send rpc to all trigger tasks to trigger checkpoint at once.
 */
public class DefaultPendingTrigger implements PendingTriggerFactory.PendingTrigger {

	private final Execution[] taskToTrigger;
	private final Map<ExecutionAttemptID, ExecutionVertex> ackTasks;
	private final ExecutionVertex[] tasksToCommitTo;

	public DefaultPendingTrigger(
		Execution[] taskToTrigger,
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks,
		ExecutionVertex[] tasksToCommitTo) {
		this.taskToTrigger = taskToTrigger;
		this.ackTasks = ackTasks;
		this.tasksToCommitTo = tasksToCommitTo;
	}

	@Override
	public void notifyPerformCheckpoint(long checkpointId, long checkpointTimestamp, ExecutionAttemptID executionAttemptID) {
		// do nothing
	}

	@Override
	public void setCheckpointOptions(CheckpointOptions checkpointOptions) {
		// do nothing
	}

	@Override
	public Execution[] getNextTriggerTasks() {
		return taskToTrigger;
	}

	@Override
	public Map<ExecutionAttemptID, ExecutionVertex> getAckTasks() {
		return ackTasks;
	}

	@Override
	public ExecutionVertex[] getCommitToTasks() {
		return tasksToCommitTo;
	}
}
