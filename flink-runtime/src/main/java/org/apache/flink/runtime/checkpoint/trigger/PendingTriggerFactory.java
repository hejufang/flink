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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Factory used to build {@link PendingTrigger}.
 */
public class PendingTriggerFactory {
	private static final Logger LOG = LoggerFactory.getLogger(PendingTriggerFactory.class);

	/** The associated jobID. **/
	private final JobID job;

	/** Whether to trigger level by level. **/
	private final boolean levelTrigger;

	/** Whether to use fast mode. **/
	private final boolean useFastMode;

	/** Checkpoint related tasks. **/
	private final CheckpointTasks tasksForCheckpoint;

	/** Savepoint related tasks. **/
	private final CheckpointTasks tasksForSavepoint;

	public PendingTriggerFactory(
			JobID job,
			CheckpointTasks tasksForCheckpoint,
			CheckpointTasks tasksForSavepoint,
			boolean useFastMode,
			boolean levelTrigger) {
		this.job = job;
		this.tasksForCheckpoint = tasksForCheckpoint;
		this.tasksForSavepoint = tasksForSavepoint;
		this.useFastMode = useFastMode;
		this.levelTrigger = levelTrigger;
	}

	public boolean isUseFastMode() {
		return useFastMode;
	}

	public PendingTrigger prepareTriggerSavepoint() throws CheckpointException {
		return createPendingTrigger(
			job,
			tasksForSavepoint.getTasksToTrigger(),
			tasksForSavepoint.getTasksToWaitFor(),
			tasksForSavepoint.getTasksToCommitTo(),
			false);
	}

	public PendingTrigger prepareTriggerCheckpoint() throws CheckpointException {
		return createPendingTrigger(
			job,
			tasksForCheckpoint.getTasksToTrigger(),
			tasksForCheckpoint.getTasksToWaitFor(),
			tasksForCheckpoint.getTasksToCommitTo(),
			useFastMode && levelTrigger);
	}

	/**
	 * Create a default trigger that sends rpc to all triggerTasks at once.
	 */
	public static PendingTrigger createPendingTrigger(
		JobID job,
		CheckpointTask[] tasksToTrigger,
		CheckpointTask[] tasksToWaitFor,
		CheckpointTask[] tasksToCommit,
		boolean levelTrigger) throws CheckpointException {
		// check if all tasks that we need to trigger are in right state.
		// if not, abort the checkpoint.
		// only the task need execute would be triggered.
		ArrayList<Execution> triggerTasks = new ArrayList();
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee == null) {
				LOG.info("Checkpoint triggering task {} of job {} is not being executed at the moment. Aborting checkpoint.",
					tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
					job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_IN_RIGHT_STATE);
			} else if (ee.getState() == tasksToTrigger[i].getExpectState()) {
				if (tasksToTrigger[i].needExecute()) {
					triggerTasks.add(ee);
				}
			} else {
				LOG.info("Checkpoint triggering task {} of job {} is not in state {} but {} instead. Aborting checkpoint.",
					tasksToTrigger[i].getTaskNameWithSubtaskIndex(),
					job,
					tasksToTrigger[i].getExpectState(),
					ee.getState());
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_IN_RIGHT_STATE);
			}
		}

		// next, check if all tasks that need to acknowledge the checkpoint are in right state.
		// if not, abort the checkpoint
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>();

		for (CheckpointTask ct : tasksToWaitFor) {
			Execution ee = ct.getCurrentExecutionAttempt();
			if (ee != null && ee.getState() == ct.getExpectState()) {
				if (ct.needExecute()) {
					ackTasks.put(ee.getAttemptId(), ct.getExecutionVertex());
				}
			} else {
				LOG.info("Checkpoint acknowledging task {} of job {} is not being executed at the moment. Aborting checkpoint.",
					ct.getTaskNameWithSubtaskIndex(),
					job);
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}

		ExecutionVertex[] commitTask = Stream.of(tasksToCommit)
			.filter(task -> task.needExecute())
			.map(task -> task.getExecutionVertex())
			.toArray(ExecutionVertex[]::new);

		return levelTrigger ?
			new LevelPendingTrigger(triggerTasks.stream().toArray(Execution[]::new), ackTasks, commitTask) :
			new DefaultPendingTrigger(triggerTasks.stream().toArray(Execution[]::new), ackTasks, commitTask);
	}

	@VisibleForTesting
	public static PendingTrigger createDefaultPendingTrigger(Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm) {
		Execution[] triggerTasks = verticesToConfirm.values()
			.stream()
			.filter(value -> value.getNumberOfInputs() == 0)
			.map(ExecutionVertex::getCurrentExecutionAttempt)
			.collect(Collectors.toList())
			.toArray(new Execution[0]);

		ExecutionVertex[] tasksToCommit = new ArrayList<>(verticesToConfirm.values()).toArray(new ExecutionVertex[0]);
		return new DefaultPendingTrigger(triggerTasks, verticesToConfirm, tasksToCommit);
	}

	/**
	 * Determine which tasks need to trigger Checkpoint.
	 */
	public interface PendingTrigger {

		/** The checkpoint synchronization phase of the task is completed. **/
		void notifyPerformCheckpoint(long checkpointId, long checkpointTimestamp, ExecutionAttemptID executionAttemptID);

		/** Set {@link CheckpointOptions} to trigger Checkpoint. **/
		void setCheckpointOptions(CheckpointOptions checkpointOptions);

		/** Get the task that triggers checkpoint next time. **/
		Execution[] getNextTriggerTasks();

		/** Get tasks that need ack. **/
		Map<ExecutionAttemptID, ExecutionVertex> getAckTasks();

		/** Get tasks that need commit. **/
		ExecutionVertex[] getCommitToTasks();
	}
}
