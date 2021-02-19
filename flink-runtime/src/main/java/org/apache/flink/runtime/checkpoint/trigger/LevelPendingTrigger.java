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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checkpoint is triggered gradually through rpc by level.
 */
public class LevelPendingTrigger implements PendingTriggerFactory.PendingTrigger {
	private static final Logger LOG = LoggerFactory.getLogger(LevelPendingTrigger.class);

	/** Tasks that need to be triggered, arranged by level. **/
	private final List<Execution[]> taskToTrigger;

	/** Tasks that have not received the perform checkpoint message at a certain level. **/
	private final Map<ExecutionAttemptID, ExecutionVertex> notYetNotifiedTasksByLevel;

	/** Tasks who need to acknowledge a checkpoint before it succeeds. */
	private final Map<ExecutionAttemptID, ExecutionVertex> tasksToAck;

	/** Tasks who need to be sent a message when a checkpoint is confirmed. */
	private final ExecutionVertex[] tasksToCommitTo;

	/** Tasks that have not received the perform Checkpoint message. **/
	private final Set<ExecutionAttemptID> receivedNotifiedTasks;

	/** The synchronization lock of notYetPerformCheckpointTasks. **/
	private final Object lock = new Object();

	/** Options for performing the checkpoint. **/
	private CheckpointOptions checkpointOptions;

	/** Identify which level is currently triggered. **/
	private int currentStage = -1;

	public LevelPendingTrigger(
		Execution[] taskToTrigger,
		Map<ExecutionAttemptID, ExecutionVertex> tasksToAck,
		ExecutionVertex[] tasksToCommitTo) {
		this.taskToTrigger = splitTasksByLevel(taskToTrigger);
		this.notYetNotifiedTasksByLevel = new HashMap<>();
		this.tasksToAck = tasksToAck;
		this.tasksToCommitTo = tasksToCommitTo;
		this.receivedNotifiedTasks = new HashSet<>(taskToTrigger.length);
	}

	@Override
	public void notifyPerformCheckpoint(long checkpointId, long checkpointTimestamp, ExecutionAttemptID executionAttemptID) {
		synchronized (lock) {
			notYetNotifiedTasksByLevel.remove(executionAttemptID);
			receivedNotifiedTasks.add(executionAttemptID);
			if (notYetNotifiedTasksByLevel.isEmpty()) {
				LOG.info("The current level has been triggered and is ready to trigger the next level.");
				while (currentStage < taskToTrigger.size() - 1 && notYetNotifiedTasksByLevel.isEmpty()) {
					for (Execution execution : getNextTriggerTasks()) {
						try {
							if (!receivedNotifiedTasks.contains(execution.getAttemptId())) {
								execution.triggerCheckpoint(checkpointId, checkpointTimestamp, checkpointOptions);
							} else {
								notYetNotifiedTasksByLevel.remove(execution.getAttemptId());
							}
						} catch (Throwable ignore) {
							// just ignore
							LOG.warn("trigger checkpoint for task {} failed, just ignore it.", execution.getVertex().getTaskNameWithSubtaskIndex(), ignore);
						}
					}
				}
			}
		}
	}

	@Override
	public void setCheckpointOptions(CheckpointOptions checkpointOptions) {
		this.checkpointOptions = checkpointOptions;
	}

	@Override
	public Execution[] getNextTriggerTasks() {
		synchronized (lock) {
			if (currentStage < taskToTrigger.size() - 1) {
				currentStage++;
				Arrays.stream(taskToTrigger.get(currentStage))
					.forEach(ee -> notYetNotifiedTasksByLevel.put(ee.getAttemptId(), ee.getVertex()));
				return taskToTrigger.get(currentStage);
			}
			return new Execution[0];
		}
	}

	@Override
	public Map<ExecutionAttemptID, ExecutionVertex> getAckTasks() {
		return tasksToAck;
	}

	@Override
	public ExecutionVertex[] getCommitToTasks() {
		return tasksToCommitTo;
	}

	private List<Execution[]> splitTasksByLevel(Execution[] taskToTrigger) {
		List<Execution[]> tasksToTriggerByLevel = new ArrayList<>();
		int left = 0;
		for (int right = left; right < taskToTrigger.length; right++) {
			if (taskToTrigger[left].getVertex().getJobvertexId() != taskToTrigger[right].getVertex().getJobvertexId()) {
				tasksToTriggerByLevel.add(Arrays.copyOfRange(taskToTrigger, left, right));
				left = right;
			}
		}

		if (left < taskToTrigger.length) {
			tasksToTriggerByLevel.add(Arrays.copyOfRange(taskToTrigger, left, taskToTrigger.length));
		}
		return tasksToTriggerByLevel;
	}
}
