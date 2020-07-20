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

package org.apache.flink.runtime.checkpoint.handler;

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * CheckpointHandler for region checkpoints.
 */
public class RegionCheckpointHandler implements CheckpointHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RegionCheckpointHandler.class);

	private final Map<ExecutionVertexID, CheckpointRegion> vertexToRegion;

	private final Map<Long, Set<CheckpointRegion>> checkpointIdToGoodRegions;
	private final Map<Long, Set<CheckpointRegion>> checkpointIdToBadRegions;
	private Map<Long, PendingCheckpoint> checkpointIdToCheckpoint;

	/**
	 * we don't actually need this because it should be promised by CheckpointCoordinator's lock,
	 * but I think it's still needed for future development in case anything changes in some day.
	 */
	private final Object lock = new Object();

	public RegionCheckpointHandler(ExecutionVertex[] vertices) {
		this.vertexToRegion = new HashMap<>();
		constructCheckpointRegions(vertices);
		this.checkpointIdToGoodRegions = new HashMap<>();
		this.checkpointIdToBadRegions = new HashMap<>();
	}

	private void constructCheckpointRegions(ExecutionVertex[] vertices) {
		for (ExecutionVertex vertex : vertices) {
			if (vertex.getAllInputEdges().length > 0) {
				throw new UnsupportedOperationException("Only support one vertex in region for now.");
			}
			vertexToRegion.put(vertex.getID(), new CheckpointRegion(new ExecutionVertex[]{vertex}));
		}
	}

	@Override
	public void loadPendingCheckpoints(Map<Long, PendingCheckpoint> pendingCheckpoints) {
		checkpointIdToCheckpoint = Collections.unmodifiableMap(pendingCheckpoints);
	}

	@Override
	public void onCheckpointComplete(CompletedCheckpoint completedCheckpoint) {
		synchronized (lock) {
			LOG.info("Checkpoint {} is completed, extracting good regions...", completedCheckpoint.getCheckpointID());
			final long checkpointId = completedCheckpoint.getCheckpointID();

			Preconditions.checkState(!checkpointIdToGoodRegions.containsKey(checkpointId), "Checkpoint ID " + checkpointId + " should only succeed once.");
			// find good regions by excluding bad regions...
			checkpointIdToGoodRegions.put(checkpointId, new HashSet<>());
			if (!checkpointIdToBadRegions.containsKey(checkpointId)) {
				checkpointIdToGoodRegions.get(checkpointId).addAll(vertexToRegion.values());
			} else {
				for (CheckpointRegion region : vertexToRegion.values()) {
					if (!checkpointIdToBadRegions.get(checkpointId).contains(region)) {
						checkpointIdToGoodRegions.get(checkpointId).add(region);
					}
				}
			}

			// let every region extracts state from checkpoint and snapshot
			checkpointIdToGoodRegions.get(checkpointId).forEach(checkpointRegion -> checkpointRegion.extractSubTaskStateAndSnapshot(completedCheckpoint));
		}
	}

	@Override
	public boolean tryHandleExpireCheckpoint(PendingCheckpoint pendingCheckpoint) {
		synchronized (lock) {
			LOG.info("Try to handle expired checkpoint {}.", pendingCheckpoint.getCheckpointId());
			// filter those notyet acknowledged tasks and replace the states
			final Collection<ExecutionVertex> notYetKnowledgedTasks = pendingCheckpoint.copyOfNotYetAcknowledgedTasks();
			final long checkpointId = pendingCheckpoint.getCheckpointId();
			for (ExecutionVertex vertex : notYetKnowledgedTasks) {
				final CheckpointRegion region = vertexToRegion.get(vertex.getID());
				if (!checkpointIdToBadRegions.containsKey(checkpointId)) {
					checkpointIdToBadRegions.put(checkpointId, new HashSet<>());
				}
				checkpointIdToBadRegions.get(checkpointId).add(region);

				final Optional<TaskStateSnapshot> snapshotOpt = region.findLatestSnapshot(checkpointId);
				if (snapshotOpt.isPresent()) {
					final PendingCheckpoint.TaskAcknowledgeResult result = pendingCheckpoint
							.replaceTaskStates(vertex.getMainExecution().getAttemptId(), snapshotOpt.get());
					if (!result.equals(PendingCheckpoint.TaskAcknowledgeResult.SUCCESS)) {
						return false;
					}
				} else {
					return false;
				}
			}
			return true;
		}
	}

	@Override
	public boolean tryHandleDeclineMessage(DeclineCheckpoint message) {
		synchronized (lock) {
			LOG.info("Try to handle {}.", message);
			final long checkpointId = message.getCheckpointId();
			if (checkpointIdToCheckpoint.containsKey(checkpointId)) {
				return tryHandleSingleTaskCheckpointFailure(checkpointIdToCheckpoint.get(checkpointId)
						.getTotalTasks().get(message.getTaskExecutionId()), Collections.singleton(checkpointId), false);
			}

			return false;
		}
	}

	@Override
	public boolean tryHandleTasksFailure(Collection<ExecutionVertex> vertices) {
		synchronized (lock) {
			LOG.info("Vertices(size={}) has failed, try to not fail the checkpoint.", vertices.size());
			for (ExecutionVertex vertex : vertices) {
				if (!tryHandleSingleTaskCheckpointFailure(vertex, checkpointIdToCheckpoint.keySet(), true)) {
					return false;
				}
			}

			return true;
		}
	}

	private boolean tryHandleSingleTaskCheckpointFailure(ExecutionVertex vertex, Collection<Long> checkpointIds, boolean taskFailed) {
		assert Thread.holdsLock(lock);
		// all pending checkpoints' region related to the task should be marked
		final CheckpointRegion region = vertexToRegion.get(vertex.getID());
		for (long checkpointId : checkpointIds) {
			if (!checkpointIdToBadRegions.containsKey(checkpointId)) {
				checkpointIdToBadRegions.put(checkpointId, new HashSet<>());
			}
			checkpointIdToBadRegions.get(checkpointId).add(region);
		}

		final ExecutionAttemptID attempt;
		if (taskFailed) {
			// We'r being very strict here because we only allow the task fails only once before replacing state(this can be improved)
			attempt = vertex.getLatestPriorExecution().getAttemptId();
		} else {
			attempt = vertex.getMainExecution().getAttemptId();
		}

		// find latest successful snapshots from region's history
		final Optional<TaskStateSnapshot> snapshotOpt = region.findLatestSnapshot();
		if (snapshotOpt.isPresent()) {
			// ingest snapshot into pending checkpoint
			for (long checkpointId : checkpointIds) {
				final PendingCheckpoint pendingCheckpoint = checkpointIdToCheckpoint.get(checkpointId);
				final PendingCheckpoint.TaskAcknowledgeResult result = pendingCheckpoint.replaceTaskStates(attempt, snapshotOpt.get());
				if (!result.equals(PendingCheckpoint.TaskAcknowledgeResult.SUCCESS)) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean tryHandleAck(AcknowledgeCheckpoint ack) {
		synchronized (lock) {
			Preconditions.checkState(checkpointIdToCheckpoint.containsKey(ack.getCheckpointId()));

			final long checkpointId = ack.getCheckpointId();
			final PendingCheckpoint pendingCheckpoint = checkpointIdToCheckpoint.get(ack.getCheckpointId());

			final String message = String.format("PendingCheckpoint %s does not contain execution %s.", checkpointId, ack.getTaskExecutionId());
			Preconditions.checkState(pendingCheckpoint.getTotalTasks().containsKey(ack.getTaskExecutionId()), message);

			final CheckpointRegion region = vertexToRegion.get(pendingCheckpoint.getTotalTasks().get(ack.getTaskExecutionId()).getID());

			if (!checkpointIdToBadRegions.containsKey(checkpointId)) {
				checkpointIdToBadRegions.put(checkpointId, new HashSet<>());
			}
			return checkpointIdToBadRegions.get(checkpointId).contains(region);
		}
	}

	@Override
	public void clearCheckpoint(long checkpointId) {
		synchronized (lock) {
			checkpointIdToGoodRegions.remove(checkpointId);
			checkpointIdToBadRegions.remove(checkpointId);
		}
	}
}
