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

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This contains executions and states.
 */
public class CheckpointRegion {

	private final ExecutionVertex vertex;

	private final ArrayDeque<RegionStateSnapshot> snapshots;

	public CheckpointRegion(ExecutionVertex[] vertices) {
		if (vertices.length > 1) {
			throw new UnsupportedOperationException("Only support one vertex in region for now.");
		}
		this.vertex = vertices[0];
		this.snapshots = new ArrayDeque<>();
	}

	public void extractSubTaskStateAndSnapshot(CompletedCheckpoint checkpoint) {
		final Map<OperatorID, OperatorState> operatorStates = checkpoint.getOperatorStates();
		final List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs()
				.stream().map(OperatorIDPair::getGeneratedOperatorID).collect(Collectors.toList());
		final int subtaskIndex = vertex.getParallelSubtaskIndex();

		final Map<OperatorID, OperatorSubtaskState> snapshotStates = new HashMap<>();
		for (OperatorID operatorID : operatorIDs) {
			if (operatorStates.containsKey(operatorID)) {
				snapshotStates.put(operatorID, operatorStates.get(operatorID).getState(subtaskIndex));
			}
		}

		snapshots.addFirst(new RegionStateSnapshot(checkpoint.getCheckpointID(), new TaskStateSnapshot(snapshotStates)));
	}

	public Optional<RegionStateSnapshot> findLatestSnapshot() {
		return findLatestSnapshot(Long.MAX_VALUE);
	}

	public Optional<RegionStateSnapshot> findLatestSnapshot(long maxCheckpointId) {
		final Iterator<RegionStateSnapshot> iter = snapshots.iterator();
		if (iter.hasNext()) {
			final RegionStateSnapshot snapshot = iter.next();
			if (snapshot.checkpointId > maxCheckpointId) {
				throw new IllegalStateException(String.format("Snapshot id %s is greater than %s.", snapshot.checkpointId, maxCheckpointId));
			}
			return Optional.of(snapshot);
		} else {
			return Optional.empty();
		}
	}

	public void cleanSnapshots(long checkpointId) {
		while (!snapshots.isEmpty() && snapshots.getLast().checkpointId <= checkpointId) {
			snapshots.pollLast();
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointRegion that = (CheckpointRegion) o;
		return Objects.equals(vertex, that.vertex);
	}

	@Override
	public int hashCode() {
		return Objects.hash(vertex);
	}

	/**
	 * RegionStateSnapshot.
	 */
	public static class RegionStateSnapshot {

		private final long checkpointId;
		private final TaskStateSnapshot snapshot;

		RegionStateSnapshot(long checkpointId, TaskStateSnapshot snapshot) {
			this.checkpointId = checkpointId;
			this.snapshot = snapshot;
		}

		public long getCheckpointId() {
			return checkpointId;
		}

		public TaskStateSnapshot getSnapshot() {
			return snapshot;
		}
	}
}
