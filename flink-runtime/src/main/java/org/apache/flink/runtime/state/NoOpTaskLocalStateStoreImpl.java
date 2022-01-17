/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongPredicate;

/**
 * This class implements a {@link TaskLocalStateStore} with no functionality and is used when local recovery is
 * disabled.
 */
public final class NoOpTaskLocalStateStoreImpl implements OwnedTaskLocalStateStore {

	/** Logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(NoOpTaskLocalStateStoreImpl.class);

	/** JobID from the owning subtask. */
	@Nonnull
	private final JobID jobID;

	/** AllocationID of the owning slot. */
	@Nonnull
	private final AllocationID allocationID;

	/** JobVertexID of the owning subtask. */
	@Nonnull
	private final JobVertexID jobVertexID;

	/** Subtask index of the owning subtask. */
	@Nonnegative
	private final int subtaskIndex;

	/** The configuration for local recovery. */
	@Nonnull
	private final LocalRecoveryConfig localRecoveryConfig;

	private final TaskLocalStateListener localStateListener;

	public NoOpTaskLocalStateStoreImpl(
			@Nonnull JobID jobID,
			@Nonnull AllocationID allocationID,
			@Nonnull JobVertexID jobVertexID,
			int subtaskIndex,
			@Nonnull LocalRecoveryConfig localRecoveryConfig,
			TaskLocalStateListener localStateListener) {
		this.jobID = jobID;
		this.allocationID = allocationID;
		this.jobVertexID = jobVertexID;
		this.subtaskIndex = subtaskIndex;
		this.localRecoveryConfig = localRecoveryConfig;
		this.localStateListener = localStateListener;
	}

	@Nonnull
	@Override
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		return localRecoveryConfig;
	}

	@Override
	public CompletableFuture<Void> dispose() {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void storeLocalState(long checkpointId, @Nullable TaskStateSnapshot localState) {
	}

	@Nullable
	@Override
	public TaskStateSnapshot retrieveLocalState(long checkpointID) {
		return null;
	}

	@Override
	public void confirmCheckpoint(long confirmedCheckpointId) {
	}

	@Override
	public void abortCheckpoint(long abortedCheckpointId) {
	}

	@Override
	public void pruneMatchingCheckpoints(LongPredicate matcher) {
	}

	@Override
	public void reportLocalStateSize(long taskLocalStateSize) {
		try {
			localStateListener.notifyTaskLocalStateSize(allocationID, jobID, jobVertexID, subtaskIndex, taskLocalStateSize);
		} catch (Exception e) {
			LOG.warn("Update metrics failed, ignore", e);
		}
	}
}
