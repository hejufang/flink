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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.checkpoint.savepoint.simple_savepoint.SavepointSimpleMetadata;
import org.apache.flink.runtime.checkpoint.trigger.PendingTriggerFactory;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been
 * acknowledged by all tasks that need to acknowledge it. Once all tasks have
 * acknowledged it, it becomes a {@link CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the
 * state handles always as serialized values, never as actual values.
 */
public class PendingCheckpoint {

	/**
	 * Result of the {@link PendingCheckpoint#acknowledgedTasks} method.
	 */
	public enum TaskAcknowledgeResult {
		SUCCESS, // successful acknowledge of the task
		DUPLICATE, // acknowledge message is a duplicate
		UNKNOWN, // unknown task acknowledged
		DISCARDED // pending checkpoint has been discarded
	}

	// ------------------------------------------------------------------------

	/** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	private final Object lock = new Object();

	private final JobID jobId;

	private final long checkpointId;

	private final long checkpointTimestamp;

	private final Map<OperatorID, OperatorState> operatorStates;

	private final Map<ExecutionAttemptID, ExecutionVertex> totalTasks;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final List<MasterState> masterState;

	/** Set of acknowledged tasks. */
	private final Set<ExecutionAttemptID> acknowledgedTasks;

	/** The checkpoint properties. */
	private final CheckpointProperties props;

	/** Target storage location to persist the checkpoint metadata to. */
	private final CheckpointStorageLocation targetLocation;

	/** Target storage location to persist the savepoint simple metadata in checkpoint dir to. */
	@Nullable
	private final CheckpointStorageCoordinatorView checkpointStorage;

	/** The promise to fulfill once the checkpoint has been completed. */
	private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

	/** The executor for potentially blocking I/O operations, like state disposal. */
	private final Executor executor;

	/** The stream we used to persist metadata. */
	private final AtomicReference<CheckpointMetadataOutputStream> metadataOutputStream;

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	private volatile ScheduledFuture<?> cancellerHandle;

	private final PendingTriggerFactory.PendingTrigger pendingTrigger;

	private final int numNeedAcknowledgedSubtasks;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
		JobID jobId,
		long checkpointId,
		long checkpointTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		CheckpointProperties props,
		CheckpointStorageLocation targetLocation,
		Executor executor) {

		this(
			jobId,
			checkpointId,
			checkpointTimestamp,
			verticesToConfirm,
			props,
			targetLocation,
			executor,
			PendingTriggerFactory.createDefaultPendingTrigger(verticesToConfirm));
	}

	public PendingCheckpoint(
		JobID jobId,
		long checkpointId,
		long checkpointTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		CheckpointProperties props,
		CheckpointStorageLocation targetLocation,
		Executor executor,
		PendingTriggerFactory.PendingTrigger pendingTrigger) {

		this(jobId,
			checkpointId,
			checkpointTimestamp,
			verticesToConfirm,
			props,
			targetLocation,
			executor,
			pendingTrigger,
			null);
	}

	public PendingCheckpoint(
		JobID jobId,
		long checkpointId,
		long checkpointTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		CheckpointProperties props,
		CheckpointStorageLocation targetLocation,
		Executor executor,
		PendingTriggerFactory.PendingTrigger pendingTrigger,
		@Nullable CheckpointStorageCoordinatorView checkpointStorage) {

		checkArgument(verticesToConfirm.size() > 0,
				"Checkpoint needs at least one vertex that commits the checkpoint");

		this.jobId = checkNotNull(jobId);
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.notYetAcknowledgedTasks = checkNotNull(verticesToConfirm);
		this.totalTasks = new HashMap<>(verticesToConfirm);
		this.props = checkNotNull(props);
		this.targetLocation = checkNotNull(targetLocation);
		this.executor = Preconditions.checkNotNull(executor);

		this.operatorStates = new HashMap<>();
		this.masterState = new ArrayList<>();
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = new CompletableFuture<>();
		this.metadataOutputStream = new AtomicReference<>(null);
		this.pendingTrigger = pendingTrigger;
		this.numNeedAcknowledgedSubtasks = verticesToConfirm.size();
		this.checkpointStorage = checkpointStorage;
	}

	// --------------------------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public JobID getJobId() {
		return jobId;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public Map<OperatorID, OperatorState> getOperatorStates() {
		return operatorStates;
	}

	public boolean isFullyAcknowledged() {
		return this.notYetAcknowledgedTasks.isEmpty() && !discarded;
	}

	public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
		return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
	}

	public Map<ExecutionAttemptID, ExecutionVertex> getTotalTasks() {
		return totalTasks;
	}

	public CheckpointStorageLocation getTargetLocation() {
		return targetLocation;
	}

	/**
	 * This method should be carefully modified. {@link #notYetAcknowledgedTasks} is not thread-safe and ConcurrentException
	 * will easily occur if we don't do a shallow copy for its values.
	 */
	public Collection<ExecutionVertex> copyOfNotYetAcknowledgedTasks() {
		return new ArrayList<>(notYetAcknowledgedTasks.values());
	}

	public boolean isDiscarded() {
		return discarded;
	}

	/**
	 * Checks whether this checkpoint can be subsumed or whether it should always continue, regardless
	 * of newer checkpoints in progress.
	 *
	 * @return True if the checkpoint can be subsumed, false otherwise.
	 */
	public boolean canBeSubsumed() {
		// If the checkpoint is forced, it cannot be subsumed.
		return !props.forceCheckpoint();
	}

	CheckpointProperties getProps() {
		return props;
	}

	/**
	 * Sets the callback for tracking this pending checkpoint.
	 *
	 * @param trackerCallback Callback for collecting subtask stats.
	 */
	void setStatsCallback(@Nullable PendingCheckpointStats trackerCallback) {
		this.statsCallback = trackerCallback;
	}

	/**
	 * Sets the handle for the canceller to this pending checkpoint. This method fails
	 * with an exception if a handle has already been set.
	 *
	 * @return true, if the handle was set, false, if the checkpoint is already disposed;
	 */
	public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
		synchronized (lock) {
			if (this.cancellerHandle == null) {
				if (!discarded) {
					this.cancellerHandle = cancellerHandle;
					return true;
				} else {
					return false;
				}
			}
			else {
				throw new IllegalStateException("A canceller handle was already set");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Progress and Completion
	// ------------------------------------------------------------------------

	/**
	 * Returns the completion future.
	 *
	 * @return A future to the completed checkpoint
	 */
	public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
		return onCompletionPromise;
	}

	public CompletedCheckpoint finalizeCheckpoint() throws IOException {

		synchronized (lock) {
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet.");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// happen-before, write savepoint simple meta to checkpoint dir
				if (props.isSavepoint() && checkpointStorage != null) {
					String savepointMetadataLocation = targetLocation.getMetadataFilePath().toString();
					final SavepointSimpleMetadata savepointSimpleMetadata = new SavepointSimpleMetadata(checkpointId, savepointMetadataLocation);
					LOG.info("Savepoint {} completed successfully, store savepoint simple metadata: {}", checkpointId, savepointSimpleMetadata);

					CheckpointStorageLocation savepointMetaInCheckpointDirLocation = checkpointStorage.initializeLocationForSavepointMetaInCheckpointDir(checkpointId);

					try (CheckpointMetadataOutputStream out = savepointMetaInCheckpointDirLocation.createMetadataOutputStream()) {
						Checkpoints.storeSavepointSimpleMetadata(savepointSimpleMetadata, out);
						out.closeAndFinalizeCheckpoint();
					}
				}

				// write out the metadata
				final Savepoint savepoint = new SavepointV2(checkpointId, operatorStates.values(), masterState);
				final CompletedCheckpointStorageLocation finalizedLocation;

				try (CheckpointMetadataOutputStream out = targetLocation.createMetadataOutputStream()) {
					metadataOutputStream.set(out);

					Checkpoints.storeCheckpointMetadata(savepoint, out);

					// We might be faced with a racing condition (with the "abort" thread). This is a safeguard.
					CheckpointMetadataOutputStream outputStream = metadataOutputStream.getAndSet(null);
					if (outputStream != null) {
						// Successfully controlled the output stream
						finalizedLocation = out.closeAndFinalizeCheckpoint();
					} else {
						// Lost control of the output stream
						throw new IOException("Metadata output stream closed by another thread.");
					}
				}

				final long finishTimestamp = System.currentTimeMillis();
				CompletedCheckpoint completed = new CompletedCheckpoint(
						jobId,
						checkpointId,
						checkpointTimestamp,
						finishTimestamp,
						operatorStates,
						masterState,
						props,
						finalizedLocation);
				completed.setCheckpointStorage(checkpointStorage);

				onCompletionPromise.complete(completed);

				// to prevent null-pointers from concurrent modification, copy reference onto stack
				PendingCheckpointStats statsCallback = this.statsCallback;
				if (statsCallback != null) {
					// Finalize the statsCallback and give the completed checkpoint a
					// callback for discards.
					CompletedCheckpointStats.DiscardCallback discardCallback =
							statsCallback.reportCompletedCheckpoint(finalizedLocation.getExternalPointer(), finishTimestamp, completed.getTotalStateSize(), numNeedAcknowledgedSubtasks);
					completed.setDiscardCallback(discardCallback);
				}

				// mark this pending checkpoint as disposed, but do NOT drop the state
				dispose(false);

				return completed;
			}
			catch (Throwable t) {
				onCompletionPromise.completeExceptionally(t);
				ExceptionUtils.rethrowIOException(t);
				return null; // silence the compiler
			}
		}
	}

	public TaskAcknowledgeResult overrideTaskStates(
			ExecutionAttemptID executionAttemptId,
			TaskStateSnapshot operatorSubtaskStates,
			long overrideCheckpointId,
			boolean notifyPerformCheckpoint) {
		synchronized (lock) {
			if (discarded) {
				return TaskAcknowledgeResult.DISCARDED;
			}

			ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);
			if (vertex != null) {
				acknowledgedTasks.add(executionAttemptId);
				++numAcknowledgedTasks;
			} else if (totalTasks.containsKey(executionAttemptId)) {
				vertex = totalTasks.get(executionAttemptId);
			} else {
				LOG.info("The execution with {} does not need to override the state.", executionAttemptId);
				return TaskAcknowledgeResult.SUCCESS;
			}

			List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs();
			int subtaskIndex = vertex.getParallelSubtaskIndex();
			long ackTimestamp = System.currentTimeMillis();

			long stateSize = 0L;

			if (operatorSubtaskStates != null) {
				for (OperatorID operatorID : operatorIDs) {

					OperatorSubtaskState operatorSubtaskState =
							operatorSubtaskStates.getSubtaskStateByOperatorID(operatorID);

					// if no real operatorSubtaskState was reported, we insert an empty state
					if (operatorSubtaskState == null) {
						operatorSubtaskState = new OperatorSubtaskState();
					}

					OperatorState operatorState = operatorStates.get(operatorID);

					if (operatorState == null) {
						operatorState = new OperatorState(
								operatorID,
								vertex.getTotalNumberOfParallelSubtasks(),
								vertex.getMaxParallelism());
						operatorStates.put(operatorID, operatorState);
					}

					operatorState.putState(subtaskIndex, overrideOperatorSubtaskState(operatorSubtaskState));
					stateSize += operatorSubtaskState.getStateSize();
				}
			}

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
						subtaskIndex,
						ackTimestamp,
						stateSize,
						0L,
						0L,
						0L,
						0L,
						overrideCheckpointId);

				statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats, true);
			}

			LOG.info("Checkpoint {} replaces states of Task {} with snapshot from checkpoint {}.",
					checkpointId, vertex.getTaskNameWithSubtaskIndex(), overrideCheckpointId);

			// only notify when task failed
			if (notifyPerformCheckpoint) {
				pendingTrigger.notifyPerformCheckpoint(checkpointId, checkpointTimestamp, executionAttemptId);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	private OperatorSubtaskState overrideOperatorSubtaskState(OperatorSubtaskState operatorSubtaskState) {
		boolean needRewrite = Stream.concat(
			operatorSubtaskState.getManagedKeyedState().stream(),
			operatorSubtaskState.getRawKeyedState().stream())
			.anyMatch(stateHandle -> stateHandle instanceof IncrementalKeyedStateHandle);
		if (needRewrite) {
			StateObjectCollection<KeyedStateHandle> managedKeyedState = overrideIncrementalStateHandle(operatorSubtaskState.getManagedKeyedState());
			StateObjectCollection<KeyedStateHandle> rawKeyedState = overrideIncrementalStateHandle(operatorSubtaskState.getRawKeyedState());
			OperatorSubtaskState replacedState = new OperatorSubtaskState(
				operatorSubtaskState.getManagedOperatorState(),
				operatorSubtaskState.getRawOperatorState(),
				managedKeyedState,
				rawKeyedState);
			return new OperatorSubtaskStatePlaceHolder(replacedState);
		} else {
			return new OperatorSubtaskStatePlaceHolder(operatorSubtaskState);
		}
	}

	private StateObjectCollection<KeyedStateHandle> overrideIncrementalStateHandle(StateObjectCollection<KeyedStateHandle> oriKeyedStateHandles) {
		StateObjectCollection<KeyedStateHandle> keyedStateHandles = new StateObjectCollection<>();
		oriKeyedStateHandles.stream().forEach(stateHandle -> {
			if (stateHandle instanceof IncrementalKeyedStateHandle) {
				keyedStateHandles.add(((IncrementalKeyedStateHandle) stateHandle).overrideWithPlaceHolder(checkpointId));
			} else {
				keyedStateHandles.add(stateHandle);
			}
		});
		return keyedStateHandles;
	}

	public PendingTriggerFactory.PendingTrigger getPendingTrigger() {
		return pendingTrigger;
	}

	/**
	 * Acknowledges the task with the given execution attempt id and the given subtask state.
	 *
	 * @param executionAttemptId of the acknowledged task
	 * @param operatorSubtaskStates of the acknowledged task
	 * @param metrics Checkpoint metrics for the stats
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			TaskStateSnapshot operatorSubtaskStates,
			CheckpointMetrics metrics) {

		synchronized (lock) {
			if (discarded) {
				return TaskAcknowledgeResult.DISCARDED;
			}

			final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

			if (vertex == null) {
				if (acknowledgedTasks.contains(executionAttemptId)) {
					return TaskAcknowledgeResult.DUPLICATE;
				} else {
					return TaskAcknowledgeResult.UNKNOWN;
				}
			} else {
				acknowledgedTasks.add(executionAttemptId);
			}

			List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs();
			int subtaskIndex = vertex.getParallelSubtaskIndex();
			long ackTimestamp = System.currentTimeMillis();

			long stateSize = 0L;

			if (operatorSubtaskStates != null) {
				for (OperatorID operatorID : operatorIDs) {

					OperatorSubtaskState operatorSubtaskState =
						operatorSubtaskStates.getSubtaskStateByOperatorID(operatorID);

					// if no real operatorSubtaskState was reported, we insert an empty state
					if (operatorSubtaskState == null) {
						operatorSubtaskState = new OperatorSubtaskState();
					}

					OperatorState operatorState = operatorStates.get(operatorID);

					if (operatorState == null) {
						operatorState = new OperatorState(
							operatorID,
							vertex.getTotalNumberOfParallelSubtasks(),
							vertex.getMaxParallelism());
						operatorStates.put(operatorID, operatorState);
					}

					operatorState.putState(subtaskIndex, operatorSubtaskState);
					stateSize += operatorSubtaskState.getStateSize();
				}
			}

			++numAcknowledgedTasks;

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Do this in millis because the web frontend works with them
				long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;

				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
					subtaskIndex,
					ackTimestamp,
					stateSize,
					metrics.getSyncDurationMillis(),
					metrics.getAsyncDurationMillis(),
					metrics.getBytesBufferedInAlignment(),
					alignmentDurationMillis,
					checkpointId);

				statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Adds a master state (state generated on the checkpoint coordinator) to
	 * the pending checkpoint.
	 *
	 * @param state The state to add
	 */
	public void addMasterState(MasterState state) {
		checkNotNull(state);

		synchronized (lock) {
			if (!discarded) {
				masterState.add(state);
			}
		}
	}


	// ------------------------------------------------------------------------
	//  Cancellation
	// ------------------------------------------------------------------------

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason, @Nullable Throwable cause) {
		try {
			CheckpointException exception = wrapWithCheckpointException(reason, cause);
			onCompletionPromise.completeExceptionally(exception);
			closeMetadataOutputStream();
			reportFailedCheckpoint(exception, reason);
			assertAbortSubsumedForced(reason);
		} finally {
			dispose(true);
		}
	}

	/**
	 * Aborts a checkpoint with reason and cause.
	 */
	public void abort(CheckpointFailureReason reason) {
		abort(reason, null);
	}

	/**
	 * Stops writing metadata, if it is being written now.
	 */
	private void closeMetadataOutputStream() {
		// We will try to gain control of this stream from
		CheckpointMetadataOutputStream outputStream = metadataOutputStream.getAndSet(null);
		if (outputStream != null) {
			// The output stream is opened and we gained an exclusive control on it.
			try {
				outputStream.close();
			} catch (IOException e) {
				LOG.warn("Failed to abort metadata writing.", e);
			}
		}
	}

	private CheckpointException wrapWithCheckpointException(CheckpointFailureReason reason, Throwable cause) {
		if (cause instanceof CheckpointException) {
			return (CheckpointException) cause;
		} else {
			return new CheckpointException(reason, cause);
		}
	}

	private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
		if (props.forceCheckpoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
			throw new IllegalStateException("Bug: forced checkpoints must never be subsumed, " +
				"the abort reason is : " + reason.message());
		}
	}

	private void dispose(boolean releaseState) {

		synchronized (lock) {
			try {
				numAcknowledgedTasks = -1;
				if (!discarded && releaseState) {
					executor.execute(new Runnable() {
						@Override
						public void run() {

							// discard the private states.
							// unregistered shared states are still considered private at this point.
							try {
								StateUtil.discardPendingCheckpoint(PendingCheckpoint.this);
							} catch (Throwable t) {
								LOG.warn("Could not properly dispose the private states in the pending checkpoint {} of job {}.",
									checkpointId, jobId, t);
							} finally {
								operatorStates.clear();
							}
						}
					});

				}
			} finally {
				discarded = true;
				notYetAcknowledgedTasks.clear();
				acknowledgedTasks.clear();
				cancelCanceller();
			}
		}
	}

	private void cancelCanceller() {
		try {
			final ScheduledFuture<?> canceller = this.cancellerHandle;
			if (canceller != null) {
				canceller.cancel(false);
			}
		}
		catch (Exception e) {
			// this code should not throw exceptions
			LOG.warn("Error while cancelling checkpoint timeout task", e);
		}
	}

	/**
	 * Reports a failed checkpoint with the given optional cause.
	 *
	 * @param cause The failure cause or <code>null</code>.
	 */
	private void reportFailedCheckpoint(Exception cause, CheckpointFailureReason reason) {
		// to prevent null-pointers from concurrent modification, copy reference onto stack
		final PendingCheckpointStats statsCallback = this.statsCallback;
		if (statsCallback != null) {
			long failureTimestamp = System.currentTimeMillis();
			statsCallback.reportFailedCheckpoint(failureTimestamp, cause, reason);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
				checkpointId, checkpointTimestamp, getNumberOfAcknowledgedTasks(), getNumberOfNonAcknowledgedTasks());
	}
}
