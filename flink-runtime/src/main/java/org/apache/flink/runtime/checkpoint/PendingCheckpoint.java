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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointStateMetadata;
import org.apache.flink.runtime.checkpoint.metadata.savepoint.SavepointSimpleMetadata;
import org.apache.flink.runtime.checkpoint.trigger.PendingTriggerFactory;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
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

	private final Map<OperatorID, OperatorStateMeta> operatorStateMetasFromSnapshot;

	private final Map<OperatorID, OperatorStateMeta> operatorStateMetasFromJobGraph;

	private final Map<ExecutionAttemptID, ExecutionVertex> totalTasks;

	private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

	private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

	private final List<MasterState> masterStates;

	private final Set<String> notYetAcknowledgedMasterStates;

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

	private int numAcknowledgedTasks;

	private boolean discarded;

	/** Optional stats tracker callback. */
	@Nullable
	private PendingCheckpointStats statsCallback;

	private volatile ScheduledFuture<?> cancellerHandle;

	private CheckpointException failureCause;

	private final PendingTriggerFactory.PendingTrigger pendingTrigger;

	private final int numNeedAcknowledgedSubtasks;

	private final int transferMaxRetryAttempts;

	private final boolean allowPersistStateMeta;

	// --------------------------------------------------------------------------------------------

	public PendingCheckpoint(
			JobID jobId,
			long checkpointId,
			long checkpointTimestamp,
			Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
			Collection<OperatorID> operatorCoordinatorsToConfirm,
			Collection<String> masterStateIdentifiers,
			CheckpointProperties props,
			CheckpointStorageLocation targetLocation,
			Executor executor,
			CompletableFuture<CompletedCheckpoint> onCompletionPromise) {
		this(
			jobId,
			checkpointId,
			checkpointTimestamp,
			verticesToConfirm,
			operatorCoordinatorsToConfirm,
			masterStateIdentifiers,
			props,
			targetLocation,
			executor,
			onCompletionPromise,
			PendingTriggerFactory.createDefaultPendingTrigger(verticesToConfirm));
	}

	public PendingCheckpoint(
		JobID jobId,
		long checkpointId,
		long checkpointTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		Collection<OperatorID> operatorCoordinatorsToConfirm,
		Collection<String> masterStateIdentifiers,
		CheckpointProperties props,
		CheckpointStorageLocation targetLocation,
		Executor executor,
		CompletableFuture<CompletedCheckpoint> onCompletionPromise,
		PendingTriggerFactory.PendingTrigger pendingTrigger) {

		this(jobId,
			checkpointId,
			checkpointTimestamp,
			verticesToConfirm,
			operatorCoordinatorsToConfirm,
			masterStateIdentifiers,
			props,
			targetLocation,
			executor,
			onCompletionPromise,
			pendingTrigger,
			null,
			1,
			false,
			null);
	}

	public PendingCheckpoint(
		JobID jobId,
		long checkpointId,
		long checkpointTimestamp,
		Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm,
		Collection<OperatorID> operatorCoordinatorsToConfirm,
		Collection<String> masterStateIdentifiers,
		CheckpointProperties props,
		CheckpointStorageLocation targetLocation,
		Executor executor,
		CompletableFuture<CompletedCheckpoint> onCompletionPromise,
		PendingTriggerFactory.PendingTrigger pendingTrigger,
		@Nullable CheckpointStorageCoordinatorView checkpointStorage,
		int transferMaxRetryAttempts,
		boolean allowPersistStateMeta,
		Map<OperatorID, OperatorStateMeta> operatorStateMetasFromJobGraph) {

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
		this.operatorStateMetasFromSnapshot = new HashMap<>();
		this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
		this.notYetAcknowledgedMasterStates = masterStateIdentifiers.isEmpty()
			? Collections.emptySet() : new HashSet<>(masterStateIdentifiers);
		this.notYetAcknowledgedOperatorCoordinators = operatorCoordinatorsToConfirm.isEmpty()
			? Collections.emptySet() : new HashSet<>(operatorCoordinatorsToConfirm);
		this.acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
		this.onCompletionPromise = checkNotNull(onCompletionPromise);
		this.pendingTrigger = pendingTrigger;
		this.numNeedAcknowledgedSubtasks = verticesToConfirm.size();
		this.checkpointStorage = checkpointStorage;
		Preconditions.checkArgument(transferMaxRetryAttempts >= 1);
		this.transferMaxRetryAttempts = transferMaxRetryAttempts;
		this.allowPersistStateMeta = allowPersistStateMeta;
		this.operatorStateMetasFromJobGraph = operatorStateMetasFromJobGraph;
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

	public CheckpointStorageLocation getCheckpointStorageLocation() {
		return targetLocation;
	}

	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	public int getNumberOfNonAcknowledgedTasks() {
		return notYetAcknowledgedTasks.size();
	}

	public int getNumberOfNonAcknowledgedOperatorCoordinators() {
		return notYetAcknowledgedOperatorCoordinators.size();
	}

	public int getNumberOfAcknowledgedTasks() {
		return numAcknowledgedTasks;
	}

	public Map<OperatorID, OperatorState> getOperatorStates() {
		return operatorStates;
	}

	public List<MasterState> getMasterStates() {
		return masterStates;
	}

	public boolean isFullyAcknowledged() {
		return areTasksFullyAcknowledged() &&
			areCoordinatorsFullyAcknowledged() &&
			areMasterStatesFullyAcknowledged();
	}

	boolean areMasterStatesFullyAcknowledged() {
		return notYetAcknowledgedMasterStates.isEmpty() && !discarded;
	}

	boolean areCoordinatorsFullyAcknowledged() {
		return notYetAcknowledgedOperatorCoordinators.isEmpty() && !discarded;
	}

	boolean areTasksFullyAcknowledged() {
		return notYetAcknowledgedTasks.isEmpty() && !discarded;
	}

	public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
		return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
	}

	public Map<ExecutionAttemptID, ExecutionVertex> getTotalTasks() {
		return totalTasks;
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
		return !props.isSavepoint();
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

	public CheckpointException getFailureCause() {
		return failureCause;
	}

	public CheckpointStorageLocation getTargetLocation() {
		return targetLocation;
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
			checkState(!isDiscarded(), "checkpoint is discarded");
			checkState(isFullyAcknowledged(), "Pending checkpoint has not been fully acknowledged yet");

			// make sure we fulfill the promise with an exception if something fails
			try {
				// happen-before, write savepoint simple meta to checkpoint dir
				if (props.isSavepoint() && checkpointStorage != null) {
					String savepointMetadataLocation = targetLocation.getMetadataFilePath().toString();
					final SavepointSimpleMetadata savepointSimpleMetadata = new SavepointSimpleMetadata(checkpointId, savepointMetadataLocation);
					LOG.info("Savepoint {} completed successfully, store savepoint simple metadata: {}", checkpointId, savepointSimpleMetadata);

					CheckpointStorageLocation savepointMetaInCheckpointDirLocation = checkpointStorage.initializeLocationForSavepointMetaInCheckpointDir(checkpointId);
					writeMetadataWithRetry(savepointSimpleMetadata, savepointMetaInCheckpointDirLocation, CheckpointStorageLocation::createMetadataOutputStream, Checkpoints::storeSavepointSimpleMetadata, transferMaxRetryAttempts);

					//handle the state meta if comes a savepoint
					try {
						// write out the metadata
						final CheckpointStateMetadata savepointStateMeta = new CheckpointStateMetadata(checkpointId, operatorStateMetasFromSnapshot.values());
						final CompletedCheckpointStorageLocation savepointStateMetaLocation =
						writeMetadataWithRetry(savepointStateMeta, targetLocation, CheckpointStorageLocation::createStateMetadataOutputStream, Checkpoints::storeCheckpointStateMetadata, transferMaxRetryAttempts);
						LOG.info("StateMeta Snapshot for checkpoint {} completed successfully, store savepoint state metadata: {}", checkpointId, savepointStateMetaLocation);
					} catch (Exception writeStateMetaException){
						LOG.warn("StateMeta Snapshot for checkpoint {} completed failed with Exception ", checkpointId, writeStateMetaException);
					}
				}

				// write out the metadata
				final CheckpointMetadata savepoint = new CheckpointMetadata(checkpointId, operatorStates.values(), masterStates);
				final CompletedCheckpointStorageLocation finalizedLocation =
					writeMetadataWithRetry(savepoint, targetLocation, CheckpointStorageLocation::createMetadataOutputStream, Checkpoints::storeCheckpointMetadata, transferMaxRetryAttempts);

				// persist state meta in checkpoint dir
				if (allowPersistStateMeta && !props.isSavepoint()) {
					final CheckpointStateMetadata checkpointStateMetadata = new CheckpointStateMetadata(checkpointId, operatorStateMetasFromJobGraph.values());
					writeMetadataWithRetry(checkpointStateMetadata, targetLocation, CheckpointStorageLocation::createStateMetadataOutputStream, Checkpoints::storeCheckpointStateMetadata, transferMaxRetryAttempts);
				}

				CompletedCheckpoint completed = new CompletedCheckpoint(
						jobId,
						checkpointId,
						checkpointTimestamp,
						System.currentTimeMillis(),
						operatorStates,
						masterStates,
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
							statsCallback.reportCompletedCheckpoint(
								finalizedLocation.getExternalPointer(),
								completed.getTotalStateSize(),
								completed.getRawTotalStateSize(),
								numNeedAcknowledgedSubtasks);
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

			List<OperatorID> operatorIDs = vertex.getJobVertex().getOperatorIDs()
					.stream().map(OperatorIDPair::getGeneratedOperatorID).collect(Collectors.toList());
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
	 * @param taskStateSnapshot of the acknowledged task
	 * @param metrics Checkpoint metrics for the stats
	 * @return TaskAcknowledgeResult of the operation
	 */
	public TaskAcknowledgeResult acknowledgeTask(
			ExecutionAttemptID executionAttemptId,
			TaskStateSnapshot taskStateSnapshot,
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

			List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
			int subtaskIndex = vertex.getParallelSubtaskIndex();
			long ackTimestamp = System.currentTimeMillis();

			long stateSize = 0L;

			if (taskStateSnapshot != null) {
				for (OperatorIDPair operatorID : operatorIDs) {

					OperatorSubtaskState operatorSubtaskState =
						taskStateSnapshot.getSubtaskStateByOperatorID(operatorID.getGeneratedOperatorID());

					// if no real operatorSubtaskState was reported, we insert an empty state
					if (operatorSubtaskState == null) {
						operatorSubtaskState = new OperatorSubtaskState();
					}

					OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

					if (operatorState == null) {
						operatorState = new OperatorState(
							operatorID.getGeneratedOperatorID(),
							vertex.getTotalNumberOfParallelSubtasks(),
							vertex.getMaxParallelism());
						operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
					}

					operatorState.putState(subtaskIndex, operatorSubtaskState);
					stateSize += operatorSubtaskState.getStateSize();

					//handle state meta data if comes a savepoint
					if (props.isSavepoint()) {

						OperatorSubtaskStateMeta operatorSubtaskStateMeta =
							taskStateSnapshot.getSubtaskStateMetaByOperatorId(operatorID.getGeneratedOperatorID());

						if (operatorSubtaskStateMeta == null) {
							operatorSubtaskStateMeta = OperatorSubtaskStateMeta.empty();
						}

						OperatorStateMeta operatorStateMeta = operatorStateMetasFromSnapshot.get(operatorID.getGeneratedOperatorID());

						if (operatorStateMeta == null) {
							Tuple2<String, String> operatorUidAndName =
								vertex.getJobVertex().getOperatorUidAndNames().get(operatorID.getGeneratedOperatorID());

							operatorStateMeta = new OperatorStateMeta(operatorID.getGeneratedOperatorID());
							if (operatorUidAndName != null){
								operatorStateMeta.setUid(operatorUidAndName.f0);
								operatorStateMeta.setOperatorName(operatorUidAndName.f1);
							}
							operatorStateMetasFromSnapshot.put(operatorID.getGeneratedOperatorID(), operatorStateMeta);
						}
						operatorStateMeta.mergeSubtaskStateMeta(operatorSubtaskStateMeta);
					}
				}
			}

			++numAcknowledgedTasks;

			// publish the checkpoint statistics
			// to prevent null-pointers from concurrent modification, copy reference onto stack
			final PendingCheckpointStats statsCallback = this.statsCallback;
			if (statsCallback != null) {
				// Do this in millis because the web frontend works with them
				long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
				long checkpointStartDelayMillis = metrics.getCheckpointStartDelayNanos() / 1_000_000;

				SubtaskStateStats subtaskStateStats = new SubtaskStateStats(
					subtaskIndex,
					ackTimestamp,
					stateSize,
					metrics.getSyncDurationMillis(),
					metrics.getAsyncDurationMillis(),
					alignmentDurationMillis,
					checkpointStartDelayMillis,
					checkpointId);

				statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	public TaskAcknowledgeResult acknowledgeCoordinatorState(
			OperatorInfo coordinatorInfo,
			@Nullable ByteStreamStateHandle stateHandle) {

		synchronized (lock) {
			if (discarded) {
				return TaskAcknowledgeResult.DISCARDED;
			}

			final OperatorID operatorId = coordinatorInfo.operatorId();
			OperatorState operatorState = operatorStates.get(operatorId);

			// sanity check for better error reporting
			if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
				return operatorState != null && operatorState.getCoordinatorState() != null
						? TaskAcknowledgeResult.DUPLICATE
						: TaskAcknowledgeResult.UNKNOWN;
			}

			if (stateHandle != null) {
				if (operatorState == null) {
					operatorState = new OperatorState(
						operatorId, coordinatorInfo.currentParallelism(), coordinatorInfo.maxParallelism());
					operatorStates.put(operatorId, operatorState);
				}
				operatorState.setCoordinatorState(stateHandle);
			}

			return TaskAcknowledgeResult.SUCCESS;
		}
	}

	/**
	 * Acknowledges a master state (state generated on the checkpoint coordinator) to
	 * the pending checkpoint.
	 *
	 * @param identifier The identifier of the master state
	 * @param state The state to acknowledge
	 */
	public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

		synchronized (lock) {
			if (!discarded) {
				if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
					masterStates.add(state);
				}
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
			LOG.warn("Abort checkpoint " + checkpointId, cause);
			failureCause = wrapWithCheckpointException(reason, cause);
			onCompletionPromise.completeExceptionally(failureCause);
			reportFailedCheckpoint(failureCause, reason);
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

	private CheckpointException wrapWithCheckpointException(CheckpointFailureReason reason, Throwable cause) {
		if (cause instanceof CheckpointException) {
			return (CheckpointException) cause;
		} else {
			return new CheckpointException(reason, cause);
		}
	}

	private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
		if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
			throw new IllegalStateException("Bug: savepoints must never be subsumed, " +
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

	private <M> CompletedCheckpointStorageLocation writeMetadataWithRetry(
			M metadata,
			CheckpointStorageLocation targetLocation,
			FunctionWithException<CheckpointStorageLocation, CheckpointMetadataOutputStream, IOException> function,
			BiConsumerWithException<M, OutputStream, IOException> consumer,
			int maxRetryAttempts) throws IOException {
		for (int retryAttempts = 1; retryAttempts <= maxRetryAttempts; retryAttempts++) {
			try (CheckpointMetadataOutputStream out = function.apply(targetLocation)) {
				consumer.accept(metadata, out);
				return out.closeAndFinalizeCheckpoint();
			} catch (Throwable t) {
				if (retryAttempts < maxRetryAttempts) {
					LOG.warn("Write metadata for checkpoint[{}] failed, retry...", checkpointId);
					continue;
				}
				throw t;
			}
		}
		throw new FlinkRuntimeException("Write metadata for checkpoint[" + checkpointId + "] failed");
	}
}
