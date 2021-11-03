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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.handler.CheckpointHandler;
import org.apache.flink.runtime.checkpoint.handler.GlobalCheckpointHandler;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.checkpoint.scheduler.CheckpointScheduler;
import org.apache.flink.runtime.checkpoint.trigger.CheckpointTasks;
import org.apache.flink.runtime.checkpoint.trigger.PendingTriggerFactory;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.PerformCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.scheduler.CheckpointSchedulerUtils.createCheckpointScheduler;
import static org.apache.flink.runtime.checkpoint.scheduler.CheckpointSchedulerUtils.setupSavepointScheduler;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state.
 * It triggers the checkpoint by sending the messages to the relevant tasks and collects the
 * checkpoint acknowledgements. It also collects and maintains the overview of the state handles
 * reported by the tasks that acknowledge the checkpoint.
 */
public class CheckpointCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	/** The number of recent checkpoints whose IDs are remembered. */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

	// ------------------------------------------------------------------------

	/** Coordinator-wide lock to safeguard the checkpoint updates. */
	private final Object lock = new Object();

	/** The job whose checkpoint this coordinator coordinates. */
	private final JobID job;

	/** Default checkpoint properties. **/
	private final CheckpointProperties checkpointProperties;

	/** The executor used for asynchronous calls, like potentially blocking I/O. */
	private final Executor executor;

	/** Tasks who need to be sent a message when a checkpoint is started. */
	private final ExecutionVertex[] tasksToTrigger;

	/** Tasks who need to acknowledge a checkpoint before it succeeds. */
	private final ExecutionVertex[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed. */
	// TODO currently we use commit vertices to receive "abort checkpoint" messages.
	private final ExecutionVertex[] tasksToCommitTo;

	/** The operator coordinators that need to be checkpointed. */
	private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;

	/** Map from checkpoint ID to the pending checkpoint. */
	@GuardedBy("lock")
	private final Map<Long, PendingCheckpoint> pendingCheckpoints;

	/** Mapping from checkpointing ID to the pending detach savepoints, the pending savepoint
	 * ids are just reported to JM's rest api "savepoint-dump". */
	private final Map<Long, String> pendingSavepointsUnsafe;

	/** Completed checkpoints. Implementations can be blocking. Make sure calls to methods
	 * accessing this don't block the job manager actor and run asynchronously. */
	private final CompletedCheckpointStore completedCheckpointStore;

	/** The root checkpoint state backend, which is responsible for initializing the
	 * checkpoint, storing the metadata, and cleaning up the checkpoint. */
	private final CheckpointStorageCoordinatorView checkpointStorage;

	/** A list of recent checkpoint IDs, to identify late messages (vs invalid ones). */
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/** Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers. */
	private final CheckpointIDCounter checkpointIdCounter;

	/** The max time (in ms) that a checkpoint may take. */
	private final long checkpointTimeout;

	/** The timer that handles the checkpoint timeouts and triggers periodic checkpoints.
	 * It must be single-threaded. Eventually it will be replaced by main thread executor. */
	private final ScheduledExecutor timer;

	private final CheckpointScheduler checkpointScheduler;

	/** The master checkpoint hooks executed by this checkpoint coordinator. */
	private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

	private final boolean unalignedCheckpointsEnabled;

	/** Actor that receives status updates from the execution graph this coordinator works for. */
	private JobStatusListener jobStatusListener;

	/** The number of consecutive failed trigger attempts. */
	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger(0);

	/** The timestamp (via {@link Clock#relativeTimeMillis()}) when the last checkpoint
	 * completed. */
	private long lastCheckpointCompletionRelativeTime;

	/** The timestamp (via {@link System#nanoTime()}) when the last checkpoint completed. */
	private long lastCheckpointCompletionNanos;

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;

	/** Flag marking the coordinator as shut down (not accepting any messages any more). */
	private volatile boolean shutdown;

	/** Optional tracker for checkpoint statistics. */
	@Nullable
	private CheckpointStatsTracker statsTracker;

	/** A factory for SharedStateRegistry objects. */
	private final SharedStateRegistryFactory sharedStateRegistryFactory;

	/** Registry that tracks state which is shared across (incremental) checkpoints. */
	private SharedStateRegistry sharedStateRegistry;

	private boolean isPreferCheckpointForRecovery;

	private final CheckpointFailureManager failureManager;

	private final Clock clock;

	private final boolean isExactlyOnceMode;

	/** Flag represents there is an in-flight trigger request. */
	private boolean isTriggering = false;

	private final CheckpointRequestDecider requestDecider;

	private final CheckpointHandler checkpointHandler;

	private final boolean aggregateUnionState;

	/** determine how to trigger the checkpoint. */
	private final PendingTriggerFactory pendingTriggerFactory;

	/** The maximum number of retries for checkpoint writing hdfs. */
	private final int transferMaxRetryAttempts;

	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	public CheckpointCoordinator(
			JobID job,
			String jobUID,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			ScheduledExecutor timer,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager) {

		this(
				job,
				jobUID,
				chkConfig,
				tasksToTrigger,
				tasksToWaitFor,
				tasksToCommitTo,
				coordinatorsToCheckpoint,
				checkpointIDCounter,
				completedCheckpointStore,
				checkpointStateBackend,
				executor,
				timer,
				sharedStateRegistryFactory,
				failureManager,
				new GlobalCheckpointHandler());
	}

	@VisibleForTesting
	public CheckpointCoordinator(
		JobID job,
		String jobUID,
		CheckpointCoordinatorConfiguration chkConfig,
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo,
		Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
		CheckpointIDCounter checkpointIDCounter,
		CompletedCheckpointStore completedCheckpointStore,
		StateBackend checkpointStateBackend,
		Executor executor,
		ScheduledExecutor timer,
		SharedStateRegistryFactory sharedStateRegistryFactory,
		CheckpointFailureManager failureManager,
		CheckpointHandler checkpointHandler) {

		this(
			job,
			jobUID,
			chkConfig,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			coordinatorsToCheckpoint,
			checkpointIDCounter,
			completedCheckpointStore,
			checkpointStateBackend,
			executor,
			timer,
			sharedStateRegistryFactory,
			failureManager,
			SystemClock.getInstance(),
			checkpointHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
	}

	@VisibleForTesting
	public CheckpointCoordinator(
			JobID job,
			String jobUID,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			ScheduledExecutor timer,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager,
			Clock clock,
			CheckpointHandler checkpointHandler,
			MetricGroup metricGroup) {
		this(job,
			jobUID,
			null,
			chkConfig,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			coordinatorsToCheckpoint,
			checkpointIDCounter,
			completedCheckpointStore,
			checkpointStateBackend,
			executor,
			timer,
			sharedStateRegistryFactory,
			failureManager,
			clock,
			checkpointHandler,
			metricGroup,
			new PendingTriggerFactory(
				job,
				new CheckpointTasks(tasksToTrigger, tasksToWaitFor, tasksToCommitTo),
				new CheckpointTasks(tasksToTrigger, tasksToWaitFor, tasksToCommitTo),
				false,
				false));
	}

	public CheckpointCoordinator(
			JobID job,
			String jobUID,
			@Nullable String namespace,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			ScheduledExecutor timer,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager,
			Clock clock,
			CheckpointHandler checkpointHandler,
			MetricGroup metricGroup,
			PendingTriggerFactory pendingTriggerFactory) {

		// sanity checks
		checkNotNull(checkpointStateBackend);

		this.job = checkNotNull(job);
		this.checkpointTimeout = chkConfig.getCheckpointTimeout();
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.coordinatorsToCheckpoint = Collections.unmodifiableCollection(coordinatorsToCheckpoint);
		this.pendingCheckpoints = new LinkedHashMap<>();
		this.pendingSavepointsUnsafe = new LinkedHashMap<>();
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.executor = checkNotNull(executor);
		this.sharedStateRegistryFactory = checkNotNull(sharedStateRegistryFactory);
		this.sharedStateRegistry = sharedStateRegistryFactory.create(executor);
		this.isPreferCheckpointForRecovery = chkConfig.isPreferCheckpointForRecovery();
		this.failureManager = checkNotNull(failureManager);
		this.clock = checkNotNull(clock);
		this.isExactlyOnceMode = chkConfig.isExactlyOnce();
		this.unalignedCheckpointsEnabled = chkConfig.isUnalignedCheckpointsEnabled();
		this.aggregateUnionState = chkConfig.isAggregateUnionState();
		this.transferMaxRetryAttempts = Math.max(chkConfig.getTransferMaxRetryAttempts(), 1);

		this.recentPendingCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
		this.masterHooks = new HashMap<>();

		this.timer = timer;
		this.checkpointScheduler = createCheckpointScheduler(job, this, chkConfig);
		setupSavepointScheduler(checkpointScheduler, jobUID, namespace, this, chkConfig);
		this.checkpointScheduler.setTimer(timer);

		this.checkpointHandler = checkpointHandler;
		checkpointHandler.loadPendingCheckpoints(pendingCheckpoints);

		this.checkpointProperties = CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy(), pendingTriggerFactory.isUseFastMode());
		this.pendingTriggerFactory = pendingTriggerFactory;

		try {
			this.checkpointStorage = checkpointStateBackend.createCheckpointStorage(job, jobUID, metricGroup);
			if (isPeriodicCheckpointingConfigured()) {
				// do not create checkpoint directory if checkpoint is disabled
				checkpointStorage.initializeBaseLocations();
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to create checkpoint storage at checkpoint coordinator side.", e);
		}

		try {
			// Make sure the checkpoint ID enumerator is running. Possibly
			// issues a blocking call to ZooKeeper.
			checkpointIDCounter.start();
		} catch (Throwable t) {
			throw new RuntimeException("Failed to start checkpoint ID counter: " + t.getMessage(), t);
		}
		this.requestDecider = new CheckpointRequestDecider(
			chkConfig.getMaxConcurrentCheckpoints(),
			this::rescheduleTrigger,
			this.clock,
			chkConfig.getMinPauseBetweenCheckpoints(),
			this.pendingCheckpoints::size,
			this.lock);
	}

	// --------------------------------------------------------------------------------------------
	//  Configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds the given master hook to the checkpoint coordinator. This method does nothing, if
	 * the checkpoint coordinator already contained a hook with the same ID (as defined via
	 * {@link MasterTriggerRestoreHook#getIdentifier()}).
	 *
	 * @param hook The hook to add.
	 * @return True, if the hook was added, false if the checkpoint coordinator already
	 *         contained a hook with the same ID.
	 */
	public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
		checkNotNull(hook);

		final String id = hook.getIdentifier();
		checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

		synchronized (lock) {
			if (!masterHooks.containsKey(id)) {
				masterHooks.put(id, hook);
				return true;
			}
			else {
				return false;
			}
		}
	}

	/**
	 * Gets the number of currently register master hooks.
	 */
	public int getNumberOfRegisteredMasterHooks() {
		synchronized (lock) {
			return masterHooks.size();
		}
	}

	/**
	 * Sets the checkpoint stats tracker.
	 *
	 * @param statsTracker The checkpoint stats tracker.
	 */
	public void setCheckpointStatsTracker(@Nullable CheckpointStatsTracker statsTracker) {
		this.statsTracker = statsTracker;
	}

	// --------------------------------------------------------------------------------------------
	//  Clean shutdown
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts down the checkpoint coordinator.
	 *
	 * <p>After this method has been called, the coordinator does not accept
	 * and further messages and cannot trigger any further checkpoints.
	 */
	public void shutdown(JobStatus jobStatus) throws Exception {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;
				LOG.info("Stopping checkpoint coordinator for job {}.", job);

				periodicScheduling = false;

				// shut down the thread that handles the timeouts and pending triggers
				checkpointScheduler.shutdownNow();

				// shut down the hooks
				MasterHooks.close(masterHooks.values(), LOG);
				masterHooks.clear();

				final CheckpointException reason = new CheckpointException(
					CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
				// clear queued requests and in-flight checkpoints
				abortPendingAndQueuedCheckpoints(reason);

				completedCheckpointStore.shutdown(jobStatus);
				checkpointIdCounter.shutdown(jobStatus, false);
			}
		}
	}

	public boolean isShutdown() {
		return shutdown;
	}

	public boolean isUseFastMode() {
		return pendingTriggerFactory.isUseFastMode();
	}

	// --------------------------------------------------------------------------------------------
	//  Triggering Checkpoints and Savepoints
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers a savepoint with the given savepoint directory as a target.
	 *
	 * @param targetLocation Target location for the savepoint, optional. If null, the
	 *                       state backend's configured default will be used.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 */
	public CompletableFuture<CompletedCheckpoint> triggerSavepoint(@Nullable final String targetLocation) {
		final CheckpointProperties properties = CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled);
		return triggerSavepointInternal(properties, false, targetLocation, -1L, DetachSavepointProperties.nonDetachSavepointProperties());
	}

	public CompletableFuture<CompletedCheckpoint> triggerSavepoint(@Nullable final String targetLocation, long savepointTimeout) {
		final CheckpointProperties properties = CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled);
		return triggerSavepointInternal(properties, false, targetLocation, savepointTimeout, DetachSavepointProperties.nonDetachSavepointProperties());
	}

	/**
	 * Trigger a detach savepoint with the given savepoint directory as a target. Different from
	 * {@link #triggerSavepoint(String)}, this targetLocation will not create a random subdir to
	 * hold the new savepoint, while directly accommodating all savepoint data and meta files.
	 *
	 * @param targetLocation format %detach_savepoint_prefix%/%date%/%jobUID%/%UUID%.
	 * @param savepointId a redundant uuid in targetLocation, just to serve {@link #pendingSavepointsUnsafe}.
	 * @return A future to the completed checkpoint
	 */
	public CompletableFuture<CompletedCheckpoint> triggerDetachSavepoint(final String targetLocation, String savepointId, long savepointTimeout) {
		Preconditions.checkNotNull(targetLocation);
		final CheckpointProperties properties = CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled);
		return triggerSavepointInternal(properties, false, targetLocation, savepointTimeout, new DetachSavepointProperties(savepointId));
	}

	public CompletableFuture<CompletedCheckpoint> triggerDetachSyncSavepoint(final String targetLocation, String savepointId, long savepointTimeout) {
		final CheckpointProperties properties = CheckpointProperties.forSyncDetachSavepoint(!unalignedCheckpointsEnabled);

		return triggerSavepointInternal(properties, false, targetLocation, savepointTimeout, new DetachSavepointProperties(savepointId));
	}

	public CompletableFuture<List<String>> getPendingSavepointsUnsafe() {
		List<String> pendingSavepointIds = new ArrayList<>(pendingSavepointsUnsafe.values());

		return CompletableFuture.completedFuture(pendingSavepointIds);
	}

	/**
	 * Triggers a synchronous savepoint with the given savepoint directory as a target.
	 *
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                              to fire any registered event-time timers.
	 * @param targetLocation Target location for the savepoint, optional. If null, the
	 *                       state backend's configured default will be used.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 */
	public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation,
			long savepointTimeout) {

		final CheckpointProperties properties = CheckpointProperties.forSyncSavepoint(!unalignedCheckpointsEnabled);

		return triggerSavepointInternal(properties, advanceToEndOfEventTime, targetLocation, savepointTimeout, DetachSavepointProperties.nonDetachSavepointProperties());
	}

	private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
			final CheckpointProperties checkpointProperties,
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation,
			final long savepointTimeout,
			final DetachSavepointProperties detachSavepointProperties) {

		checkNotNull(checkpointProperties);

		// TODO, call triggerCheckpoint directly after removing timer thread
		// for now, execute the trigger in timer thread to avoid competition
		final CompletableFuture<CompletedCheckpoint> resultFuture = new CompletableFuture<>();
		timer.execute(() -> triggerCheckpoint(
			checkpointProperties,
			targetLocation,
			false,
			advanceToEndOfEventTime,
			savepointTimeout,
			detachSavepointProperties)
		.whenComplete((completedCheckpoint, throwable) -> {
			if (throwable == null) {
				resultFuture.complete(completedCheckpoint);
			} else {
				resultFuture.completeExceptionally(throwable);
			}
		}));
		return resultFuture;
	}

	/**
	 * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint
	 * timestamp. The return value is a future. It completes when the checkpoint triggered finishes
	 * or an error occurred.
	 *
	 * @param isPeriodic Flag indicating whether this triggered checkpoint is
	 * periodic. If this flag is true, but the periodic scheduler is disabled,
	 * the checkpoint will be declined.
	 * @return a future to the completed checkpoint.
	 */
	public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
		return triggerCheckpoint(checkpointProperties, null, isPeriodic, false);
	}

	@VisibleForTesting
	public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime) {
		return triggerCheckpoint(props, externalSavepointLocation, isPeriodic, advanceToEndOfTime, -1L);
	}

	public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
		CheckpointProperties props,
		@Nullable String externalSavepointLocation,
		boolean isPeriodic,
		boolean advanceToEndOfTime,
		long savepointTimeout) {
		return triggerCheckpoint(props, externalSavepointLocation, isPeriodic, advanceToEndOfTime, savepointTimeout, DetachSavepointProperties.nonDetachSavepointProperties());
	}

	public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
		CheckpointProperties props,
		@Nullable String externalSavepointLocation,
		boolean isPeriodic,
		boolean advanceToEndOfTime,
		long savepointTimeout,
		DetachSavepointProperties detachSavepointProperties) {
		if (advanceToEndOfTime && !(props.isSynchronous() && props.isSavepoint())) {
			return FutureUtils.completedExceptionally(new IllegalArgumentException(
				"Only synchronous savepoints are allowed to advance the watermark to MAX."));
		}

		CheckpointTriggerRequest request = new CheckpointTriggerRequest(
			props, externalSavepointLocation, isPeriodic, advanceToEndOfTime, savepointTimeout, detachSavepointProperties);
		requestDecider
			.chooseRequestToExecute(request, isTriggering, lastCheckpointCompletionRelativeTime)
			.ifPresent(this::startTriggeringCheckpoint);
		return request.onCompletionPromise;
	}

	private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
		try {
			synchronized (lock) {
				preCheckGlobalState(request.isPeriodic);
			}

			PendingTriggerFactory.PendingTrigger pendingTrigger = request.props.isSavepoint() ?
				pendingTriggerFactory.prepareTriggerSavepoint() : pendingTriggerFactory.prepareTriggerCheckpoint();

			final Map<ExecutionAttemptID, ExecutionVertex> ackTasks = pendingTrigger.getAckTasks();

			// we will actually trigger this checkpoint!
			Preconditions.checkState(!isTriggering);
			isTriggering = true;

			final long timestamp = System.currentTimeMillis();
			final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
				initializeCheckpoint(request.props, request.externalSavepointLocation, request.detachSavepointProperties)
					.thenApplyAsync(
						(checkpointIdAndStorageLocation) -> createPendingCheckpoint(
							timestamp,
							request.props,
							ackTasks,
							request.isPeriodic,
							checkpointIdAndStorageLocation.checkpointId,
							checkpointIdAndStorageLocation.checkpointStorageLocation,
							request.getOnCompletionFuture(),
							request.getSavepointTimeout(),
							pendingTrigger),
						timer);

			final CompletableFuture<?> masterStatesComplete = pendingCheckpointCompletableFuture
					.thenCompose(this::snapshotMasterState);

			final CompletableFuture<?> coordinatorCheckpointsComplete = pendingCheckpointCompletableFuture
					.thenComposeAsync((pendingCheckpoint) ->
							OperatorCoordinatorCheckpoints.triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
									coordinatorsToCheckpoint, pendingCheckpoint, timer),
							timer);

			FutureUtils.assertNoException(
				CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete)
					.handleAsync(
						(ignored, throwable) -> {
							final PendingCheckpoint checkpoint =
								FutureUtils.getWithoutException(pendingCheckpointCompletableFuture);

							Preconditions.checkState(
								checkpoint != null || throwable != null,
								"Either the pending checkpoint needs to be created or an error must have been occurred.");

							if (throwable != null) {
								// the initialization might not be finished yet
								if (checkpoint == null) {
									onTriggerFailure(request, throwable);
								} else {
									onTriggerFailure(checkpoint, throwable);
								}
							} else {
								if (checkpoint.isDiscarded()) {
									onTriggerFailure(
										checkpoint,
										new CheckpointException(
											CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
											checkpoint.getFailureCause()));
								} else {
									// no exception, no discarding, everything is OK
									final long checkpointId = checkpoint.getCheckpointId();
									snapshotTaskState(
										timestamp,
										checkpointId,
										checkpoint.getCheckpointStorageLocation(),
										request.props,
										request.advanceToEndOfTime,
										pendingTrigger);

									coordinatorsToCheckpoint.forEach((ctx) -> ctx.afterSourceBarrierInjection(checkpointId));

									onTriggerSuccess();
								}
							}

							return null;
						},
						timer)
					.exceptionally(error -> {
						if (!isShutdown()) {
							throw new CompletionException(error);
						} else if (findThrowable(error, RejectedExecutionException.class).isPresent()) {
							LOG.debug("Execution rejected during shutdown");
						} else {
							LOG.warn("Error encountered during shutdown", error);
						}
						return null;
					}));
		} catch (Throwable throwable) {
			onTriggerFailure(request, throwable);
		}
	}

	/**
	 * Initialize the checkpoint trigger asynchronously. It will be executed in io thread due to
	 * it might be time-consuming.
	 *
	 * @param props checkpoint properties
	 * @param externalSavepointLocation the external savepoint location, it might be null
	 * @return the future of initialized result, checkpoint id and checkpoint location
	 */
	private CompletableFuture<CheckpointIdAndStorageLocation> initializeCheckpoint(
		CheckpointProperties props,
		@Nullable String externalSavepointLocation,
		DetachSavepointProperties detachSavepointProperties) {

		return CompletableFuture.supplyAsync(() -> {
			try {
				// this must happen outside the coordinator-wide lock, because it communicates
				// with external services (in HA mode) and may block for a while.
				long checkpointID = checkpointIdCounter.getAndIncrement();

				CheckpointStorageLocation checkpointStorageLocation;
				if (detachSavepointProperties.isDetachSavepoint) {
					checkpointStorageLocation = checkpointStorage.initializeLocationForDetachSavepoint(checkpointID, externalSavepointLocation);
					pendingSavepointsUnsafe.put(checkpointID, detachSavepointProperties.savepointId);
				} else {
					checkpointStorageLocation = props.isSavepoint() ?
						checkpointStorage
							.initializeLocationForSavepoint(checkpointID, externalSavepointLocation) :
						checkpointStorage.initializeLocationForCheckpoint(checkpointID);
				}

				return new CheckpointIdAndStorageLocation(checkpointID, checkpointStorageLocation);
			} catch (Throwable throwable) {
				throw new CompletionException(throwable);
			}
		}, executor);
	}

	private PendingCheckpoint createPendingCheckpoint(
		long timestamp,
		CheckpointProperties props,
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks,
		boolean isPeriodic,
		long checkpointID,
		CheckpointStorageLocation checkpointStorageLocation,
		CompletableFuture<CompletedCheckpoint> onCompletionPromise,
		long savepointTimeout,
		PendingTriggerFactory.PendingTrigger pendingTrigger) {
		synchronized (lock) {
			try {
				// since we haven't created the PendingCheckpoint yet, we need to check the
				// global state here.
				preCheckGlobalState(isPeriodic);
			} catch (Throwable t) {
				throw new CompletionException(t);
			}
		}

		final PendingCheckpoint checkpoint = new PendingCheckpoint(
			job,
			checkpointID,
			timestamp,
			ackTasks,
			OperatorInfo.getIds(coordinatorsToCheckpoint),
			masterHooks.keySet(),
			props,
			checkpointStorageLocation,
			executor,
			onCompletionPromise,
			pendingTrigger,
			checkpointStorage,
			transferMaxRetryAttempts);

		if (statsTracker != null) {
			PendingCheckpointStats callback = statsTracker.reportPendingCheckpoint(
				checkpointID,
				timestamp,
				props,
				pendingTrigger.getCommitToTasks().length);

			checkpoint.setStatsCallback(callback);
		}

		synchronized (lock) {
			pendingCheckpoints.put(checkpointID, checkpoint);

			ScheduledFuture<?> cancellerHandle = null;
			if (props.isSavepoint() && props.isSynchronous() && savepointTimeout > 0) {
				cancellerHandle = checkpointScheduler.scheduleTimeoutCanceller(savepointTimeout, new CheckpointCanceller(checkpoint));
			} else {
				cancellerHandle = checkpointScheduler.scheduleTimeoutCanceller(new CheckpointCanceller(checkpoint));
			}

			if (!checkpoint.setCancellerHandle(cancellerHandle)) {
				// checkpoint is already disposed!
				cancellerHandle.cancel(false);
			}
		}

		LOG.info("Triggering checkpoint {} (type={}) @ {} for job {}.", checkpointID, checkpoint.getProps().getCheckpointType(), timestamp, job);
		return checkpoint;
	}

	/**
	 * Snapshot master hook states asynchronously.
	 *
	 * @param checkpoint the pending checkpoint
	 * @return the future represents master hook states are finished or not
	 */
	private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
		if (masterHooks.isEmpty()) {
			return CompletableFuture.completedFuture(null);
		}

		final long checkpointID = checkpoint.getCheckpointId();
		final long timestamp = checkpoint.getCheckpointTimestamp();

		final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
		for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
			MasterHooks
				.triggerHook(masterHook, checkpointID, timestamp, executor)
				.whenCompleteAsync(
					(masterState, throwable) -> {
						try {
							synchronized (lock) {
								if (masterStateCompletableFuture.isDone()) {
									return;
								}
								if (checkpoint.isDiscarded()) {
									throw new IllegalStateException(
										"Checkpoint " + checkpointID + " has been discarded");
								}
								if (throwable == null) {
									checkpoint.acknowledgeMasterState(
										masterHook.getIdentifier(), masterState);
									if (checkpoint.areMasterStatesFullyAcknowledged()) {
										masterStateCompletableFuture.complete(null);
									}
								} else {
									masterStateCompletableFuture.completeExceptionally(throwable);
								}
							}
						} catch (Throwable t) {
							masterStateCompletableFuture.completeExceptionally(t);
						}
					},
					timer);
		}
		return masterStateCompletableFuture;
	}

	/**
	 * Snapshot task state.
	 *
	 * @param timestamp the timestamp of this checkpoint reques
	 * @param checkpointID the checkpoint id
	 * @param checkpointStorageLocation the checkpoint location
	 * @param props the checkpoint properties
	 * @param advanceToEndOfTime Flag indicating if the source should inject a {@code MAX_WATERMARK}
	 *                               in the pipeline to fire any registered event-time timers.
	 * @param pendingTrigger Determine which tasks need to trigger Checkpoint.
	 */
	private void snapshotTaskState(
		long timestamp,
		long checkpointID,
		CheckpointStorageLocation checkpointStorageLocation,
		CheckpointProperties props,
		boolean advanceToEndOfTime,
		PendingTriggerFactory.PendingTrigger pendingTrigger) {

		final CheckpointOptions checkpointOptions = new CheckpointOptions(
			props.getCheckpointType(),
			checkpointStorageLocation.getLocationReference(),
			isExactlyOnceMode,
			props.getCheckpointType() == CheckpointType.CHECKPOINT && unalignedCheckpointsEnabled);
		pendingTrigger.setCheckpointOptions(checkpointOptions);

		// send the messages to the tasks that trigger their checkpoint
		for (Execution execution: pendingTrigger.getNextTriggerTasks()) {
			if (props.isSynchronous()) {
				execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
			} else {
				execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
			}
		}
	}

	/**
	 * Trigger request is successful.
	 * NOTE, it must be invoked if trigger request is successful.
	 */
	private void onTriggerSuccess() {
		isTriggering = false;
		numUnsuccessfulCheckpointsTriggers.set(0);
		executeQueuedRequest();
	}

	/**
	 * The trigger request is failed prematurely without a proper initialization.
	 * There is no resource to release, but the completion promise needs to fail manually here.
	 *
	 * @param onCompletionPromise the completion promise of the checkpoint/savepoint
	 * @param throwable the reason of trigger failure
	 */
	private void onTriggerFailure(
		CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
		final CheckpointException checkpointException =
			getCheckpointException(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
		onCompletionPromise.completeExceptionally(checkpointException);
		onTriggerFailure((PendingCheckpoint) null, checkpointException);
	}

	/**
	 * The trigger request is failed.
	 * NOTE, it must be invoked if trigger request is failed.
	 *
	 * @param checkpoint the pending checkpoint which is failed. It could be null if it's failed
	 *                   prematurely without a proper initialization.
	 * @param throwable the reason of trigger failure
	 */
	private void onTriggerFailure(@Nullable PendingCheckpoint checkpoint, Throwable throwable) {
		// beautify the stack trace a bit
		throwable = ExceptionUtils.stripCompletionException(throwable);

		try {
			coordinatorsToCheckpoint.forEach(OperatorCoordinatorCheckpointContext::abortCurrentTriggering);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn(
					"Failed to trigger checkpoint {} for job {}. ({} consecutive failed attempts so far)",
					checkpoint.getCheckpointId(),
					job,
					numUnsuccessful,
					throwable);
				final CheckpointException cause =
					getCheckpointException(
						CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
				synchronized (lock) {
					abortPendingCheckpoint(checkpoint, cause);
				}
			}
		} finally {
			isTriggering = false;
			executeQueuedRequest();
		}
	}

	private void executeQueuedRequest() {
		requestDecider.chooseQueuedRequestToExecute(isTriggering, lastCheckpointCompletionRelativeTime).ifPresent(this::startTriggeringCheckpoint);
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	/**
	 * Receives a {@link PerformCheckpoint} message for a pending checkpoint.
	 *
	 * @param message Checkpoint perform from the task manager
	 */
	public void receivePerformCheckpointMessage(PerformCheckpoint message) {
		if (shutdown || message == null) {
			return;
		}

		if (!job.equals(message.getJob())) {
			throw new IllegalArgumentException("Received perform checkpoint message for job " +
				message.getJob() + " while this coordinator handles job " + job);
		}

		final long checkpointId = message.getCheckpointId();
		PendingCheckpoint checkpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}

			checkpoint = pendingCheckpoints.get(checkpointId);
			if (checkpoint != null && !checkpoint.isDiscarded()) {
				checkpoint.getPendingTrigger()
					.notifyPerformCheckpoint(checkpointId, checkpoint.getCheckpointTimestamp(), message.getTaskExecutionId());
			} else {
				LOG.warn("Received perform checkpoint message for checkpoint {} from task {} of job {}. " +
					"But the checkpoint might has expired.", checkpointId, message.getTaskExecutionId(), message.getJob());
			}
		}
	}

	/**
	 * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
	 *
	 * @param message Checkpoint decline from the task manager
	 * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
	 */
	public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
		if (shutdown || message == null) {
			return;
		}

		if (!job.equals(message.getJob())) {
			throw new IllegalArgumentException("Received DeclineCheckpoint message for job " +
				message.getJob() + " from " + taskManagerLocationInfo + " while this coordinator handles job " + job);
		}

		final long checkpointId = message.getCheckpointId();
		final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

		PendingCheckpoint checkpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}

			checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				if (sendActionToHandler(handler -> handler.tryHandleDeclineMessage(message),
						Collections.singleton(checkpoint.getCheckpointId()))) {
					LOG.info("Decline message {} is ignored.", message.toString());
					return;
				}
			}

			if (checkpoint != null) {
				Preconditions.checkState(
					!checkpoint.isDiscarded(),
					"Received message for discarded but non-removed checkpoint " + checkpointId);
				LOG.info("Decline checkpoint {} by task {} of job {} at {}.",
					checkpointId,
					message.getTaskExecutionId(),
					job,
					taskManagerLocationInfo);
				final CheckpointException checkpointException;
				if (message.getReason() == null) {
					checkpointException =
						new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED);
				} else {
					checkpointException = getCheckpointException(
						CheckpointFailureReason.JOB_FAILURE, message.getReason());
				}
				abortPendingCheckpoint(
					checkpoint,
					checkpointException,
					message.getTaskExecutionId());
			} else if (LOG.isDebugEnabled()) {
				if (recentPendingCheckpoints.contains(checkpointId)) {
					// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
					LOG.debug("Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
							checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
				} else {
					// message is for an unknown checkpoint. might be so old that we don't even remember it any more
					LOG.debug("Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
							checkpointId, message.getTaskExecutionId(), job, taskManagerLocationInfo, reason);
				}
			}
		}
	}

	/**
	 * Receives an AcknowledgeCheckpoint message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint ack from the task manager
	 *
	 * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
	 * @return Flag indicating whether the ack'd checkpoint was associated
	 * with a pending checkpoint.
	 *
	 * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint store.
	 */
	public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) throws CheckpointException {
		if (shutdown || message == null) {
			return false;
		}

		if (!job.equals(message.getJob())) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job {} from {} : {}", job, taskManagerLocationInfo, message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				if (sendActionToHandler(handler -> handler.tryHandleAck(message), Collections.singleton(checkpoint.getCheckpointId()))) {
					return false;
				}

				switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
					case SUCCESS:
						LOG.debug("Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
							checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						if (checkpoint.areTasksFullyAcknowledged()) {
							completePendingCheckpoint(checkpoint);
						}
						break;
					case DUPLICATE:
						LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
						break;
					case UNKNOWN:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
								"because the task's execution attempt id was unknown. Discarding " +
								"the state handle to avoid lingering state.", message.getCheckpointId(),
							message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

						break;
					case DISCARDED:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {} at {}, " +
							"because the pending checkpoint had been discarded. Discarding the " +
								"state handle tp avoid lingering state.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);

						discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());
				}

				return true;
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else {
				boolean wasPendingCheckpoint;

				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					wasPendingCheckpoint = true;
					LOG.warn("Received late message for now expired checkpoint attempt {} from task " +
						"{} of job {} at {}.", checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
				}
				else {
					LOG.debug("Received message for an unknown checkpoint {} from task {} of job {} at {}.",
						checkpointId, message.getTaskExecutionId(), message.getJob(), taskManagerLocationInfo);
					wasPendingCheckpoint = false;
				}

				// try to discard the state so that we don't have lingering state lying around
				discardSubtaskState(message.getJob(), message.getTaskExecutionId(), message.getCheckpointId(), message.getSubtaskState());

				return wasPendingCheckpoint;
			}
		}
	}

	/**
	 * Try to complete the given pending checkpoint.
	 *
	 * <p>Important: This method should only be called in the checkpoint lock scope.
	 *
	 * @param pendingCheckpoint to complete
	 * @throws CheckpointException if the completion failed
	 */
	private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
		final long checkpointId = pendingCheckpoint.getCheckpointId();
		final CompletedCheckpoint completedCheckpoint;

		// As a first step to complete the checkpoint, we register its state with the registry
		Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
		sharedStateRegistry.registerAll(operatorStates.values());

		try {
			try {
				completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();
				failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
			}
			catch (Exception e1) {
				// abort the current pending checkpoint if we fails to finalize the pending checkpoint.
				if (!pendingCheckpoint.isDiscarded()) {
					abortPendingCheckpoint(
						pendingCheckpoint,
						new CheckpointException(
							CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1));
				}

				throw new CheckpointException("Could not finalize the pending checkpoint " + checkpointId + '.',
					CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
			}

			// the pending checkpoint must be discarded after the finalization
			Preconditions.checkState(pendingCheckpoint.isDiscarded() && completedCheckpoint != null);

			try {
				completedCheckpointStore.addCheckpoint(completedCheckpoint);
			} catch (Exception exception) {
				// we failed to store the completed checkpoint. Let's clean up
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							completedCheckpoint.discardOnFailedStoring();
						} catch (Throwable t) {
							LOG.warn("Could not properly discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), t);
						}
					}
				});

				sendAbortedMessages(checkpointId, pendingCheckpoint.getCheckpointTimestamp());
				throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.',
					CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, exception);
			}

			checkpointHandler.onCheckpointComplete(completedCheckpoint);
		} finally {
			pendingCheckpoints.remove(checkpointId);
			pendingSavepointsUnsafe.remove(checkpointId);
			timer.execute(this::executeQueuedRequest);
		}

		rememberRecentCheckpointId(checkpointId);

		// drop those pending checkpoints that are at prior to the completed one
		dropSubsumedCheckpoints(checkpointId);

		// record the time when this was completed, to calculate
		// the 'min delay between checkpoints'
		lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

		LOG.info("Completed checkpoint {} for job {} ({} bytes in {} ms).", checkpointId, job,
			completedCheckpoint.getStateSize(), completedCheckpoint.getDuration());

		if (LOG.isDebugEnabled()) {
			StringBuilder builder = new StringBuilder();
			builder.append("Checkpoint state: ");
			for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
				builder.append(state);
				builder.append(", ");
			}
			// Remove last two chars ", "
			builder.setLength(builder.length() - 2);

			LOG.debug(builder.toString());
		}

		// send the "notify complete" call to all vertices, coordinators, etc.
		ExecutionVertex[] tasksToCommitTo = pendingCheckpoint.getPendingTrigger().getCommitToTasks();
		sendAcknowledgeMessages(tasksToCommitTo, checkpointId, completedCheckpoint.getTimestamp());
	}

	private void sendAcknowledgeMessages(ExecutionVertex[] tasksToCommitTo, long checkpointId, long timestamp) {
		// commit tasks
		for (ExecutionVertex ev : tasksToCommitTo) {
			Execution ee = ev.getCurrentExecutionAttempt();
			if (ee != null) {
				// ignore if this vertex doesn't finish its own checkpoint
				if (!sendNotificationToHandler(handler -> handler.tryHandleCompletedNotification(ee.getVertex(), checkpointId),
						Collections.singleton(checkpointId))) {
					ee.notifyCheckpointComplete(checkpointId, timestamp);
				}
			}
		}

		// commit coordinators
		for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
			coordinatorContext.checkpointComplete(checkpointId);
		}
	}

	private void sendAbortedMessages(long checkpointId, long timeStamp) {
		// send notification of aborted checkpoints asynchronously.
		executor.execute(() -> {
			// send the "abort checkpoint" messages to necessary vertices.
			for (ExecutionVertex ev : tasksToCommitTo) {
				Execution ee = ev.getCurrentExecutionAttempt();
				if (ee != null) {
					ee.notifyCheckpointAborted(checkpointId, timeStamp);
				}
			}
		});
	}

	/**
	 * Fails all pending checkpoints which have not been acknowledged by the given execution
	 * attempt id.
	 *
	 * @param executionAttemptId for which to discard unacknowledged pending checkpoints
	 * @param cause of the failure
	 */
	public void failUnacknowledgedPendingCheckpointsFor(ExecutionAttemptID executionAttemptId, Throwable cause) {
		synchronized (lock) {
			if (sendActionToHandler(handler -> handler.tryHandleFailUnacknowledgedPendingCheckpoints(executionAttemptId, cause),
				pendingCheckpoints.values().stream().map(PendingCheckpoint::getCheckpointId).collect(Collectors.toList()))) {
				return;
			}

			abortPendingCheckpoints(
				checkpoint -> !checkpoint.isAcknowledgedBy(executionAttemptId),
				new CheckpointException(CheckpointFailureReason.TASK_FAILURE, cause));
		}
	}

	private void rememberRecentCheckpointId(long id) {
		if (recentPendingCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
			recentPendingCheckpoints.removeFirst();
		}
		recentPendingCheckpoints.addLast(id);
	}

	private void dropSubsumedCheckpoints(long checkpointId) {
		abortPendingCheckpoints(
			checkpoint -> checkpoint.getCheckpointId() < checkpointId && checkpoint.canBeSubsumed(),
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_SUBSUMED));
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpoint State Restoring
	// --------------------------------------------------------------------------------------------

	/**
	 * Restores the latest checkpointed state.
	 *
	 * @param tasks Map of job vertices to restore. State for these vertices is
	 * restored via {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param errorIfNoCheckpoint Fail if no completed checkpoint is available to
	 * restore from.
	 * @param allowNonRestoredState Allow checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 * @return <code>true</code> if state was restored, <code>false</code> otherwise.
	 * @throws IllegalStateException If the CheckpointCoordinator is shut down.
	 * @throws IllegalStateException If no completed checkpoint is available and
	 *                               the <code>failIfNoCheckpoint</code> flag has been set.
	 * @throws IllegalStateException If the checkpoint contains state that cannot be
	 *                               mapped to any job vertex in <code>tasks</code> and the
	 *                               <code>allowNonRestoredState</code> flag has not been set.
	 * @throws IllegalStateException If the max parallelism changed for an operator
	 *                               that restores state from this checkpoint.
	 * @throws IllegalStateException If the parallelism changed for an operator
	 *                               that restores <i>non-partitioned</i> state from this
	 *                               checkpoint.
	 */
	@Deprecated
	public boolean restoreLatestCheckpointedState(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean errorIfNoCheckpoint,
			boolean allowNonRestoredState) throws Exception {

		return restoreLatestCheckpointedStateInternal(new HashSet<>(tasks.values()), true, errorIfNoCheckpoint, allowNonRestoredState, false, false, null);
	}

	/**
	 * Restores the latest checkpointed state to a set of subtasks. This method represents a "local"
	 * or "regional" failover and does restore states to coordinators. Note that a regional failover
	 * might still include all tasks.
	 *
	 * @param tasks Set of job vertices to restore. State for these vertices is
	 * restored via {@link Execution#setInitialState(JobManagerTaskRestore)}.

	 * @return <code>true</code> if state was restored, <code>false</code> otherwise.
	 * @throws IllegalStateException If the CheckpointCoordinator is shut down.
	 * @throws IllegalStateException If no completed checkpoint is available and
	 *                               the <code>failIfNoCheckpoint</code> flag has been set.
	 * @throws IllegalStateException If the checkpoint contains state that cannot be
	 *                               mapped to any job vertex in <code>tasks</code> and the
	 *                               <code>allowNonRestoredState</code> flag has not been set.
	 * @throws IllegalStateException If the max parallelism changed for an operator
	 *                               that restores state from this checkpoint.
	 * @throws IllegalStateException If the parallelism changed for an operator
	 *                               that restores <i>non-partitioned</i> state from this
	 *                               checkpoint.
	 */
	public boolean restoreLatestCheckpointedStateToSubtasks(final Set<ExecutionJobVertex> tasks) throws Exception {
		// when restoring subtasks only we accept potentially unmatched state for the
		// following reasons
		//   - the set frequently does not include all Job Vertices (only the ones that are part
		//     of the restarted region), meaning there will be unmatched state by design.
		//   - because what we might end up restoring from an original savepoint with unmatched
		//     state, if there is was no checkpoint yet.
		return restoreLatestCheckpointedStateInternal(tasks, false, false, true, false, false, null);
	}

	/**
	 * Restores the latest checkpointed state to all tasks and all coordinators.
	 * This method represents a "global restore"-style operation where all stateful tasks
	 * and coordinators from the given set of Job Vertices are restored.
	 * are restored to their latest checkpointed state.
	 *
	 * @param tasks Set of job vertices to restore. State for these vertices is
	 * restored via {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param allowNonRestoredState Allow checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 * @return <code>true</code> if state was restored, <code>false</code> otherwise.
	 * @throws IllegalStateException If the CheckpointCoordinator is shut down.
	 * @throws IllegalStateException If no completed checkpoint is available and
	 *                               the <code>failIfNoCheckpoint</code> flag has been set.
	 * @throws IllegalStateException If the checkpoint contains state that cannot be
	 *                               mapped to any job vertex in <code>tasks</code> and the
	 *                               <code>allowNonRestoredState</code> flag has not been set.
	 * @throws IllegalStateException If the max parallelism changed for an operator
	 *                               that restores state from this checkpoint.
	 * @throws IllegalStateException If the parallelism changed for an operator
	 *                               that restores <i>non-partitioned</i> state from this
	 *                               checkpoint.
	 */
	public boolean restoreLatestCheckpointedStateToAll(
			final Set<ExecutionJobVertex> tasks,
			final boolean allowNonRestoredState) throws Exception {

		return restoreLatestCheckpointedStateInternal(tasks, true, false, allowNonRestoredState, false, false, null);
	}

	public boolean restoreLatestCheckpointedStateToAll(
			final Set<ExecutionJobVertex> tasks,
			final boolean allowNonRestoredState,
			final ClassLoader loader) throws Exception {

		return restoreLatestCheckpointedStateInternal(tasks, true, false, allowNonRestoredState, true, false, loader);
	}

	private boolean restoreLatestCheckpointedStateInternal(
		final Set<ExecutionJobVertex> tasks,
		final boolean restoreCoordinators,
		final boolean errorIfNoCheckpoint,
		final boolean allowNonRestoredState,
		final boolean fromStorage,
		final boolean fromSavepoint,
		@Nullable ClassLoader userClassLoader) throws Exception {

		synchronized (lock) {
			if (shutdown) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			CompletedCheckpoint latest;
			if (!fromSavepoint) {
				List<CompletedCheckpoint> oriCompletedCheckpoints = completedCheckpointStore.getAllCheckpoints();

				// Recover the checkpoints, TODO this could be done only when there is a new leader, not on each recovery
				completedCheckpointStore.recover();

				/* ---------------- DC Failure Tolerance ---------------- */

				final Set<CompletedCheckpoint> checkpointsOnStorage = findAllCompletedCheckpointsOnStorage(
					tasks, allowNonRestoredState, fromStorage, userClassLoader);
				LOG.info("Find {} checkpoints {} on HDFS.", checkpointsOnStorage.size(),
						checkpointsOnStorage.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet()));
				final Set<Long> checkpointsOnStore = completedCheckpointStore.getAllCheckpoints()
					.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet());
				final Set<CompletedCheckpoint> extraCheckpoints = new HashSet<>();
				for (CompletedCheckpoint checkpoint : checkpointsOnStorage) {
					if (!checkpointsOnStore.contains(checkpoint.getCheckpointID())) {
						extraCheckpoints.add(checkpoint);
					}
				}

				long latestCheckpointIdOnStore = checkpointsOnStore.stream().max(Long::compareTo).orElse(-1L);
				long latestCheckpointIdOnStorage = checkpointsOnStorage.stream().map(CompletedCheckpoint::getCheckpointID).max(Long::compareTo).orElse(-1L);

				// checkpoints on HDFS but not on zookeeper!!!
				LOG.info("There are {} checkpoints are on HDFS but not on Zookeeper.", extraCheckpoints.size());
				if (extraCheckpoints.size() > 0) {
					final List<CompletedCheckpoint> extraCheckpointsSortedList = new ArrayList<>(extraCheckpoints);
					extraCheckpointsSortedList.sort((o1, o2) -> new Long(o1.getCheckpointID() - o2.getCheckpointID()).intValue());
					for (CompletedCheckpoint checkpoint : extraCheckpointsSortedList) {
						completedCheckpointStore.addCheckpointInOrder(checkpoint);
					}
				}

				/* ---------------- DC Failure Tolerance --------------------- */

				// Only re-register shared state when the CompletedCheckpointStore changes.
				List<CompletedCheckpoint> curCompletedCheckpoints = completedCheckpointStore.getAllCheckpoints();
				if (!ListUtils.isEqualList(oriCompletedCheckpoints, curCompletedCheckpoints)) {
					LOG.info("The completed checkpoint store has changed, and the shared state needs to be re-registered. " +
							"Previous checkpoints are {}, restored checkpoints are: {}.",
						oriCompletedCheckpoints.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet()),
						curCompletedCheckpoints.stream().map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet()));
					// We create a new shared state registry object, so that all pending async disposal requests from previous
					// runs will go against the old object (were they can do no harm).
					// This must happen under the checkpoint lock.
					sharedStateRegistry.close();
					sharedStateRegistry = sharedStateRegistryFactory.create(executor);

					// Now, we re-register all (shared) states from the checkpoint store with the new registry
					for (CompletedCheckpoint completedCheckpoint : completedCheckpointStore.getAllCheckpoints()) {
						completedCheckpoint.registerSharedStatesAfterRestored(sharedStateRegistry);
						// inject Checkpoint Storage to expire savepoint simple metadata
						completedCheckpoint.setCheckpointStorage(checkpointStorage);
					}
				} else {
					LOG.info("The completed checkpoint store has not changed, no need to re-register shared state.");
				}
				LOG.info("After restoring CompletedCheckpointStore, checkpoints {}, savepoints {}.",
					completedCheckpointStore.getAllCheckpoints().stream().filter(CompletedCheckpoint::isCheckpoint).map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet()),
					completedCheckpointStore.getAllCheckpoints().stream().filter(CompletedCheckpoint::isSavepoint).map(CompletedCheckpoint::getCheckpointID).collect(Collectors.toSet()));

				LOG.debug("Status of the shared state registry of job {} after restore: {}.", job, sharedStateRegistry);

				// Restore from the latest checkpoint
				latest = completedCheckpointStore.getLatestCheckpoint(isPreferCheckpointForRecovery);
				if (latest != null && latestCheckpointIdOnStorage > latestCheckpointIdOnStore) {
					checkpointIdCounter.setCount(latest.getCheckpointID() + 1);
				}

				if (latest == null) {
					if (errorIfNoCheckpoint) {
						throw new IllegalStateException("No completed checkpoint available");
					} else {
						LOG.debug("Resetting the master hooks.");
						MasterHooks.reset(masterHooks.values(), LOG);

						return false;
					}
				}
			} else {
				latest = completedCheckpointStore.getLatestCheckpoint(false);
			}

			LOG.info("Restoring job {} from latest valid checkpoint: {}.", job, latest);

			// re-assign the task states
			final Map<OperatorID, OperatorState> operatorStates = latest.getOperatorStates();

			UnionStateAggregator unionStateAggregator = aggregateUnionState ?
				new FileUnionStateAggregator(latest.getExternalPointer()) : new NonUnionStateAggregator();
			StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState, unionStateAggregator);

			stateAssignmentOperation.assignStates();

			// call master hooks for restore. we currently call them also on "regional restore" because
			// there is no other failure notification mechanism in the master hooks
			// ultimately these should get removed anyways in favor of the operator coordinators

			MasterHooks.restoreMasterHooks(
					masterHooks,
					latest.getMasterHookStates(),
					latest.getCheckpointID(),
					allowNonRestoredState,
					LOG);

			if (restoreCoordinators) {
				restoreStateToCoordinators(operatorStates);
			}

			// update metrics

			if (statsTracker != null) {
				long restoreTimestamp = System.currentTimeMillis();
				RestoredCheckpointStats restored = new RestoredCheckpointStats(
					latest.getCheckpointID(),
					latest.getProperties(),
					restoreTimestamp,
					latest.getExternalPointer());

				statsTracker.reportRestoredCheckpoint(restored);
			}

			return true;
		}
	}

	@VisibleForTesting
	public Set<CompletedCheckpoint> findAllCompletedCheckpointsOnStorage(
			Set<ExecutionJobVertex> tasks,
			boolean allowNonRestoredState,
			boolean findCheckpointInCheckpointStore,
			@Nullable ClassLoader userClassLoader) throws IOException {
		final Set<CompletedCheckpoint> result = new HashSet<>();
		final Map<JobVertexID, ExecutionJobVertex> mappings = new HashMap<>();
		tasks.forEach(ejv -> mappings.put(ejv.getJobVertexId(), ejv));

		int onRetrievingCheckpointsIdx = 0;
		if (findCheckpointInCheckpointStore && userClassLoader != null) {
			// tuple.f0: external pointer, tuple.f1: if completeCheckpoint is savepoint?
			List<Tuple2<String, Boolean>> completedCheckpointPointersOnStorage;
			completedCheckpointPointersOnStorage = checkpointStorage.findCompletedCheckpointPointerV2();
			for (Tuple2<String, Boolean> completedCheckpointPointer : completedCheckpointPointersOnStorage) {
				try {
					if (result.size() >= completedCheckpointStore.getMaxNumberOfRetainedCheckpoints()) {
						int numCheckpointsOnStorage = completedCheckpointPointersOnStorage.size();
						LOG.info("HDFS has loaded {} completed checkpoints, skip checkpoints {}",
							result.size(), completedCheckpointPointersOnStorage.subList(onRetrievingCheckpointsIdx, numCheckpointsOnStorage));
						break;
					} else {
						onRetrievingCheckpointsIdx++;
					}
					final CompletedCheckpointStorageLocation checkpointStorageLocation;
					if (completedCheckpointPointer.f1) {
						checkpointStorageLocation = checkpointStorage.resolveSavepoint(completedCheckpointPointer.f0);
					} else {
						checkpointStorageLocation = checkpointStorage.resolveCheckpoint(completedCheckpointPointer.f0);
					}
					final CompletedCheckpoint completedCheckpoint = Checkpoints.loadAndValidateCheckpoint(
							job, mappings, checkpointStorageLocation, userClassLoader, allowNonRestoredState, completedCheckpointPointer.f1);
					result.add(completedCheckpoint);
				} catch (IllegalStateException e) {
					LOG.error("Failed to rollback to checkpoint/savepoint from {}", completedCheckpointPointer.f0, e);
					throw e;
				} catch (Exception e) {
					LOG.warn("Fail to load checkpoint on {}.", completedCheckpointPointer, e);
				}
			}
		}

		return Collections.unmodifiableSet(result);
	}

	/**
	 * Restore the state with given savepoint.
	 *
	 * @param savepointPointer The pointer to the savepoint.
	 * @param allowNonRestored True if allowing checkpoint state that cannot be
	 *                         mapped to any job vertex in tasks.
	 * @param tasks            Map of job vertices to restore. State for these
	 *                         vertices is restored via
	 *                         {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param userClassLoader  The class loader to resolve serialized classes in
	 *                         legacy savepoint versions.
	 */
	public boolean restoreSavepoint(
			String savepointPointer,
			boolean allowNonRestored,
			Map<JobVertexID, ExecutionJobVertex> tasks,
			ClassLoader userClassLoader) throws Exception {

		Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

		LOG.info("Starting job {} from savepoint {} ({})",
				job, savepointPointer, (allowNonRestored ? "allowing non restored state" : ""));

		final CompletedCheckpointStorageLocation checkpointLocation = checkpointStorage.resolveCheckpoint(savepointPointer);

		// Load the savepoint as a checkpoint into the system
		CompletedCheckpoint savepoint = Checkpoints.loadAndValidateCheckpoint(
				job, tasks, checkpointLocation, userClassLoader, allowNonRestored);

		completedCheckpointStore.addCheckpoint(savepoint);

		// Reset the checkpoint ID counter
		long nextCheckpointId = savepoint.getCheckpointID() + 1;
		checkpointIdCounter.setCount(nextCheckpointId);

		LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

		return restoreLatestCheckpointedStateInternal(new HashSet<>(tasks.values()), true, true, allowNonRestored, true, true, userClassLoader);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public int getNumberOfPendingCheckpoints() {
		synchronized (lock) {
			return this.pendingCheckpoints.size();
		}
	}

	public int getNumberOfRetainedSuccessfulCheckpoints() {
		synchronized (lock) {
			return completedCheckpointStore.getNumberOfRetainedCheckpoints();
		}
	}

	public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
		synchronized (lock) {
			return new HashMap<>(this.pendingCheckpoints);
		}
	}

	public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
		synchronized (lock) {
			return completedCheckpointStore.getAllCheckpoints();
		}
	}

	public CheckpointStorageCoordinatorView getCheckpointStorage() {
		return checkpointStorage;
	}

	public CompletedCheckpointStore getCheckpointStore() {
		return completedCheckpointStore;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	/**
	 * @deprecated use {@link #getNumQueuedRequests()}
	 */
	@Deprecated
	@VisibleForTesting
	PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
		return requestDecider.getTriggerRequestQueue();
	}

	public boolean isTriggering() {
		return isTriggering;
	}

	public CheckpointIDCounter getCheckpointIdCounter() {
		return checkpointIdCounter;
	}

	@VisibleForTesting
	boolean isCurrentPeriodicTriggerAvailable() {
		return checkpointScheduler.inPeriodicallyScheduling();
	}

	/**
	 * Returns whether periodic checkpointing has been configured.
	 *
	 * @return <code>true</code> if periodic checkpoints have been configured.
	 */
	public boolean isPeriodicCheckpointingConfigured() {
		return checkpointScheduler.isPeriodicCheckpointingConfigured();
	}

	// --------------------------------------------------------------------------------------------
	//  Periodic scheduling of checkpoints
	// --------------------------------------------------------------------------------------------

	public void startCheckpointScheduler() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			// make sure all prior timers are cancelled
			stopCheckpointScheduler();

			periodicScheduling = true;
			checkpointScheduler.startScheduling();
		}
	}

	public void stopCheckpointScheduler() {
		synchronized (lock) {
			periodicScheduling = false;

			checkpointScheduler.stopScheduling();

			final CheckpointException reason =
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
			abortPendingAndQueuedCheckpoints(reason);

			numUnsuccessfulCheckpointsTriggers.set(0);
		}
	}

	public void startCheckpointScheduler(JobStatus newJobStatus) {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (!periodicScheduling) {
				// make sure all prior timers are cancelled
				stopCheckpointScheduler();

				periodicScheduling = true;
				checkpointScheduler.startScheduling();
				LOG.info("Start CheckpointScheduler when JobStatus transition to {}.", newJobStatus);
			}
		}
	}

	public void stopCheckpointScheduler(JobStatus newJobStatus) {
		synchronized (lock) {
			if (newJobStatus.isTerminalState()) {
				periodicScheduling = false;
				checkpointScheduler.stopScheduling();

				final CheckpointException reason =
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
				abortPendingAndQueuedCheckpoints(reason);

				numUnsuccessfulCheckpointsTriggers.set(0);
				LOG.info("Stop CheckpointScheduler when JobStatus transition to {}.", newJobStatus);
			}
		}
	}

	public void onTaskFailure(Collection<ExecutionVertex> vertices, CheckpointException exception) {
		synchronized (lock) {
			if (sendActionToHandler(handler -> handler.tryHandleTasksFailure(vertices),
					pendingCheckpoints.values().stream().map(PendingCheckpoint::getCheckpointId).collect(Collectors.toList()))) {
				return;
			}

			abortPendingCheckpoints(exception);
		}
	}

	/**
	 * Aborts all the pending checkpoints due to en exception.
	 * @param exception The exception.
	 */
	public void abortPendingCheckpoints(CheckpointException exception) {
		synchronized (lock) {
			abortPendingCheckpoints(ignored -> true, exception);
		}
	}

	private void abortPendingCheckpoints(
		Predicate<PendingCheckpoint> checkpointToFailPredicate,
		CheckpointException exception) {

		assert Thread.holdsLock(lock);

		final PendingCheckpoint[] pendingCheckpointsToFail = pendingCheckpoints
			.values()
			.stream()
			.filter(checkpointToFailPredicate)
			.toArray(PendingCheckpoint[]::new);

		// do not traverse pendingCheckpoints directly, because it might be changed during traversing
		for (PendingCheckpoint pendingCheckpoint : pendingCheckpointsToFail) {
			abortPendingCheckpoint(pendingCheckpoint, exception);
		}
	}

	private void rescheduleTrigger(long tillNextMillis) {}

	private void restoreStateToCoordinators(final Map<OperatorID, OperatorState> operatorStates) throws Exception {
		for (OperatorCoordinatorCheckpointContext coordContext : coordinatorsToCheckpoint) {
			final OperatorState state = operatorStates.get(coordContext.operatorId());
			if (state == null) {
				continue;
			}

			final ByteStreamStateHandle coordinatorState = state.getCoordinatorState();
			if (coordinatorState != null) {
				coordContext.resetToCheckpoint(coordinatorState.getData());
			}
		}
	}

	// ------------------------------------------------------------------------
	//  job status listener that schedules / cancels periodic checkpoints
	// ------------------------------------------------------------------------

	public JobStatusListener createActivatorDeactivator() {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (jobStatusListener == null) {
				jobStatusListener = new CheckpointCoordinatorDeActivator(this);
			}

			return jobStatusListener;
		}
	}

	int getNumQueuedRequests() {
		return requestDecider.getNumQueuedRequests();
	}

	// ------------------------------------------------------------------------

	/**
	 * Discards the given state object asynchronously belonging to the given job, execution attempt
	 * id and checkpoint id.
	 *
	 * @param jobId identifying the job to which the state object belongs
	 * @param executionAttemptID identifying the task to which the state object belongs
	 * @param checkpointId of the state object
	 * @param subtaskState to discard asynchronously
	 */
	private void discardSubtaskState(
			final JobID jobId,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final TaskStateSnapshot subtaskState) {

		if (subtaskState != null) {
			executor.execute(new Runnable() {
				@Override
				public void run() {

					try {
						subtaskState.discardState();
					} catch (Throwable t2) {
						LOG.warn("Could not properly discard state object of checkpoint {} " +
							"belonging to task {} of job {}.", checkpointId, executionAttemptID, jobId, t2);
					}
				}
			});
		}
	}

	private void abortPendingCheckpoint(
		PendingCheckpoint pendingCheckpoint,
		CheckpointException exception) {

		abortPendingCheckpoint(pendingCheckpoint, exception, null);
	}

	private void abortPendingCheckpoint(
		PendingCheckpoint pendingCheckpoint,
		CheckpointException exception,
		@Nullable final ExecutionAttemptID executionAttemptID) {

		assert(Thread.holdsLock(lock));

		if (!pendingCheckpoint.isDiscarded()) {
			try {
				// release resource here
				pendingCheckpoint.abort(
					exception.getCheckpointFailureReason(), exception.getCause());

				if (pendingCheckpoint.getProps().isSavepoint() &&
					pendingCheckpoint.getProps().isSynchronous()) {
					if (pendingCheckpoint.getProps().isResumeSourceIfFail()) {
						LOG.warn("Detach sync savepoint (savepoint with blocked source) {} which was triggered at {}, resume source",
							pendingCheckpoint.getCheckpointId(), new Date(pendingCheckpoint.getCheckpointTimestamp()), exception);
					} else {
						failureManager.handleSynchronousSavepointFailure(exception);
					}
				} else if (executionAttemptID != null) {
					failureManager.handleTaskLevelCheckpointException(
						exception, pendingCheckpoint.getCheckpointId(), executionAttemptID);
				} else {
					failureManager.handleJobLevelCheckpointException(
						exception, pendingCheckpoint.getCheckpointId());
				}
			} finally {
				sendAbortedMessages(pendingCheckpoint.getCheckpointId(), pendingCheckpoint.getCheckpointTimestamp());
				pendingCheckpoints.remove(pendingCheckpoint.getCheckpointId());
				pendingSavepointsUnsafe.remove(pendingCheckpoint.getCheckpointId());
				rememberRecentCheckpointId(pendingCheckpoint.getCheckpointId());
				timer.execute(this::executeQueuedRequest);
			}
		}
	}

	private void preCheckGlobalState(boolean isPeriodic) throws CheckpointException {
		// abort if the coordinator has been shutdown in the meantime
		if (shutdown) {
			throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
		}

		// Don't allow periodic checkpoint if scheduling has been disabled
		if (isPeriodic && !periodicScheduling) {
			throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
		}
	}

	private void abortPendingAndQueuedCheckpoints(CheckpointException exception) {
		assert(Thread.holdsLock(lock));
		requestDecider.abortAll(exception);
		abortPendingCheckpoints(exception);
	}

	/**
	 * The canceller of checkpoint. The checkpoint might be cancelled if it doesn't finish in a
	 * configured period.
	 */
	private class CheckpointCanceller implements Runnable {

		private final PendingCheckpoint pendingCheckpoint;

		private CheckpointCanceller(PendingCheckpoint pendingCheckpoint) {
			this.pendingCheckpoint = checkNotNull(pendingCheckpoint);
		}

		@Override
		public void run() {
			synchronized (lock) {
				// only do the work if the checkpoint is not discarded anyways
				// note that checkpoint completion discards the pending checkpoint object
				if (!pendingCheckpoint.isDiscarded()) {
					if (sendActionToHandler(handler -> handler.tryHandleExpireCheckpoint(pendingCheckpoint),
							Collections.singleton(pendingCheckpoint.getCheckpointId()))) {
						return;
					}

					LOG.info("Checkpoint {} of job {} expired before completing.",
						pendingCheckpoint.getCheckpointId(), job);

					abortPendingCheckpoint(
						pendingCheckpoint,
						new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED));
				}
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

	private void checkAndResetCheckpointScheduler() {
		if (!shutdown && periodicScheduling && !checkpointScheduler.isScheduling()) {
			synchronized (lock) {
				if (pendingCheckpoints.isEmpty() || allPendingCheckpointsDiscarded()) {
					checkpointScheduler.startScheduling();
				}
			}
		}
	}

	private boolean allPendingCheckpointsDiscarded() {
		return pendingCheckpoints.values().stream().allMatch(PendingCheckpoint::isDiscarded);
	}

	private static CheckpointException getCheckpointException(
		CheckpointFailureReason defaultReason, Throwable throwable) {

		final Optional<CheckpointException> checkpointExceptionOptional =
			findThrowable(throwable, CheckpointException.class);
		return checkpointExceptionOptional
			.orElseGet(() -> new CheckpointException(defaultReason, throwable));
	}

	private static class DetachSavepointProperties {
		private final boolean isDetachSavepoint;
		@Nullable
		private final String savepointId;

		public DetachSavepointProperties(@Nullable String savepointId) {
			this.isDetachSavepoint = (savepointId != null);
			this.savepointId = savepointId;
		}

		static DetachSavepointProperties nonDetachSavepointProperties() {
			return new DetachSavepointProperties(null);
		}
	}

	private static class CheckpointIdAndStorageLocation {
		private final long checkpointId;
		private final CheckpointStorageLocation checkpointStorageLocation;

		CheckpointIdAndStorageLocation(
			long checkpointId,
			CheckpointStorageLocation checkpointStorageLocation) {

			this.checkpointId = checkpointId;
			this.checkpointStorageLocation = checkNotNull(checkpointStorageLocation);
		}
	}

	static class CheckpointTriggerRequest {
		final long timestamp;
		final CheckpointProperties props;
		final @Nullable String externalSavepointLocation;
		final boolean isPeriodic;
		final boolean advanceToEndOfTime;
		final long savepointTimeout;
		final DetachSavepointProperties detachSavepointProperties;

		private final CompletableFuture<CompletedCheckpoint> onCompletionPromise = new CompletableFuture<>();

		CheckpointTriggerRequest(
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime) {
			this(props, externalSavepointLocation, isPeriodic, advanceToEndOfTime, -1L);
		}

		CheckpointTriggerRequest(
				CheckpointProperties props,
				@Nullable String externalSavepointLocation,
				boolean isPeriodic,
				boolean advanceToEndOfTime,
				long savepointTimeout) {

			this(props, externalSavepointLocation, isPeriodic, advanceToEndOfTime, savepointTimeout, DetachSavepointProperties.nonDetachSavepointProperties());
		}

		CheckpointTriggerRequest(
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime,
			long savepointTimeout,
			DetachSavepointProperties detachSavepointProperties) {

			this.timestamp = System.currentTimeMillis();
			this.props = checkNotNull(props);
			this.externalSavepointLocation = externalSavepointLocation;
			this.isPeriodic = isPeriodic;
			this.advanceToEndOfTime = advanceToEndOfTime;
			this.savepointTimeout = savepointTimeout;
			this.detachSavepointProperties = detachSavepointProperties;
		}

		CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
			return onCompletionPromise;
		}

		public void completeExceptionally(CheckpointException exception) {
			onCompletionPromise.completeExceptionally(exception);
		}

		public boolean isForce() {
			return props.forceCheckpoint();
		}

		long getSavepointTimeout() {
			return savepointTimeout;
		}
	}

	/**
	 * This should hold CheckpointCoordinator's lock because handler may modify the pending checkpoint.
	 */
	private boolean sendActionToHandler(
			Function<CheckpointHandler, Boolean> function,
			Collection<Long> checkpointIds) {
		assert Thread.holdsLock(lock);

		if (checkpointIds.size() == 0) {
			LOG.info("No need to let checkpoint handler do this because there is no pending checkpoints.");
			return false;
		}

		boolean ack;
		try {
			ack = function.apply(checkpointHandler);
		} catch (Throwable t) {
			LOG.error("CheckpointHandler fails to handle this action.", t);
			return false;
		}

		if (ack) {
			List<PendingCheckpoint> finalizedCheckpoints = checkpointIds.stream()
					.map(pendingCheckpoints::get)
					.filter(PendingCheckpoint::isFullyAcknowledged)
					.sorted((o1, o2) -> (int) (o1.getCheckpointId() - o2.getCheckpointId())).collect(Collectors.toList());
			for (PendingCheckpoint checkpoint : finalizedCheckpoints) {
				try {
					completePendingCheckpoint(checkpoint);
				} catch (CheckpointException t) {
					LOG.warn("Error while processing checkpoint acknowledgement message", t);
				}
			}
			LOG.info("CheckpointHandler handles this action as expected for checkpoints [{}].",
					checkpointIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
			return true;
		}

		return false;
	}

	/**
	 * Similar to {@link #sendActionToHandler(Function, Collection)} but this function will not modify any states
	 * such as PendingCheckpoints.
	 */
	private boolean sendNotificationToHandler(
			Function<CheckpointHandler, Boolean> function,
			Collection<Long> checkpointIds) {
		assert Thread.holdsLock(lock);

		boolean ack;
		try {
			ack = function.apply(checkpointHandler);
		} catch (Throwable t) {
			LOG.error("CheckpointHandler fails to handle this notification.", t);
			return false;
		}

		if (ack) {
			LOG.info("CheckpointHandler handles this notification as expected for checkpoints [{}].",
					checkpointIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
		}

		return ack;
	}
}
