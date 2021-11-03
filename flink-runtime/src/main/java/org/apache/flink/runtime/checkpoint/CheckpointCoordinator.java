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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.handler.CheckpointHandler;
import org.apache.flink.runtime.checkpoint.handler.GlobalCheckpointHandler;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.checkpoint.scheduler.CheckpointScheduler;
import org.apache.flink.runtime.checkpoint.trigger.CheckpointTasks;
import org.apache.flink.runtime.checkpoint.trigger.PendingTriggerFactory;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.CheckpointTaskIdentifier;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.InitializeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.PerformCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.scheduler.CheckpointSchedulerUtils.createCheckpointScheduler;
import static org.apache.flink.runtime.checkpoint.scheduler.CheckpointSchedulerUtils.setupSavepointScheduler;
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

	/** Lock specially to make sure that trigger requests do not overtake each other.
	 * This is not done with the coordinator-wide lock, because as part of triggering,
	 * blocking operations may happen (distributed atomic counters).
	 * Using a dedicated lock, we avoid blocking the processing of 'acknowledge/decline'
	 * messages during that phase. */
	private final Object triggerLock = new Object();

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
	private final ExecutionVertex[] tasksToCommitTo;

	/** Map from checkpoint ID to the pending checkpoint. */
	private final Map<Long, PendingCheckpoint> pendingCheckpoints;

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

	/** The maximum number of checkpoints that may be in progress at the same time. */
	private final int maxConcurrentCheckpointAttempts;

	private final CheckpointScheduler checkpointScheduler;

	/** The master checkpoint hooks executed by this checkpoint coordinator. */
	private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

	/** Actor that receives status updates from the execution graph this coordinator works for. */
	private JobStatusListener jobStatusListener;

	/** The number of consecutive failed trigger attempts. */
	private final AtomicInteger numUnsuccessfulCheckpointsTriggers = new AtomicInteger(0);

	/** The timestamp (via {@link System#nanoTime()}) when the last checkpoint completed. */
	private long lastCheckpointCompletionNanos;

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;

	/** Flag whether a trigger request could not be handled immediately. Non-volatile, because only
	 * accessed in synchronized scope */
	private boolean triggerRequestQueued;

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

	private final CheckpointHandler checkpointHandler;

	private final Set<CheckpointTaskIdentifier> initializations;

	private final boolean aggregateUnionState;

	/** Timestamp of start restoring checkpoints when job starts, only use once and set to -1*/
	private long startSchedulingTimestamp = -1;

	/** number of subtasks to restore checkpoint */
	private int numRestoreTasks;

	/** determine how to trigger the checkpoint. */
	private final PendingTriggerFactory pendingTriggerFactory;

	// --------------------------------------------------------------------------------------------

	@VisibleForTesting
	public CheckpointCoordinator(
		JobID job,
		CheckpointCoordinatorConfiguration chkConfig,
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo,
		CheckpointIDCounter checkpointIDCounter,
		CompletedCheckpointStore completedCheckpointStore,
		StateBackend checkpointStateBackend,
		Executor executor,
		SharedStateRegistryFactory sharedStateRegistryFactory,
		CheckpointFailureManager failureManager) {
		this(job,
			null,
			chkConfig,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			checkpointIDCounter,
			completedCheckpointStore,
			checkpointStateBackend,
			executor,
			sharedStateRegistryFactory,
			failureManager,
			new GlobalCheckpointHandler(),
			UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
	}

	public CheckpointCoordinator(
			JobID job,
			@Nullable String jobName,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager,
			CheckpointHandler checkpointHandler,
			MetricGroup metricGroup) {
		this(job,
			jobName,
			chkConfig,
			tasksToTrigger,
			tasksToWaitFor,
			tasksToCommitTo,
			checkpointIDCounter,
			completedCheckpointStore,
			checkpointStateBackend,
			executor,
			sharedStateRegistryFactory,
			failureManager,
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
			@Nullable String jobName,
			CheckpointCoordinatorConfiguration chkConfig,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			StateBackend checkpointStateBackend,
			Executor executor,
			SharedStateRegistryFactory sharedStateRegistryFactory,
			CheckpointFailureManager failureManager,
			CheckpointHandler checkpointHandler,
			MetricGroup metricGroup,
			PendingTriggerFactory pendingTriggerFactory) {

		// sanity checks
		checkNotNull(checkpointStateBackend);

		this.job = checkNotNull(job);
		this.checkpointTimeout = chkConfig.getCheckpointTimeout();
		this.maxConcurrentCheckpointAttempts = chkConfig.getMaxConcurrentCheckpoints();
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<>();
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.executor = checkNotNull(executor);
		this.sharedStateRegistryFactory = checkNotNull(sharedStateRegistryFactory);
		this.sharedStateRegistry = sharedStateRegistryFactory.create(executor);
		this.isPreferCheckpointForRecovery = chkConfig.isPreferCheckpointForRecovery();
		this.failureManager = checkNotNull(failureManager);
		this.aggregateUnionState = chkConfig.isAggregateUnionState();

		this.recentPendingCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
		this.masterHooks = new HashMap<>();

		this.checkpointScheduler = createCheckpointScheduler(job, this, chkConfig);
		setupSavepointScheduler(checkpointScheduler, jobName, this, chkConfig);

		this.checkpointHandler = checkpointHandler;
		checkpointHandler.loadPendingCheckpoints(pendingCheckpoints);

		this.initializations = new HashSet<>(tasksToWaitFor.length);

		this.checkpointProperties = CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy(), pendingTriggerFactory.isUseFastMode());

		this.pendingTriggerFactory = pendingTriggerFactory;

		try {
			this.checkpointStorage = checkpointStateBackend.createCheckpointStorage(job, jobName, metricGroup);
			if (isPeriodicCheckpointingConfigured()) {
				// do not create checkpoint directory if checkpoint is disabled
				checkpointStorage.initializeLocation();
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

		numRestoreTasks = tasksToWaitFor.length;
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
				triggerRequestQueued = false;

				// shut down the thread that handles the timeouts and pending triggers
				checkpointScheduler.shutdownNow();

				// shut down the hooks
				MasterHooks.close(masterHooks.values(), LOG);
				masterHooks.clear();

				// clear and discard all pending checkpoints
				for (PendingCheckpoint pending : pendingCheckpoints.values()) {
					failPendingCheckpoint(pending, CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
				}
				pendingCheckpoints.clear();

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
	 * @param timestamp The timestamp for the savepoint.
	 * @param targetLocation Target location for the savepoint, optional. If null, the
	 *                       state backend's configured default will be used.
	 * @return A future to the completed checkpoint
	 * @throws IllegalStateException If no savepoint directory has been
	 *                               specified and no default savepoint directory has been
	 *                               configured
	 */
	public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
			final long timestamp,
			@Nullable final String targetLocation) {

		final CheckpointProperties properties = CheckpointProperties.forSavepoint();
		return triggerSavepointInternal(timestamp, properties, false, targetLocation);
	}

	/**
	 * Triggers a synchronous savepoint with the given savepoint directory as a target.
	 *
	 * @param timestamp The timestamp for the savepoint.
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
			final long timestamp,
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation) {

		final CheckpointProperties properties = CheckpointProperties.forSyncSavepoint();

		return triggerSavepointInternal(timestamp, properties, advanceToEndOfEventTime, targetLocation).handle(
				(completedCheckpoint, throwable) -> {
					if (throwable != null) {
						failureManager.handleSynchronousSavepointFailure(throwable);
						throw new CompletionException(throwable);
					}
					return completedCheckpoint;
				});
	}

	private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
			final long timestamp,
			final CheckpointProperties checkpointProperties,
			final boolean advanceToEndOfEventTime,
			@Nullable final String targetLocation) {

		checkNotNull(checkpointProperties);

		try {
			PendingCheckpoint pendingCheckpoint = triggerCheckpoint(
					timestamp,
					checkpointProperties,
					targetLocation,
					false,
					advanceToEndOfEventTime);

			return pendingCheckpoint.getCompletionFuture();
		} catch (CheckpointException e) {
			Throwable cause = new CheckpointException("Failed to trigger savepoint.", e.getCheckpointFailureReason());
			return FutureUtils.completedExceptionally(cause);
		}
	}

	/**
	 * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 * @param isPeriodic Flag indicating whether this triggered checkpoint is
	 * periodic. If this flag is true, but the periodic scheduler is disabled,
	 * the checkpoint will be declined.
	 * @return <code>true</code> if triggering the checkpoint succeeded.
	 */
	public boolean triggerCheckpoint(long timestamp, boolean isPeriodic) {
		try {
			triggerCheckpoint(timestamp, checkpointProperties, null, isPeriodic, false);
			return true;
		} catch (CheckpointException e) {
			long latestGeneratedCheckpointId = getCheckpointIdCounter().get();
			// here we can not get the failed pending checkpoint's id,
			// so we pass the negative latest generated checkpoint id as a special flag
			failureManager.handleJobLevelCheckpointException(e, -1 * latestGeneratedCheckpointId);
			return false;
		}
	}

	@VisibleForTesting
	public PendingCheckpoint triggerCheckpoint(
			long timestamp,
			CheckpointProperties props,
			@Nullable String externalSavepointLocation,
			boolean isPeriodic,
			boolean advanceToEndOfTime) throws CheckpointException {

		if (advanceToEndOfTime && !(props.isSynchronous() && props.isSavepoint())) {
			throw new IllegalArgumentException("Only synchronous savepoints are allowed to advance the watermark to MAX.");
		}

		// make some eager pre-checks
		synchronized (lock) {
			// abort if the coordinator has been shutdown in the meantime
			if (shutdown) {
				throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
			}

			// Don't allow periodic checkpoint if scheduling has been disabled
			if (isPeriodic && !periodicScheduling) {
				throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
			}

			// validate whether the checkpoint can be triggered, with respect to the limit of
			// concurrent checkpoints, and the minimum time between checkpoints.
			// these checks are not relevant for savepoints
			if (!props.forceCheckpoint()) {
				// sanity check: there should never be more than one trigger request queued
				if (triggerRequestQueued) {
					LOG.warn("Trying to trigger another checkpoint for job {} while one was queued already.", job);
					throw new CheckpointException(CheckpointFailureReason.ALREADY_QUEUED);
				}

				checkConcurrentCheckpoints();

				checkMinPauseBetweenCheckpoints();
			}
		}

		PendingTriggerFactory.PendingTrigger pendingTrigger = props.isSavepoint() ?
			pendingTriggerFactory.prepareTriggerSavepoint() : pendingTriggerFactory.prepareTriggerCheckpoint();

		// check if all tasks finish the initialization of the states
		if (initializations.size() < tasksToWaitFor.length) {
			LOG.info("Only {} tasks finish the initialization of state. Aborting checkpoint.", initializations.size());
			throw new CheckpointException(CheckpointFailureReason.NOT_ALL_TASKS_INITIALIZED);
		}

		// we will actually trigger this checkpoint!

		// we lock with a special lock to make sure that trigger requests do not overtake each other.
		// this is not done with the coordinator-wide lock, because the 'checkpointIdCounter'
		// may issue blocking operations. Using a different lock than the coordinator-wide lock,
		// we avoid blocking the processing of 'acknowledge/decline' messages during that time.
		synchronized (triggerLock) {

			if (statsTracker != null) {
				statsTracker.reportTriggerCheckpoint();
			}

			final CheckpointStorageLocation checkpointStorageLocation;
			final long checkpointID;

			try {
				// this must happen outside the coordinator-wide lock, because it communicates
				// with external services (in HA mode) and may block for a while.
				checkpointID = checkpointIdCounter.getAndIncrement();

				checkpointStorageLocation = props.isSavepoint() ?
						checkpointStorage.initializeLocationForSavepoint(checkpointID, externalSavepointLocation) :
						checkpointStorage.initializeLocationForCheckpoint(checkpointID);
			}
			catch (Throwable t) {
				if (statsTracker != null) {
					statsTracker.reportTriggerFailedCheckpoint();
				}
				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint for job {} ({} consecutive failed attempts so far).",
						job,
						numUnsuccessful,
						t);
				throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
			}

			final PendingCheckpoint checkpoint = new PendingCheckpoint(
				job,
				checkpointID,
				timestamp,
				pendingTrigger.getAckTasks(),
				props,
				checkpointStorageLocation,
				executor,
				pendingTrigger,
				checkpointStorage);

			if (statsTracker != null) {
				PendingCheckpointStats callback = statsTracker.reportPendingCheckpoint(
					checkpointID,
					timestamp,
					props,
					pendingTrigger.getAckTasks().size());

				checkpoint.setStatsCallback(callback);
			}

			// schedule the timer that will clean up the expired checkpoints
			final Runnable canceller = () -> {
				synchronized (lock) {
					// only do the work if the checkpoint is not discarded anyways
					// note that checkpoint completion discards the pending checkpoint object
					if (!checkpoint.isDiscarded()) {
						if (sendActionToHandler(handler -> handler.tryHandleExpireCheckpoint(checkpoint),
								Collections.singleton(checkpoint.getCheckpointId()))) {
							return;
						}

						LOG.info("Checkpoint {} of job {} expired before completing.", checkpointID, job);

						failPendingCheckpoint(checkpoint, CheckpointFailureReason.CHECKPOINT_EXPIRED);
						pendingCheckpoints.remove(checkpointID);
						rememberRecentCheckpointId(checkpointID);

						triggerQueuedRequests();
					}
				}
			};

			try {
				// re-acquire the coordinator-wide lock
				synchronized (lock) {
					// since we released the lock in the meantime, we need to re-check
					// that the conditions still hold.
					if (shutdown) {
						throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
					}
					else if (!props.forceCheckpoint()) {
						if (triggerRequestQueued) {
							LOG.warn("Trying to trigger another checkpoint for job {} while one was queued already.", job);
							throw new CheckpointException(CheckpointFailureReason.ALREADY_QUEUED);
						}

						checkConcurrentCheckpoints();

						checkMinPauseBetweenCheckpoints();
					}

					LOG.info("Triggering checkpoint {} @ {} for job {}.", checkpointID, timestamp, job);

					pendingCheckpoints.put(checkpointID, checkpoint);

					if (!advanceToEndOfTime) {
						// schedule the timer that will clean up the expired checkpoints
						ScheduledFuture<?> cancellerHandle = checkpointScheduler.scheduleTimeoutCanceller(canceller);

						if (!checkpoint.setCancellerHandle(cancellerHandle)) {
							// checkpoint is already disposed!
							cancellerHandle.cancel(false);
						}
					} else {
						LOG.info("Do not expire the checkpoint {} @ {} for job {} because of stop-with-savepoint.", checkpointID, timestamp, job);
					}

					// trigger the master hooks for the checkpoint
					final List<MasterState> masterStates = MasterHooks.triggerMasterHooks(masterHooks.values(),
							checkpointID, timestamp, executor, Time.milliseconds(checkpointTimeout));
					for (MasterState s : masterStates) {
						checkpoint.addMasterState(s);
					}
				}
				// end of lock scope

				final CheckpointOptions checkpointOptions = new CheckpointOptions(
						props.getCheckpointType(),
						checkpointStorageLocation.getLocationReference());
				pendingTrigger.setCheckpointOptions(checkpointOptions);

				// send the messages to the tasks that trigger their checkpoint
				for (Execution execution: pendingTrigger.getNextTriggerTasks()) {
					if (props.isSynchronous()) {
						execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
					} else {
						execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
					}
				}

				LOG.info("Successfully trigger checkpoint {} @ {} for job {}.", checkpointID, timestamp, job);
				numUnsuccessfulCheckpointsTriggers.set(0);
				return checkpoint;
			}
			catch (Throwable t) {
				// guard the map against concurrent modifications
				synchronized (lock) {
					pendingCheckpoints.remove(checkpointID);
				}

				if (statsTracker != null) {
					statsTracker.reportTriggerFailedCheckpoint();
				}
				int numUnsuccessful = numUnsuccessfulCheckpointsTriggers.incrementAndGet();
				LOG.warn("Failed to trigger checkpoint {} for job {}. ({} consecutive failed attempts so far)",
						checkpointID, job, numUnsuccessful, t);

				if (!checkpoint.isDiscarded()) {
					failPendingCheckpoint(checkpoint, CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, t);
				}

				try {
					checkpointStorageLocation.disposeOnFailure();
				}
				catch (Throwable t2) {
					LOG.warn("Cannot dispose failed checkpoint storage location {}", checkpointStorageLocation, t2);
				}

				throw new CheckpointException(CheckpointFailureReason.EXCEPTION, t);
			}

		} // end trigger lock
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	public void receiveInitializationMessage(InitializeCheckpoint message) {
		if (shutdown || message == null) {
			return;
		}

		if (!job.equals(message.getJob())) {
			throw new IllegalArgumentException("Received InitializeCheckpoint message for job " +
					message.getJob() + " while this coordinator handles job " + job);
		}

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return;
			}

			initializations.add(message.getIdentifier());
			if (initializations.size() == numRestoreTasks) {
				statsTracker.setJobStartupCheckpointRestoreDelay(System.currentTimeMillis() - startSchedulingTimestamp);
			}
		}
	}

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

			pendingCheckpoints.remove(checkpointId);

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				LOG.info("Decline checkpoint {} by task {} of job {} at {}.",
					checkpointId,
					message.getTaskExecutionId(),
					job,
					taskManagerLocationInfo);
				discardCheckpoint(checkpoint, message.getReason(), message.getTaskExecutionId());
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else if (LOG.isDebugEnabled()) {
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

						if (checkpoint.isFullyAcknowledged()) {
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

		LOG.info("All acknowledgements of checkpoint {} received. Metadata persistence started.", checkpointId);

		// As a first step to complete the checkpoint, we register its state with the registry
		Map<OperatorID, OperatorState> operatorStates = pendingCheckpoint.getOperatorStates();
		sharedStateRegistry.registerAll(operatorStates.values());

		LOG.info("Metadata persistence of checkpoint {}: shared state metadata registered.", checkpointId);

		try {
			try {
				completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();
				failureManager.handleCheckpointSuccess(pendingCheckpoint.getCheckpointId());
				LOG.info("Metadata persistence of checkpoint {}: checkpoint metadata registered.", checkpointId);
			}
			catch (Exception e1) {
				// abort the current pending checkpoint if we fails to finalize the pending checkpoint.
				if (!pendingCheckpoint.isDiscarded()) {
					failPendingCheckpoint(pendingCheckpoint, CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, e1);
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

				throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.',
					CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE, exception);
			}

			checkpointHandler.onCheckpointComplete(completedCheckpoint);
		} finally {
			pendingCheckpoints.remove(checkpointId);

			triggerQueuedRequests();
		}

		rememberRecentCheckpointId(checkpointId);

		// drop those pending checkpoints that are at prior to the completed one
		dropSubsumedCheckpoints(checkpointId);
		LOG.info("Metadata persistence of checkpoint {}: outdated pending checkpoints' metadata dropped.", checkpointId);

		// record the time when this was completed, to calculate
		// the 'min delay between checkpoints'
		lastCheckpointCompletionNanos = System.nanoTime();

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

		// send the "notify complete" call to all vertices
		final long timestamp = completedCheckpoint.getTimestamp();

		for (ExecutionVertex ev : pendingCheckpoint.getPendingTrigger().getCommitToTasks()) {
			Execution ee = ev.getMainExecution();

			if (ee != null) {
				// ignore if this vertex doesn't finish its own checkpoint
				if (!sendNotificationToHandler(handler -> handler.tryHandleCompletedNotification(ee.getVertex(), checkpointId),
						Collections.singleton(checkpointId))) {
					ee.notifyCheckpointComplete(checkpointId, timestamp);
				}
			}
		}
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

			Iterator<PendingCheckpoint> pendingCheckpointIterator = pendingCheckpoints.values().iterator();

			while (pendingCheckpointIterator.hasNext()) {
				final PendingCheckpoint pendingCheckpoint = pendingCheckpointIterator.next();

				if (!pendingCheckpoint.isAcknowledgedBy(executionAttemptId)) {
					pendingCheckpointIterator.remove();
					discardCheckpoint(pendingCheckpoint, cause, executionAttemptId);
				}
			}
		}
	}

	private void rememberRecentCheckpointId(long id) {
		if (recentPendingCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
			recentPendingCheckpoints.removeFirst();
		}
		recentPendingCheckpoints.addLast(id);
	}

	private void dropSubsumedCheckpoints(long checkpointId) {
		Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();

		while (entries.hasNext()) {
			PendingCheckpoint p = entries.next().getValue();
			// remove all pending checkpoints that are lesser than the current completed checkpoint
			if (p.getCheckpointId() < checkpointId && p.canBeSubsumed()) {
				rememberRecentCheckpointId(p.getCheckpointId());
				failPendingCheckpoint(p, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
				entries.remove();
			}
		}
	}

	/**
	 * Triggers the queued request, if there is one.
	 *
	 * <p>NOTE: The caller of this method must hold the lock when invoking the method!
	 */
	private void triggerQueuedRequests() {
		if (triggerRequestQueued) {
			triggerRequestQueued = false;

			// trigger the checkpoint from the trigger timer, to finish the work of this thread before
			// starting with the next checkpoint
			if (periodicScheduling) {
				checkpointScheduler.pauseScheduling(); // just in case, should be unnecessary
				checkpointScheduler.resumeScheduling();
			}
			else {
				checkpointScheduler.triggerOnce();
			}
		}
	}

	@VisibleForTesting
	int getNumScheduledTasks() {
		return checkpointScheduler.getNumberOfScheduledTasks();
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpoint State Restoring
	// --------------------------------------------------------------------------------------------

	public boolean restoreLatestCheckpointedState(
		Map<JobVertexID, ExecutionJobVertex> tasks,
		boolean errorIfNoCheckpoint,
		boolean allowNonRestoredState) throws Exception {

		return restoreLatestCheckpointedState(
			tasks,
			errorIfNoCheckpoint,
			allowNonRestoredState,
			false,
			false,
			null);
	}

	/**
	 * Restores the latest checkpointed state.
	 *
	 * @param tasks Map of job vertices to restore. State for these vertices is
	 * restored via {@link Execution#setInitialState(JobManagerTaskRestore)}.
	 * @param errorIfNoCheckpoint Fail if no completed checkpoint is available to
	 * restore from.
	 * @param allowNonRestoredState Allow checkpoint state that cannot be mapped
	 * to any job vertex in tasks.
	 * @param findCheckpointInCheckpointStorage whether find checkpoint in checkpointStore.
	 * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint versions.
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
	public boolean restoreLatestCheckpointedState(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean errorIfNoCheckpoint,
			boolean allowNonRestoredState,
			boolean findCheckpointInCheckpointStorage,
			boolean crossVersion,
			@Nullable ClassLoader userClassLoader) throws Exception {

		synchronized (lock) {
			if (shutdown) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			// Recover the checkpoints, TODO this could be done only when there is a new leader, not on each recovery
			// do not need to recover the checkpoint store on failover
			// if findCheckpointInCheckpointStorage equals to true, it means the job just starts and needs to load
			// checkpoint from HDFS
			List<CompletedCheckpoint> oriCompletedCheckpoints = completedCheckpointStore.getAllCheckpoints();
			if (findCheckpointInCheckpointStorage) {
				if (!crossVersion) {
					completedCheckpointStore.recover();
				} else {
					// clear all checkpoints and restore from hdfs
					completedCheckpointStore.clearAllCheckpoints();
				}
			}

			final Set<CompletedCheckpoint> checkpointsOnStorage = findAllCompletedCheckpointsOnStorage(
					tasks, allowNonRestoredState, findCheckpointInCheckpointStorage, crossVersion, userClassLoader);
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
			CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint(isPreferCheckpointForRecovery);
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

			LOG.info("Restoring job {} from latest valid checkpoint: {}.", job, latest);

			// re-assign the task states
			final Map<OperatorID, OperatorState> operatorStates = latest.getOperatorStates();

			UnionStateAggregator unionStateAggregator = aggregateUnionState ?
				new FileUnionStateAggregator(latest.getExternalPointer()) : new NonUnionStateAggregator();
			StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState, unionStateAggregator);

			stateAssignmentOperation.assignStates();

			// call master hooks for restore

			MasterHooks.restoreMasterHooks(
					masterHooks,
					latest.getMasterHookStates(),
					latest.getCheckpointID(),
					allowNonRestoredState,
					LOG);

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
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean allowNonRestoredState,
			boolean findCheckpointInCheckpointStore,
			boolean crossVersion,
			@Nullable ClassLoader userClassLoader) throws IOException {
		final Set<CompletedCheckpoint> result = new HashSet<>();

		LOG.info("Looking for completed checkpoints on HDFS (cross-version: {})", crossVersion);
		int onRetrievingCheckpointsIdx = 0;
		if (findCheckpointInCheckpointStore && userClassLoader != null) {
			// tuple.f0: external pointer, tuple.f1: if completeCheckpoint is savepoint?
			List<Tuple2<String, Boolean>> completedCheckpointPointersOnStorage;
			if (!crossVersion) {
				completedCheckpointPointersOnStorage = checkpointStorage.findCompletedCheckpointPointerV2();
			} else {
				completedCheckpointPointersOnStorage = checkpointStorage.findCompletedCheckpointPointerForCrossVersion();
			}

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
							job, tasks, checkpointStorageLocation, userClassLoader, allowNonRestoredState, completedCheckpointPointer.f1);
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

		return restoreLatestCheckpointedState(tasks, true, allowNonRestored);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public int getNumberOfPendingCheckpoints() {
		return this.pendingCheckpoints.size();
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
			startSchedulingTimestamp = System.currentTimeMillis();

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
			triggerRequestQueued = false;
			periodicScheduling = false;

			checkpointScheduler.stopScheduling();

			abortPendingCheckpoints(new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND));

			numUnsuccessfulCheckpointsTriggers.set(0);
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
			for (PendingCheckpoint p : pendingCheckpoints.values()) {
				failPendingCheckpoint(p, exception.getCheckpointFailureReason());
			}

			pendingCheckpoints.clear();
		}
	}

	/**
	 * If too many checkpoints are currently in progress, we need to mark that a request is queued.
	 *
	 * @throws CheckpointException If too many checkpoints are currently in progress.
	 */
	private void checkConcurrentCheckpoints() throws CheckpointException {
		if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
			triggerRequestQueued = true;
			checkpointScheduler.pauseScheduling();
			throw new CheckpointException(CheckpointFailureReason.TOO_MANY_CONCURRENT_CHECKPOINTS);
		}
	}

	/**
	 * Make sure the minimum interval between checkpoints has passed.
	 *
	 * @throws CheckpointException If the minimum interval between checkpoints has not passed.
	 */
	private void checkMinPauseBetweenCheckpoints() throws CheckpointException {
		checkpointScheduler.checkMinPauseSinceLastCheckpoint(lastCheckpointCompletionNanos);
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

	// ------------------------------------------------------------------------

	/**
	 * Discards the given pending checkpoint because of the given cause.
	 *
	 * @param pendingCheckpoint to discard
	 * @param cause for discarding the checkpoint
	 * @param executionAttemptID the execution attempt id of the failing task.
	 */
	private void discardCheckpoint(
		PendingCheckpoint pendingCheckpoint,
		@Nullable Throwable cause,
		ExecutionAttemptID executionAttemptID) {
		assert(Thread.holdsLock(lock));
		Preconditions.checkNotNull(pendingCheckpoint);

		final long checkpointId = pendingCheckpoint.getCheckpointId();

		LOG.info("Discarding checkpoint {} of job {}.", checkpointId, job, cause);

		if (cause == null) {
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, CheckpointFailureReason.CHECKPOINT_DECLINED, executionAttemptID);
		} else if (cause instanceof CheckpointException) {
			CheckpointException exception = (CheckpointException) cause;
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, exception.getCheckpointFailureReason(), cause, executionAttemptID);
		} else {
			failPendingCheckpointDueToTaskFailure(pendingCheckpoint, CheckpointFailureReason.JOB_FAILURE, cause, executionAttemptID);
		}

		rememberRecentCheckpointId(checkpointId);

		// we don't have to schedule another "dissolving" checkpoint any more because the
		// cancellation barriers take care of breaking downstream alignments
		// we only need to make sure that suspended queued requests are resumed

		boolean haveMoreRecentPending = false;
		for (PendingCheckpoint p : pendingCheckpoints.values()) {
			if (!p.isDiscarded() && p.getCheckpointId() >= pendingCheckpoint.getCheckpointId()) {
				haveMoreRecentPending = true;
				break;
			}
		}

		if (!haveMoreRecentPending) {
			triggerQueuedRequests();
		}
	}

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

	private void failPendingCheckpoint(
			final PendingCheckpoint pendingCheckpoint,
			final CheckpointFailureReason reason) {

		failPendingCheckpoint(pendingCheckpoint, reason, null);
	}

	private void failPendingCheckpoint(
		final PendingCheckpoint pendingCheckpoint,
		final CheckpointFailureReason reason,
		@Nullable final Throwable cause) {

		CheckpointException exception = new CheckpointException(reason, cause);
		pendingCheckpoint.abort(reason, cause);
		failureManager.handleJobLevelCheckpointException(exception, pendingCheckpoint.getCheckpointId());

		checkAndResetCheckpointScheduler();
	}

	private void failPendingCheckpointDueToTaskFailure(
		final PendingCheckpoint pendingCheckpoint,
		final CheckpointFailureReason reason,
		final ExecutionAttemptID executionAttemptID) {

		failPendingCheckpointDueToTaskFailure(pendingCheckpoint, reason, null, executionAttemptID);
	}

	private void failPendingCheckpointDueToTaskFailure(
			final PendingCheckpoint pendingCheckpoint,
			final CheckpointFailureReason reason,
			@Nullable final Throwable cause,
			final ExecutionAttemptID executionAttemptID) {

		CheckpointException exception = wrapWithCheckpointException(reason, cause);
		pendingCheckpoint.abort(reason, cause);
		failureManager.handleTaskLevelCheckpointException(exception, pendingCheckpoint.getCheckpointId(), executionAttemptID);

		checkAndResetCheckpointScheduler();
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
					triggerRequestQueued = false;
					checkpointScheduler.startScheduling();
				}
			}
		}
	}

	private boolean allPendingCheckpointsDiscarded() {
		return pendingCheckpoints.values().stream().allMatch(PendingCheckpoint::isDiscarded);
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
