/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.event.CompoundRecorder;
import org.apache.flink.event.WarehouseJobStartEventMessageRecorder;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.NoOpFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyResolving;
import org.apache.flink.runtime.executiongraph.speculation.SpeculationStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.PerformCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class which can be used to implement {@link SchedulerNG}.
 */
public abstract class SchedulerBase implements SchedulerNG {

	private final Logger log;

	private final JobGraph jobGraph;

	protected final ExecutionGraph executionGraph;

	private final SchedulingTopology schedulingTopology;

	private final InputsLocationsRetriever inputsLocationsRetriever;

	private final StateLocationRetriever stateLocationRetriever;

	private final BackPressureStatsTracker backPressureStatsTracker;

	private final Executor ioExecutor;

	private final Configuration jobMasterConfiguration;

	private final SlotProvider slotProvider;

	private final ScheduledExecutorService futureExecutor;

	private final ClassLoader userCodeLoader;

	private final CheckpointRecoveryFactory checkpointRecoveryFactory;

	private final Time rpcTimeout;

	private final RestartStrategy restartStrategy;

	private final BlobWriter blobWriter;

	private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

	private final Time slotRequestTimeout;

	private final boolean legacyScheduling;

	protected final ExecutionVertexVersioner executionVertexVersioner;

	private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

	private ComponentMainThreadExecutor mainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
		"SchedulerBase is not initialized with proper main thread executor. " +
			"Call to SchedulerBase.setMainThreadExecutor(...) required.");

	/** The implementation that decides how to speculate the slow tasks. */
	private final SpeculationStrategy speculationStrategy;

	protected final RemoteBlacklistReporter remoteBlacklistReporter;

	protected ResourceManagerGateway resourceManagerGateway = null;

	protected final long executionCancellationTimeout;
	protected final boolean executionCancellationTimeoutEnable;

	private final boolean allowNonRestoredState;

	protected final WarehouseJobStartEventMessageRecorder warehouseJobStartEventMessageRecorder;

	protected final CompoundRecorder compoundRecorder;

	// warehouse messages
	public static final String EVENT_METRIC_NAME = "executionGraphEvent";

	private static final String METRIC_FALLBACK_TO_RESTART_NAME = "fallbackToRestart";


	private final String savepointLocationPrefix;

	public SchedulerBase(
			final Logger log,
			final JobGraph jobGraph,
			final BackPressureStatsTracker backPressureStatsTracker,
			final Executor ioExecutor,
			final Configuration jobMasterConfiguration,
			final SlotProvider slotProvider,
			final ScheduledExecutorService futureExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final RestartStrategyFactory restartStrategyFactory,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final Time slotRequestTimeout,
			final ShuffleMaster<?> shuffleMaster,
			final JobMasterPartitionTracker partitionTracker,
			final ExecutionVertexVersioner executionVertexVersioner,
			final boolean legacyScheduling,
			final SpeculationStrategy.Factory speculationStrategyFactory,
			final RemoteBlacklistReporter remoteBlacklistReporter) throws Exception {

		this.log = checkNotNull(log);
		this.jobGraph = checkNotNull(jobGraph);
		this.backPressureStatsTracker = checkNotNull(backPressureStatsTracker);
		this.ioExecutor = checkNotNull(ioExecutor);
		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.slotProvider = checkNotNull(slotProvider);
		this.futureExecutor = checkNotNull(futureExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.checkpointRecoveryFactory = checkNotNull(checkpointRecoveryFactory);
		this.rpcTimeout = checkNotNull(rpcTimeout);
		this.warehouseJobStartEventMessageRecorder = new WarehouseJobStartEventMessageRecorder(null, jobGraph.getJobID().toString(), false);
		this.compoundRecorder = new CompoundRecorder(this.warehouseJobStartEventMessageRecorder);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
			jobGraph.getSerializedExecutionConfig()
				.deserializeValue(userCodeLoader)
				.getRestartStrategy();

		this.restartStrategy = RestartStrategyResolving.resolve(restartStrategyConfiguration,
			restartStrategyFactory,
			jobGraph.isCheckpointingEnabled());

		if (legacyScheduling) {
			log.info("Using restart strategy {} for {} ({}).", this.restartStrategy, jobGraph.getName(), jobGraph.getJobID());
		}

		this.blobWriter = checkNotNull(blobWriter);
		this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
		this.slotRequestTimeout = checkNotNull(slotRequestTimeout);
		this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
		this.legacyScheduling = legacyScheduling;

		this.speculationStrategy = checkNotNull(speculationStrategyFactory.create(this), "null speculation strategy");

		this.remoteBlacklistReporter = remoteBlacklistReporter;

		this.allowNonRestoredState = jobMasterConfiguration.getBoolean(CheckpointingOptions.ALLOW_NON_RESTORED_STATE);
		this.savepointLocationPrefix = jobMasterConfiguration.getString(CheckpointingOptions.SAVEPOINT_LOCATION_PREFIX);
		this.executionCancellationTimeout = jobMasterConfiguration.getLong(ExecutionOptions.EXECUTION_CANCELLATION_TIMEOUT);
		this.executionCancellationTimeoutEnable = jobMasterConfiguration.getBoolean(ExecutionOptions.EXECUTION_CANCELLATION_TIMEOUT_ENABLE);

		this.executionGraph = createAndRestoreExecutionGraph(jobManagerJobMetricGroup, checkNotNull(shuffleMaster), checkNotNull(partitionTracker));
		this.schedulingTopology = executionGraph.getSchedulingTopology();

		this.inputsLocationsRetriever = new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);
		this.stateLocationRetriever = executionVertexId -> getExecutionVertex(executionVertexId).getPreferredLocationBasedOnState();

		this.coordinatorMap = createCoordinatorMap();
	}

	private ExecutionGraph createAndRestoreExecutionGraph(
		JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker) throws Exception {

		ExecutionGraph newExecutionGraph = createExecutionGraph(currentJobManagerJobMetricGroup, shuffleMaster, partitionTracker);

		final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator != null) {
			// check whether we find a valid checkpoint
			if (!checkpointCoordinator.restoreLatestCheckpointedStateToAll(
				new HashSet<>(newExecutionGraph.getAllVertices().values()),
					allowNonRestoredState,
					userCodeLoader)) {

				// check whether we can restore from a savepoint
				tryRestoreExecutionGraphFromSavepoint(newExecutionGraph, jobGraph.getSavepointRestoreSettings());
			}
		}

		return newExecutionGraph;
	}

	private ExecutionGraph createExecutionGraph(
		JobManagerJobMetricGroup currentJobManagerJobMetricGroup,
		ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

		final FailoverStrategy.Factory failoverStrategy = legacyScheduling ?
			FailoverStrategyLoader.loadFailoverStrategy(jobMasterConfiguration, log) :
			new NoOpFailoverStrategy.Factory();

		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration,
			futureExecutor,
			ioExecutor,
			slotProvider,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			restartStrategy,
			currentJobManagerJobMetricGroup,
			blobWriter,
			slotRequestTimeout,
			log,
			shuffleMaster,
			partitionTracker,
			failoverStrategy,
			speculationStrategy,
			remoteBlacklistReporter,
			compoundRecorder);
	}

	/**
	 * Tries to restore the given {@link ExecutionGraph} from the provided {@link SavepointRestoreSettings}.
	 *
	 * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
	 * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about the savepoint to restore from
	 * @throws Exception if the {@link ExecutionGraph} could not be restored
	 */
	private void tryRestoreExecutionGraphFromSavepoint(ExecutionGraph executionGraphToRestore, SavepointRestoreSettings savepointRestoreSettings) throws Exception {
		if (savepointRestoreSettings.restoreSavepoint()) {
			final CheckpointCoordinator checkpointCoordinator = executionGraphToRestore.getCheckpointCoordinator();
			if (checkpointCoordinator != null) {
				checkpointCoordinator.restoreSavepoint(
					savepointRestoreSettings.getRestorePath(),
					savepointRestoreSettings.allowNonRestoredState(),
					executionGraphToRestore.getAllVertices(),
					userCodeLoader);
			}
		}
	}

	protected void resetForNewExecutions(final Collection<ExecutionVertexID> vertices) {
		final Set<CoLocationGroup> colGroups = new HashSet<>();
		vertices.forEach(executionVertexId -> {
			final ExecutionVertex ev = getExecutionVertex(executionVertexId);

			final CoLocationGroup cgroup = ev.getJobVertex().getCoLocationGroup();
			if (cgroup != null && !colGroups.contains(cgroup)){
				cgroup.resetConstraints();
				colGroups.add(cgroup);
			}

			ev.resetForNewExecution();
		});

	}

	protected void restoreState(final Set<ExecutionVertexID> vertices, final boolean isGlobalRecovery) throws Exception {
		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			return;
		}

		// if there is checkpointed state, reload it into the executions

		// abort pending checkpoints to
		// i) enable new checkpoint triggering without waiting for last checkpoint expired.
		// ii) ensure the EXACTLY_ONCE semantics if needed.
		checkpointCoordinator.onTaskFailure(vertices.stream().map(this::getExecutionVertex).collect(Collectors.toList()),
				new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

		final Set<ExecutionJobVertex> jobVerticesToRestore = getInvolvedExecutionJobVertices(vertices);
		if (isGlobalRecovery) {
			checkpointCoordinator.restoreLatestCheckpointedStateToAll(jobVerticesToRestore, true);
		} else {
			checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(jobVerticesToRestore);
		}
	}

	private Set<ExecutionJobVertex> getInvolvedExecutionJobVertices(
			final Set<ExecutionVertexID> executionVertices) {

		final Set<ExecutionJobVertex> tasks = new HashSet<>();
		for (ExecutionVertexID executionVertexID : executionVertices) {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
			tasks.add(executionVertex.getJobVertex());
		}
		return tasks;
	}

	protected void transitionToScheduled(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		executionVertexDeploymentOptions.forEach(deploymentOption -> {
			ExecutionVertex vertex = getExecutionVertex(deploymentOption.getExecutionVertexId());
			vertex.getExecution(deploymentOption.getDeploymentOption().isDeployCopy()).transitionState(ExecutionState.SCHEDULED);
		});
	}

	protected void setGlobalFailureCause(@Nullable final Throwable cause) {
		if (cause != null) {
			executionGraph.initFailureCause(cause);
		}
	}

	protected ComponentMainThreadExecutor getMainThreadExecutor() {
		return mainThreadExecutor;
	}

	protected void failJob(Throwable cause) {
		incrementVersionsOfAllVertices();
		executionGraph.failJob(cause);
	}

	protected final SchedulingTopology getSchedulingTopology() {
		return schedulingTopology;
	}

	protected final ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
		return executionGraph.getResultPartitionAvailabilityChecker();
	}

	protected final InputsLocationsRetriever getInputsLocationsRetriever() {
		return inputsLocationsRetriever;
	}

	protected final StateLocationRetriever getStateLocationRetriever() {
		return stateLocationRetriever;
	}

	protected final void prepareExecutionGraphForNgScheduling() {
		executionGraph.enableNgScheduling(new UpdateSchedulerNgOnInternalFailuresListener(this, jobGraph.getJobID()));
		executionGraph.transitionToRunning();
	}

	protected Optional<ExecutionVertexID> getExecutionVertexId(final ExecutionAttemptID executionAttemptId) {
		return Optional.ofNullable(executionGraph.getRegisteredExecutions().get(executionAttemptId))
			.map(this::getExecutionVertexId);
	}

	protected Boolean isCopyExecution(final ExecutionAttemptID executionAttemptId) {
		return Optional.ofNullable(executionGraph.getRegisteredExecutions().get(executionAttemptId))
				.map(Execution::isCopy).orElse(false);
	}

	protected ExecutionVertexID getExecutionVertexIdOrThrow(final ExecutionAttemptID executionAttemptId) {
		return getExecutionVertexId(executionAttemptId)
			.orElseThrow(() -> new IllegalStateException("Cannot find execution " + executionAttemptId));
	}

	private ExecutionVertexID getExecutionVertexId(final Execution execution) {
		return execution.getVertex().getID();
	}

	public ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
		return executionGraph.getAllVertices().get(executionVertexId.getJobVertexId()).getTaskVertices()[executionVertexId.getSubtaskIndex()];
	}

	public ExecutionJobVertex getExecutionJobVertex(final JobVertexID jobVertexId) {
		return executionGraph.getAllVertices().get(jobVertexId);
	}

	public Optional<Execution> getExecution(final ExecutionAttemptID executionAttemptID) {
		return Optional.ofNullable(executionGraph.getRegisteredExecutions().get(executionAttemptID));
	}

	protected JobGraph getJobGraph() {
		return jobGraph;
	}

	protected abstract long getNumberOfRestarts();

	protected abstract long getNumberOfFailFilteredByAggregatedStrategy();

	protected abstract long getNumberOfRestartsAggrByBackoffTime();

	protected abstract String getFailoverStrategyName();

	protected abstract String getRestartStrategyName();

	protected abstract Long getNumberOfFallbackToFullRestarts();

	protected abstract long getNumberOfNoResourceAvailableExceptions();

	private Map<ExecutionVertexID, ExecutionVertexVersion> incrementVersionsOfAllVertices() {
		return executionVertexVersioner.recordVertexModifications(
			IterableUtils.toStream(schedulingTopology.getVertices())
				.map(SchedulingExecutionVertex::getId)
				.collect(Collectors.toSet()));
	}

	public void transitionExecutionGraphState(final JobStatus current, final JobStatus newState) {
		executionGraph.transitionState(current, newState);
	}

	public void transitionAllExecutionState(final ExecutionState newState) {
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			Execution currentExecutionAttempt = vertex.getCurrentExecutionAttempt();
			currentExecutionAttempt.transitionState(newState);
		}
	}

	@VisibleForTesting
	CheckpointCoordinator getCheckpointCoordinator() {
		return executionGraph.getCheckpointCoordinator();
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public void setMainThreadExecutor(final ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
		initializeOperatorCoordinators(mainThreadExecutor);
		executionGraph.start(mainThreadExecutor);
	}

	@Override
	public void setResourceManager(final ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);
	}

	@Override
	public void registerJobStatusListener(final JobStatusListener jobStatusListener) {
		executionGraph.registerJobStatusListener(jobStatusListener);
	}

	@Override
	public final void startScheduling() {
		mainThreadExecutor.assertRunningInMainThread();
		registerJobMetrics();
		startAllOperatorCoordinators();
		startSchedulingInternal();
	}

	private void registerJobMetrics() {

		// register full restart gauge metrics and rate metrics
		jobManagerJobMetricGroup.gauge(MetricNames.FULL_RESTARTS, this::getNumberOfRestarts);
		jobManagerJobMetricGroup.meter(MetricNames.FULL_RESTARTS_RATE, new MeterView(executionGraph.getNumberOfRestartsCounter(), 60));
		jobManagerJobMetricGroup.gauge(MetricNames.NO_RESOURCE_AVAILABLE_EXCEPTION, this::getNumberOfNoResourceAvailableExceptions);
		jobManagerJobMetricGroup.gauge(MetricNames.NUM_FAIL_FILTERED_BY_AGGREGATED_STRATEGY_NAME, this::getNumberOfFailFilteredByAggregatedStrategy);
		jobManagerJobMetricGroup.gauge(MetricNames.NUM_RESTARTS_AGGR_BY_BACKOFF_TIME, this::getNumberOfRestartsAggrByBackoffTime);

		jobManagerJobMetricGroup.gauge(EVENT_METRIC_NAME, warehouseJobStartEventMessageRecorder.getJobStartEventMessageSet());

		jobManagerJobMetricGroup.gauge(MetricNames.EXECUTION_STATE_TIME, () -> (TagGaugeStore) () -> executionGraph.getAllVertices().values().stream()
				.flatMap((Function<ExecutionJobVertex, Stream<Tuple3<ExecutionState, Long, String>>>) executionJobVertex -> executionJobVertex.getExecutionStateTime().entrySet().stream()
						.map(stateTimeEntry -> new Tuple3<>(stateTimeEntry.getKey(), stateTimeEntry.getValue(), String.valueOf(executionJobVertex.getJobVertex().getIndexInCreatedOrder()))))
				.map(tuple -> new TagGaugeStore.TagGaugeMetric(
						tuple.f1,
						new TagGaugeStore.TagValuesBuilder()
								.addTagValue(MetricNames.EXECUTION_STATE_TAG_STATE_NAME, tuple.f0.name())
								.addTagValue(MetricNames.EXECUTION_STATE_TAG_VERTEX_INDEX_NAME, tuple.f2)
								.build()))
				.collect(Collectors.toList()));

		jobManagerJobMetricGroup.gauge(MetricNames.NUM_RESTARTS, () -> (TagGaugeStore) () -> {
			List<TagGaugeStore.TagGaugeMetric> tagGaugeMetrics = new ArrayList<>();
			tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
				getNumberOfRestarts() - getNumberOfFallbackToFullRestarts(),
				new TagGaugeStore.TagValuesBuilder()
					.addTagValue(MetricNames.NUM_RESTARTS_TAG_FAILOVER_STRATEGY_NAME, getFailoverStrategyName())
					.addTagValue(MetricNames.NUM_RESTARTS_TAG_RESTART_STRATEGY_NAME, getRestartStrategyName())
					.build()));
			tagGaugeMetrics.add(new TagGaugeStore.TagGaugeMetric(
				getNumberOfFallbackToFullRestarts(),
				new TagGaugeStore.TagValuesBuilder()
					.addTagValue(MetricNames.NUM_RESTARTS_TAG_FAILOVER_STRATEGY_NAME, METRIC_FALLBACK_TO_RESTART_NAME)
					.addTagValue(MetricNames.NUM_RESTARTS_TAG_RESTART_STRATEGY_NAME, getRestartStrategyName())
					.build()));
			return tagGaugeMetrics;
		});

	}

	protected abstract void startSchedulingInternal();

	@Override
	public void suspend(Throwable cause) {
		mainThreadExecutor.assertRunningInMainThread();

		incrementVersionsOfAllVertices();
		executionGraph.suspend(cause);
		disposeAllOperatorCoordinators();
	}

	@Override
	public void cancel() {
		mainThreadExecutor.assertRunningInMainThread();

		incrementVersionsOfAllVertices();
		executionGraph.cancel();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return executionGraph.getTerminationFuture().thenApply(FunctionUtils.nullFn());
	}

	@Override
	public final boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		final Optional<Execution> execution = getExecution(taskExecutionState.getID());
		final boolean isCopyExecution = isCopyExecution(taskExecutionState.getID());

		boolean updateSuccess = executionGraph.updateState(taskExecutionState);

		if (updateSuccess) {
			checkState(execution.isPresent());

			if (isNotifiable(getExecutionVertexId(execution.get()), taskExecutionState, isCopyExecution)) {
				updateTaskExecutionStateInternal(getExecutionVertexId(execution.get()), taskExecutionState, execution.get().getAssignedResourceLocation());
			}
			return true;
		} else {
			return false;
		}
	}

	private boolean isNotifiable(
			final ExecutionVertexID executionVertexId,
			final TaskExecutionState taskExecutionState) {
		return isNotifiable(executionVertexId, taskExecutionState, false);
	}

	private boolean isNotifiable(
			final ExecutionVertexID executionVertexId,
			final TaskExecutionState taskExecutionState,
			final boolean isCopyExecution) {

		final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

		// only notifies FINISHED and FAILED states which are needed at the moment.
		// can be refined in FLINK-14233 after the legacy scheduler is removed and
		// the actions are factored out from ExecutionGraph.
		switch (taskExecutionState.getExecutionState()) {
			case FINISHED:
			case FAILED:
				// only notifies a state update if it's effective, namely it successfully
				// turns the execution state to the expected value.
				if (executionVertex.getExecution(isCopyExecution)
						.getState() == taskExecutionState.getExecutionState()) {
					return true;
				} else if (taskExecutionState.getExecutionState() == ExecutionState.FINISHED
						&& executionVertex.getExecution(isCopyExecution).getState() == ExecutionState.FAILED) {
					// try to finish the task but splits' status is not compatible
					return true;
				} else {
					log.warn("The state is not compatible in {}", executionVertex
							.getExecution(isCopyExecution(taskExecutionState.getID())).getVertexWithAttempt());
				}
				break;
			default:
				break;
		}

		return false;
	}

	protected void updateTaskExecutionStateInternal(final ExecutionVertexID executionVertexId, final TaskExecutionState taskExecutionState, @Nullable final TaskManagerLocation taskManagerLocation) {
	}

	protected void tryReportBlacklist(TaskManagerLocation taskManagerLocation, Throwable error) {
		try {
			if (taskManagerLocation != null) {
				remoteBlacklistReporter.onFailure(taskManagerLocation.getFQDNHostname(), taskManagerLocation.getResourceID(), error, System.currentTimeMillis());
			} else if (error instanceof NoResourceAvailableException) {
				remoteBlacklistReporter.clearBlacklist();
			} else {
				log.info("taskManagerLocation is null, not report to blacklist.");
			}
		} catch (Throwable t) {
			log.info("Error while report failure to blacklist tracker", t);
		}
	}

	@Override
	public SerializedInputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new IllegalArgumentException("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			throw new IllegalArgumentException("Cannot find execution vertex for vertex ID " + vertexID);
		}

		if (vertex.getSplitAssigner() == null) {
			throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
		}

		final InputSplit nextInputSplit = execution.getNextInputSplit();

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return new SerializedInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			IOException reason = new IOException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			throw reason;
		}
	}

	@Override
	public ExecutionState requestPartitionState(
		final IntermediateDataSetID intermediateResultId,
		final ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException {

		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return execution.getState();
		}
		else {
			final IntermediateResult intermediateResult =
				executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
					.getPartitionById(resultPartitionId.getPartitionId())
					.getProducer()
					.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return producerExecution.getState();
				} else {
					throw new PartitionProducerDisposedException(resultPartitionId);
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID "
					+ intermediateResultId + " not found.");
			}
		}
	}

	@Override
	public final void scheduleOrUpdateConsumers(final ResultPartitionID partitionId) {
		mainThreadExecutor.assertRunningInMainThread();

		try {
			executionGraph.scheduleOrUpdateConsumers(partitionId);
		} catch (ExecutionGraphException e) {
			throw new RuntimeException(e);
		}

		if (executionGraph.getRegisteredExecutions().get(partitionId.getProducerId()).isCopy()) {
			scheduleCopyConsumersInternal(partitionId.getPartitionId());
		} else {
			scheduleOrUpdateConsumersInternal(partitionId.getPartitionId());
		}
	}

	protected void scheduleCopyConsumersInternal(IntermediateResultPartitionID resultPartitionId) {
	}

	protected void scheduleOrUpdateConsumersInternal(IntermediateResultPartitionID resultPartitionId) {
	}

	@Override
	public ArchivedExecutionGraph requestJob() {
		mainThreadExecutor.assertRunningInMainThread();
		return ArchivedExecutionGraph.createFrom(executionGraph);
	}

	@Override
	public JobStatus requestJobStatus() {
		return executionGraph.getState();
	}

	@Override
	public JobDetails requestJobDetails() {
		mainThreadExecutor.assertRunningInMainThread();
		return WebMonitorUtils.createDetailsForJob(executionGraph, jobMasterConfiguration);
	}

	@Override
	public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName) throws UnknownKvStateLocation, FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return location;
			} else {
				throw new UnknownKvStateLocation(registrationName);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateRegistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName, final KvStateID kvStateId, final InetSocketAddress kvStateServerAddress) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateUnregistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
		mainThreadExecutor.assertRunningInMainThread();

		executionGraph.updateAccumulators(accumulatorSnapshot);
	}

	@Override
	public Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(final JobVertexID jobVertexId) throws FlinkException {
		final ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexId);
		if (jobVertex == null) {
			throw new FlinkException("JobVertexID not found " +
				jobVertexId);
		}

		return backPressureStatsTracker.getOperatorBackPressureStats(jobVertex);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(
		final String targetDirectory,
		final boolean cancelJob,
		final long savepointTimeout) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			throw new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
		} else if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			throw new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'.");
		}

		log.info("Triggering {}savepoint(timeout={}) for job {}.", cancelJob ? "cancel-with-" : "", savepointTimeout, jobGraph.getJobID());

		if (cancelJob) {
			checkpointCoordinator.stopCheckpointScheduler();
		}

		return checkpointCoordinator
			.triggerSavepoint(targetDirectory, savepointTimeout)
			.thenApply(CompletedCheckpoint::getExternalPointer)
			.handleAsync((path, throwable) -> {
				if (throwable != null) {
					if (cancelJob) {
						startCheckpointScheduler(checkpointCoordinator);
					}
					throw new CompletionException(throwable);
				} else if (cancelJob) {
					log.info("Savepoint stored in {}. Now cancelling {}.", path, jobGraph.getJobID());
					cancel();
				}
				return path;
			}, mainThreadExecutor);
	}

	@Override
	public CompletableFuture<String> triggerDetachSavepoint(String savepointId, boolean blockSource, long savepointTimeout) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			throw new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
		}

		log.info("Triggering {}detach-savepoint for job {}, with savepoint_id={},timeout={}.",
			blockSource ? "block-source-with-" : "", jobGraph.getJobID(), savepointId, savepointTimeout);

		checkNotNull(savepointLocationPrefix, "savepoint directory prefix for detach savepoint is not set, " +
			"set this value in config state.savepoint.location-prefix.");
		LocalDate currentDate = LocalDate.now();
		String dateSubDir = String.format("%04d%02d%02d", currentDate.getYear(), currentDate.getMonthValue(), currentDate.getDayOfMonth());
		String jobUID = jobGraph.getJobUID() == null ? jobGraph.getName() : jobGraph.getJobUID();
		String namespace = jobMasterConfiguration.getString(CheckpointingOptions.CHECKPOINTS_NAMESPACE);
		String manualSavepointPath;
		if (namespace != null) {
			manualSavepointPath = String.format("%s/%s/%s/%s/%s", savepointLocationPrefix, dateSubDir, jobUID, namespace, savepointId);
		} else {
			manualSavepointPath = String.format("%s/%s/%s/%s", savepointLocationPrefix, dateSubDir, jobUID, savepointId);
		}
		log.info("On triggering manual savepoint at {}", manualSavepointPath);

		if (blockSource) {
			checkpointCoordinator.stopCheckpointScheduler();

			return checkpointCoordinator
				.triggerDetachSyncSavepoint(manualSavepointPath, savepointId, savepointTimeout)
				.thenApply(CompletedCheckpoint::getExternalPointer)
				.handleAsync((path, throwable) -> {
				if (throwable != null) {
					startCheckpointScheduler(checkpointCoordinator);
					log.info("Detach savepoint {} with source block fails, resume blocked sources", savepointId);
					throw new CompletionException(throwable);
				} else {
					log.info("Detach savepoint {} stored in {} succeed. Now keep blocking source.", savepointId, path);
				}
				return path;
			}, mainThreadExecutor);
		} else {
			return checkpointCoordinator
				.triggerDetachSavepoint(manualSavepointPath, savepointId, savepointTimeout)
				.thenApply(CompletedCheckpoint::getExternalPointer)
				.handleAsync((path, throwable) -> {
					if (throwable != null) {
						log.info("Detach savepoint {} without source block fails", savepointId);
						throw new CompletionException(throwable);
					}
					return path;
				}, mainThreadExecutor);
		}
	}

	@Override
	public CompletableFuture<List<String>> dumpPendingSavepoints() {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator == null) {
			throw new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID()));
		}

		return checkpointCoordinator.getPendingSavepointsUnsafe();
	}

	private void startCheckpointScheduler(final CheckpointCoordinator checkpointCoordinator) {
		mainThreadExecutor.assertRunningInMainThread();

		if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
			try {
				checkpointCoordinator.startCheckpointScheduler();
			} catch (IllegalStateException ignored) {
				// Concurrent shut down of the coordinator
			}
		}
	}

	@Override
	public void acknowledgeCheckpoint(final JobID jobID, final ExecutionAttemptID executionAttemptID, final long checkpointId, final CheckpointMetrics checkpointMetrics, final TaskStateSnapshot checkpointState) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final AcknowledgeCheckpoint ackMessage = new AcknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			checkpointMetrics,
			checkpointState);

		final String taskManagerLocationInfo = retrieveTaskManagerLocation(executionAttemptID);

		if (checkpointCoordinator != null) {
			futureExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveAcknowledgeMessage(ackMessage, taskManagerLocationInfo);
				} catch (Throwable t) {
					log.warn("Error while processing checkpoint acknowledgement message", t);
				}
			});
		} else {
			String errorMessage = "Received AcknowledgeCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public void performCheckpoint(PerformCheckpoint performCheckpoint) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		if (checkpointCoordinator != null) {
			futureExecutor.execute(() -> {
				try {
					checkpointCoordinator.receivePerformCheckpointMessage(performCheckpoint);
				} catch (Exception e) {
					log.error("Error in CheckpointCoordinator while processing {}.", performCheckpoint, e);
				}
			});
		}
	}

	@Override
	public void declineCheckpoint(final DeclineCheckpoint decline) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
		final String taskManagerLocationInfo = retrieveTaskManagerLocation(decline.getTaskExecutionId());

		if (checkpointCoordinator != null) {
			futureExecutor.execute(() -> {
				try {
					checkpointCoordinator.receiveDeclineMessage(decline, taskManagerLocationInfo);
				} catch (Exception e) {
					log.error("Error in CheckpointCoordinator while processing {}", decline, e);
				}
			});
		} else {
			String errorMessage = "Received DeclineCheckpoint message for job {} with no CheckpointCoordinator";
			if (executionGraph.getState() == JobStatus.RUNNING) {
				log.error(errorMessage, jobGraph.getJobID());
			} else {
				log.debug(errorMessage, jobGraph.getJobID());
			}
		}
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(final String targetDirectory, final boolean advanceToEndOfEventTime, long savepointTimeout) {
		mainThreadExecutor.assertRunningInMainThread();

		final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		if (checkpointCoordinator == null) {
			return FutureUtils.completedExceptionally(new IllegalStateException(
				String.format("Job %s is not a streaming job.", jobGraph.getJobID())));
		}

		if (targetDirectory == null && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
			log.info("Trying to cancel job {} with savepoint, but no savepoint directory configured.", jobGraph.getJobID());

			return FutureUtils.completedExceptionally(new IllegalStateException(
				"No savepoint directory configured. You can either specify a directory " +
					"while cancelling via -s :targetDirectory or configure a cluster-wide " +
					"default via key '" + CheckpointingOptions.SAVEPOINT_DIRECTORY.key() + "'."));
		}

		log.info("Triggering stop-with-savepoint for job {}.", jobGraph.getJobID());

		// we stop the checkpoint coordinator so that we are guaranteed
		// to have only the data of the synchronous savepoint committed.
		// in case of failure, and if the job restarts, the coordinator
		// will be restarted by the CheckpointCoordinatorDeActivator.
		checkpointCoordinator.stopCheckpointScheduler();

		final CompletableFuture<String> savepointFuture = checkpointCoordinator
			.triggerSynchronousSavepoint(advanceToEndOfEventTime, targetDirectory, savepointTimeout)
			.thenApply(CompletedCheckpoint::getExternalPointer);

		final CompletableFuture<JobStatus> terminationFuture = executionGraph
			.getTerminationFuture()
			.handle((jobstatus, throwable) -> {

				if (throwable != null) {
					log.info("Failed during stopping job {} with a savepoint. Reason: {}", jobGraph.getJobID(), throwable.getMessage());
					throw new CompletionException(throwable);
				} else if (jobstatus != JobStatus.FINISHED) {
					log.info("Failed during stopping job {} with a savepoint. Reason: Reached state {} instead of FINISHED.", jobGraph.getJobID(), jobstatus);
					throw new CompletionException(new FlinkException("Reached state " + jobstatus + " instead of FINISHED."));
				}
				return jobstatus;
			});

		return savepointFuture.thenCompose((path) ->
			terminationFuture.thenApply((jobStatus -> path)))
			.handleAsync((path, throwable) -> {
				if (throwable != null) {
					// restart the checkpoint coordinator if stopWithSavepoint failed.
					startCheckpointScheduler(checkpointCoordinator);
					throw new CompletionException(throwable);
				}

				return path;
			}, mainThreadExecutor);
	}

	private String retrieveTaskManagerLocation(ExecutionAttemptID executionAttemptID) {
		final Optional<Execution> currentExecution = Optional.ofNullable(executionGraph.getRegisteredExecutions().get(executionAttemptID));

		return currentExecution
			.map(Execution::getAssignedResourceLocation)
			.map(TaskManagerLocation::toString)
			.orElse("Unknown location");
	}

	// ------------------------------------------------------------------------
	//  Operator Coordinators
	//
	//  Note: It may be worthwhile to move the OperatorCoordinators out
	//        of the scheduler (have them owned by the JobMaster directly).
	//        Then we could avoid routing these events through the scheduler and
	//        doing this lazy initialization dance. However, this would require
	//        that the Scheduler does not eagerly construct the CheckpointCoordinator
	//        in the ExecutionGraph and does not eagerly restore the savepoint while
	//        doing that. Because during savepoint restore, the OperatorCoordinators
	//        (or at least their holders) already need to exist, to accept the restored
	//        state. But some components they depend on (Scheduler and MainThreadExecutor)
	//        are not fully usable and accessible at that point.
	// ------------------------------------------------------------------------

	@Override
	public void deliverOperatorEventToCoordinator(
			final ExecutionAttemptID taskExecutionId,
			final OperatorID operatorId,
			final OperatorEvent evt) throws FlinkException {

		// Failure semantics (as per the javadocs of the method):
		// If the task manager sends an event for a non-running task or an non-existing operator
		// coordinator, then respond with an exception to the call. If task and coordinator exist,
		// then we assume that the call from the TaskManager was valid, and any bubbling exception
		// needs to cause a job failure.

		final Execution exec = executionGraph.getRegisteredExecutions().get(taskExecutionId);
		if (exec == null || exec.getState() != ExecutionState.RUNNING) {
			// This situation is common when cancellation happens, or when the task failed while the
			// event was just being dispatched asynchronously on the TM side.
			// It should be fine in those expected situations to just ignore this event, but, to be
			// on the safe, we notify the TM that the event could not be delivered.
			throw new TaskNotRunningException("Task is not known or in state running on the JobManager.");
		}

		final OperatorCoordinatorHolder coordinator = coordinatorMap.get(operatorId);
		if (coordinator == null) {
			throw new FlinkException("No coordinator registered for operator " + operatorId);
		}

		try {
			coordinator.handleEventFromOperator(exec.getParallelSubtaskIndex(), evt);
		} catch (Throwable t) {
			ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
			handleGlobalFailure(t);
		}
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			OperatorID operator,
			CoordinationRequest request) throws FlinkException {

		final OperatorCoordinatorHolder coordinatorHolder = coordinatorMap.get(operator);
		if (coordinatorHolder == null){
			throw new FlinkException("Coordinator of operator " + operator + " does not exist");
		}

		final OperatorCoordinator coordinator = coordinatorHolder.coordinator();
		if (coordinator instanceof CoordinationRequestHandler) {
			return ((CoordinationRequestHandler) coordinator).handleCoordinationRequest(request);
		} else {
			throw new FlinkException("Coordinator of operator " + operator + " cannot handle client event");
		}
	}

	private void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor) {
		for (OperatorCoordinatorHolder coordinatorHolder : getAllCoordinators()) {
			coordinatorHolder.lazyInitialize(this, mainThreadExecutor);
		}
	}

	private void startAllOperatorCoordinators() {
		final Collection<OperatorCoordinatorHolder> coordinators = getAllCoordinators();
		try {
			for (OperatorCoordinatorHolder coordinator : coordinators) {
				coordinator.start();
			}
		}
		catch (Throwable t) {
			log.error("String operator coordinator failed.", t);
			ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
			coordinators.forEach(IOUtils::closeQuietly);
			throw new FlinkRuntimeException("Failed to start the operator coordinators", t);
		}
	}

	private void disposeAllOperatorCoordinators() {
		getAllCoordinators().forEach(IOUtils::closeQuietly);
	}

	private Collection<OperatorCoordinatorHolder> getAllCoordinators() {
		return coordinatorMap.values();
	}

	private Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap() {
		Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap = new HashMap<>();
		for (ExecutionJobVertex vertex : executionGraph.getAllVertices().values()) {
			for (OperatorCoordinatorHolder holder : vertex.getOperatorCoordinators()) {
				coordinatorMap.put(holder.operatorId(), holder);
			}
		}
		return coordinatorMap;
	}

	protected Time getRpcTimeout() {
		return rpcTimeout;
	}

	protected ScheduledExecutorService getFutureExecutor() {
		return futureExecutor;
	}

	// ------------------------------------------------------------------------
	//  access utils for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	JobID getJobId() {
		return jobGraph.getJobID();
	}
}
