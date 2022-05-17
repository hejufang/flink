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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BenchmarkOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.GatewayTaskDeployment;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.restart.ThrowingRestartStrategy;
import org.apache.flink.runtime.executiongraph.speculation.SpeculationStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.BackPressureStatsTracker;
import org.apache.flink.runtime.scheduler.strategy.EagerWithBlockEdgeSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.LazyFromSourcesSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.LoggerHelper;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

	private final Logger log;

	private final ClassLoader userCodeLoader;

	private final ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutor delayExecutor;

	private final SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexOperations executionVertexOperations;

	private final Set<ExecutionVertexID> verticesWaitingForRestart;

	private final ScheduledExecutor taskCancelingCheckExecutor;

	private final boolean jobLogDetailDisable;

	private final boolean batchRequestSlotsEnable;

	private final int batchTaskCount;

	private final boolean taskSubmitToRunningStatus;

	private final boolean taskScheduledFinishEnable;

	private final JobResultClientManager jobResultClientManager;

	public DefaultScheduler(
			final Logger log,
			final JobGraph jobGraph,
			final BackPressureStatsTracker backPressureStatsTracker,
			final Executor ioExecutor,
			final Configuration jobMasterConfiguration,
			final ScheduledExecutorService futureExecutor,
			final ScheduledExecutor delayExecutor,
			final ClassLoader userCodeLoader,
			final CheckpointRecoveryFactory checkpointRecoveryFactory,
			final Time rpcTimeout,
			final BlobWriter blobWriter,
			final JobManagerJobMetricGroup jobManagerJobMetricGroup,
			final ShuffleMaster<?> shuffleMaster,
			final JobMasterPartitionTracker partitionTracker,
			final SchedulingStrategyFactory schedulingStrategyFactory,
			final FailoverStrategy.Factory failoverStrategyFactory,
			final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
			final ExecutionVertexOperations executionVertexOperations,
			final ExecutionVertexVersioner executionVertexVersioner,
			final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
			final SpeculationStrategy.Factory speculationStrategyFactory,
			final RemoteBlacklistReporter remoteBlacklistReporter,
			final JobResultClientManager jobResultClientManager) throws Exception {

		super(
			log,
			jobGraph,
			backPressureStatsTracker,
			ioExecutor,
			jobMasterConfiguration,
			new ThrowingSlotProvider(), // this is not used any more in the new scheduler
			futureExecutor,
			userCodeLoader,
			checkpointRecoveryFactory,
			rpcTimeout,
			new ThrowingRestartStrategy.ThrowingRestartStrategyFactory(),
			blobWriter,
			jobManagerJobMetricGroup,
			Time.seconds(0), // this is not used any more in the new scheduler
			shuffleMaster,
			partitionTracker,
			executionVertexVersioner,
			false,
			speculationStrategyFactory,
			remoteBlacklistReporter);

		warehouseJobStartEventMessageRecorder.createSchedulerStart();

		this.log = log;

		this.delayExecutor = checkNotNull(delayExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.executionVertexOperations = checkNotNull(executionVertexOperations);
		this.batchRequestSlotsEnable = jobMasterConfiguration.getBoolean(JobManagerOptions.JOBMANAGER_BATCH_REQUEST_SLOTS_ENABLE);
		this.batchTaskCount = jobMasterConfiguration.getInteger(JobManagerOptions.JOBMANAGER_SUBMIT_BATCH_TASK_COUNT);
		this.taskSubmitToRunningStatus = jobMasterConfiguration.getBoolean(CoreOptions.FLINK_SUBMIT_RUNNING_NOTIFY);
		if (taskSubmitToRunningStatus && !batchRequestSlotsEnable) {
			throw new IllegalArgumentException(CoreOptions.FLINK_SUBMIT_RUNNING_NOTIFY.key() + " can be true only when "
				+ JobManagerOptions.JOBMANAGER_BATCH_REQUEST_SLOTS_ENABLE.key() + " is true first.");
		}
		this.jobResultClientManager = jobResultClientManager;

		final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(
			getSchedulingTopology(),
			getResultPartitionAvailabilityChecker());

		this.executionFailureHandler = new ExecutionFailureHandler(
			getSchedulingTopology(),
			failoverStrategy,
			restartBackoffTimeStrategy);
		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology(), executionGraph.getLogicalTopology());
		if (schedulingStrategy instanceof EagerWithBlockEdgeSchedulingStrategy &&
				!(restartBackoffTimeStrategy instanceof NoRestartBackoffTimeStrategy)) {
			throw new UnsupportedOperationException("EagerWithBlockEdgeSchedulingStrategy can only work with NoRestartBackoffTimeStrategy.");
		}

		this.jobLogDetailDisable = jobMasterConfiguration.getBoolean(CoreOptions.FLINK_JOB_LOG_DETAIL_DISABLE);
		if (schedulingStrategy instanceof LazyFromSourcesSchedulingStrategy &&
				jobMasterConfiguration.getBoolean(JobManagerOptions.JOBMANAGER_BATCH_REQUEST_SLOTS_ENABLE)) {
			throw new UnsupportedOperationException("LazyFromSourcesSchedulingStrategy doesn't support batch request slots.");
		}
		log.info("Using failover strategy {} scheduler {} for {} ({}).",
			LoggerHelper.secMark("failoverStrategy", failoverStrategy),
			LoggerHelper.secMark("schedulerStrategy", schedulingStrategy.getClass().getSimpleName()),
			LoggerHelper.secMark("jobName", jobGraph.getName()),
			LoggerHelper.secMark("jobID", jobGraph.getJobID()));

		int maxTasksPerWorker = jobMasterConfiguration.getInteger(JobManagerOptions.JOBMANAGER_MAX_TASKS_PER_JOB);
		int minWorkersPerJob = jobMasterConfiguration.getInteger(JobManagerOptions.JOBMANAGER_MIN_WORKERS_PER_JOB);

		final ExecutionSlotAllocationContext slotAllocationContext = new ExecutionSlotAllocationContext(
			getStateLocationRetriever(),
			getInputsLocationsRetriever(),
			executionVertexID -> getExecutionVertex(executionVertexID).getResourceProfile(),
			executionVertexID -> getExecutionVertex(executionVertexID).getLatestPriorAllocation(),
			getSchedulingTopology(),
			() -> getJobGraph().getSlotSharingGroups(),
			() -> getJobGraph().getCoLocationGroupDescriptors(),
			jobLogDetailDisable,
			batchRequestSlotsEnable,
			maxTasksPerWorker,
			minWorkersPerJob);
		this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory).createInstance(getInputsLocationsRetriever(), slotAllocationContext);

		this.verticesWaitingForRestart = new HashSet<>();
		this.taskCancelingCheckExecutor = new ScheduledExecutorServiceAdapter(Executors.newSingleThreadScheduledExecutor(
			new ExecutorThreadFactory("Task canceling check")));
		this.taskScheduledFinishEnable = jobMasterConfiguration.get(BenchmarkOptions.JOB_SCHEDULED_THEN_FINISH_ENABLE);
		warehouseJobStartEventMessageRecorder.createSchedulerFinish();
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public long getNumberOfRestarts() {
		return executionFailureHandler.getNumberOfRestarts();
	}

	@Override
	protected long getNumberOfFailFilteredByAggregatedStrategy() {
		return executionFailureHandler.getNumberOfFailFilteredByAggregatedStrategy();
	}

	@Override
	protected long getNumberOfRestartsAggrByBackoffTime() {
		return executionFailureHandler.getNumberOfRestartsAggrByBackoffTime();
	}

	@Override
	protected String getFailoverStrategyName() {
		return executionFailureHandler.getFailoverStrategy().getClass().getSimpleName();
	}

	@Override
	protected String getRestartStrategyName() {
		return executionFailureHandler.getRestartStrategy().getClass().getSimpleName();
	}

	@Override
	protected Long getNumberOfFallbackToFullRestarts() {
		return executionFailureHandler.getNumberOfFallbackToFullRestarts();
	}

	@Override
	protected long getNumberOfNoResourceAvailableExceptions() {
		return executionFailureHandler.getNumberOfNoResourceAvailableExceptions();
	}

	@Override
	public long getNumberOfPartitionExceptions() {
		return executionFailureHandler.getNumberOfPartitionExceptions();
	}

	@Override
	protected void startSchedulingInternal() {
		log.info("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());
		prepareExecutionGraphForNgScheduling();
		warehouseJobStartEventMessageRecorder.scheduleTaskStart(executionGraph.getGlobalModVersion());
		schedulingStrategy.startScheduling();
	}

	@Override
	protected void updateTaskExecutionStateInternal(final ExecutionVertexID executionVertexId, final TaskExecutionState taskExecutionState, @Nullable final TaskManagerLocation taskManagerLocation) {
		schedulingStrategy.onExecutionStateChange(executionVertexId, taskExecutionState.getExecutionState());
		maybeHandleTaskFailure(taskExecutionState, executionVertexId, taskManagerLocation);
	}

	private void maybeHandleTaskFailure(final TaskExecutionState taskExecutionState, final ExecutionVertexID executionVertexId, @Nullable final TaskManagerLocation taskManagerLocation) {
		if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
			final Throwable error = taskExecutionState.getError(userCodeLoader);
			handleTaskFailure(executionVertexId, error, taskManagerLocation);
		}
	}

	private void handleTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error, @Nullable final TaskManagerLocation taskManagerLocation) {
		tryReportBlacklist(taskManagerLocation, error);
		setGlobalFailureCause(error);
		notifyCoordinatorsAboutTaskFailure(executionVertexId, error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getFailureHandlingResult(executionVertexId, error);
		if (failureHandlingResult.canRestart() && failureHandlingResult.isGlobalFailure()) {
			// Fallback to global restart.
			handleGlobalFailure(new JobException("Fallback to global restart.", error));
			return;
		}
		maybeRestartTasks(failureHandlingResult);
	}

	private void notifyCoordinatorsAboutTaskFailure(final ExecutionVertexID executionVertexId, @Nullable final Throwable error) {
		final ExecutionJobVertex jobVertex = getExecutionJobVertex(executionVertexId.getJobVertexId());
		final int subtaskIndex = executionVertexId.getSubtaskIndex();

		jobVertex.getOperatorCoordinators().forEach(c -> c.subtaskFailed(subtaskIndex, error));
	}

	@Override
	public void handleGlobalFailure(final Throwable error) {
		setGlobalFailureCause(error);

		log.info("Trying to recover from a global failure.", error);
		final FailureHandlingResult failureHandlingResult = executionFailureHandler.getGlobalFailureHandlingResult(error);
		maybeRestartTasks(failureHandlingResult);
	}

	private void maybeRestartTasks(final FailureHandlingResult failureHandlingResult) {
		if (failureHandlingResult.canRestart()) {
			restartTasksWithDelay(failureHandlingResult);
		} else {
			failJob(failureHandlingResult.getError());
		}
	}

	private void restartTasksWithDelay(final FailureHandlingResult failureHandlingResult) {
		final Set<ExecutionVertexID> verticesToRestart = failureHandlingResult.getVerticesToRestart();

		final Set<ExecutionVertexVersion> executionVertexVersions =
			new HashSet<>(executionVertexVersioner.recordVertexModifications(verticesToRestart).values());
		final boolean globalRecovery = failureHandlingResult.isGlobalFailure();

		addVerticesToRestartPending(verticesToRestart);

		// Only cancel pending slot, allocated slot will be released in execution.finishCancellation.
		cancelSlotRequests(verticesToRestart);
		final CompletableFuture<?> cancelFuture = cancelTasksAsync(verticesToRestart);

		delayExecutor.schedule(
			() -> FutureUtils.assertNoException(
				cancelFuture.thenRunAsync(restartTasks(executionVertexVersions, globalRecovery), getMainThreadExecutor())),
			failureHandlingResult.getRestartDelayMS(),
			TimeUnit.MILLISECONDS);
	}

	private void addVerticesToRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.addAll(verticesToRestart);
		transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.RESTARTING);
	}

	private void removeVerticesFromRestartPending(final Set<ExecutionVertexID> verticesToRestart) {
		verticesWaitingForRestart.removeAll(verticesToRestart);
		if (verticesWaitingForRestart.isEmpty()) {
			transitionExecutionGraphState(JobStatus.RESTARTING, JobStatus.RUNNING);
		}
	}

	private Runnable restartTasks(final Set<ExecutionVertexVersion> executionVertexVersions, final boolean isGlobalRecovery) {
		return () -> {
			final Set<ExecutionVertexID> verticesToRestart = executionVertexVersioner.getUnmodifiedExecutionVertices(executionVertexVersions);

			removeVerticesFromRestartPending(verticesToRestart);

			resetForNewExecutions(verticesToRestart);

			try {
				restoreState(verticesToRestart, isGlobalRecovery);
			} catch (Throwable t) {
				handleGlobalFailure(t);
				return;
			}

			schedulingStrategy.restartTasks(verticesToRestart);
		};
	}

	private void cancelSlotRequests(final Set<ExecutionVertexID> verticesToRestart) {
		verticesToRestart.forEach(executionSlotAllocator::cancel);
	}

	private CompletableFuture<?> cancelTasksAsync(final Set<ExecutionVertexID> verticesToRestart) {
		final List<CompletableFuture<?>> cancelFutures = verticesToRestart.stream()
			.map(this::cancelExecutionVertex)
			.collect(Collectors.toList());

		return FutureUtils.combineAll(cancelFutures);
	}

	private CompletableFuture<?> cancelExecutionVertex(final ExecutionVertexID executionVertexId) {
		final ExecutionVertex vertex = getExecutionVertex(executionVertexId);

		notifyCoordinatorOfCancellation(vertex);

		CompletableFuture<?> cancel = executionVertexOperations.cancel(vertex);
		Execution execution = vertex.getCurrentExecutionAttempt();
		if (executionCancellationTimeoutEnable) {
			log.debug("check execution {} cancellation timeout enable", execution.getVertexWithAttempt());
			taskCancelingCheckExecutor.schedule(() -> checkCancelingTimeoutDelay(execution),
				executionCancellationTimeout, TimeUnit.MILLISECONDS);
		}
		return cancel;
	}

	private void checkCancelingTimeoutDelay(Execution execution) {

		if (!execution.getReleaseFuture().isDone()) {
			CompletableFuture<Void> checkCancelingTimeout = CompletableFuture.supplyAsync(() -> {
				checkCancelingTimeout(execution);
				return null;
			}, getMainThreadExecutor());
			// kill tm one by one to reduce main thread pressure.
			try {
				checkCancelingTimeout.get();
			} catch (Exception e) {
				log.error("checkCancelingTimeout fail, execution: {}", execution);
			}
		}
	}

	private void checkCancelingTimeout(Execution execution) {
		// If the task status stack in canceling, force close the taskManager.
		if (!execution.getReleaseFuture().isDone()) {
			log.warn("Task did not exit gracefully within " + executionCancellationTimeout / 1000 + " + seconds.");
			ResourceID resourceID = execution.getAssignedResourceLocation().getResourceID();
			FlinkException cause = new FlinkException("Task did not exit gracefully, it should be force shutdown.");
			if (resourceManagerGateway == null) {
				log.warn("Scheduler has no ResourceManager connected");
				return;
			}
			try {
				resourceManagerGateway.releaseTaskManager(resourceID, cause);
			} catch (Exception e) {
				log.error("close canceling timeout task {} fail", execution, e);
			}
		}
	}

	@Override
	protected void scheduleOrUpdateConsumersInternal(final IntermediateResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(partitionId);
	}

	@Override
	protected void scheduleCopyConsumersInternal(final IntermediateResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(partitionId, true);
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		validateDeploymentOptions(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(executionVertexDeploymentOptions);

		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		warehouseJobStartEventMessageRecorder.scheduleTaskAllocateResource(executionGraph.getGlobalModVersion());
		waitForAllSlotsAndDeploy(deploymentHandles);
	}

	private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		deploymentOptions.stream()
			.forEach(deploymentOption -> {
				ExecutionVertexID vertexID = deploymentOption.getExecutionVertexId();
				ExecutionVertex vertex = getExecutionVertex(vertexID);
				ExecutionState currentState = deploymentOption.getDeploymentOption().isDeployCopy() ?
					vertex.getCopyExecution().getState() : vertex.getExecutionState();
				checkState(
					currentState == ExecutionState.CREATED,
					"expected vertex %s to be in CREATED state, was: %s", vertexID, currentState);
			});
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(option -> ExecutionVertexSchedulingRequirementsMapper.from(getExecutionVertex(option.getExecutionVertexId()), option.getDeploymentOption()))
			.collect(Collectors.toList()));
	}

	private static List<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
		FutureUtils.assertNoException(
			assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles)));
	}

	private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle));
			slotAssignedFutures.add(slotAssigned);
		}
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	public BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
		return (ignored, throwable) -> {
			propagateIfNonNull(throwable);
			long startDeployTaskTime = System.currentTimeMillis();
			executionGraph.setScheduledTimestamp();
			warehouseJobStartEventMessageRecorder.scheduleTaskFinish(executionGraph.getGlobalModVersion());
			warehouseJobStartEventMessageRecorder.deployTaskStart(executionGraph.getGlobalModVersion());

			if (taskScheduledFinishEnable) {
				transitionExecutionGraphState(JobStatus.RUNNING, JobStatus.FINISHED);
				transitionAllExecutionState(ExecutionState.FINISHED);
				warehouseJobStartEventMessageRecorder.deployTaskFinish(executionGraph.getGlobalModVersion());
				if (jobResultClientManager != null) {
					jobResultClientManager.getJobChannelManager(getJobId()).completeAllTask();
				}
				return null;
			}

			if (batchRequestSlotsEnable) {
				GatewayDeploymentManager gatewayDeploymentManager = new GatewayDeploymentManager();
				for (final DeploymentHandle deploymentHandle : deploymentHandles) {
					final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
					final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
					checkState(slotAssigned.isDone());

					FutureUtils.assertNoException(
						slotAssigned.handle(deployOrHandleError(deploymentHandle, gatewayDeploymentManager)));
				}
				for (Map.Entry<ResourceID, List<GatewayTaskDeployment>> entry : gatewayDeploymentManager.getGatewayDeploymentList().entrySet()) {
					if (taskSubmitToRunningStatus) {
						notifyTaskRunning(entry);
					}
					TaskManagerGateway gateway = gatewayDeploymentManager.getTaskManagerGateway(entry.getKey());
					getFutureExecutor().execute(() -> {
						submitTaskList(entry, gateway);
					});
				}
				log.info("Deploy Task take {} ms for job {} with tasks {}.", System.currentTimeMillis() - startDeployTaskTime, getJobId(), deploymentHandles.size());
			} else {
				for (final DeploymentHandle deploymentHandle : deploymentHandles) {
					final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
					final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
					checkState(slotAssigned.isDone());

					FutureUtils.assertNoException(
						slotAssigned.handle(deployOrHandleError(deploymentHandle)));
				}
			}
			if (jobLogDetailDisable) {
				log.info("Deploy Task take {} ms for job {}.", System.currentTimeMillis() - startDeployTaskTime, getJobId());
			} else {
				log.info("Deploy Task take {} ms.", System.currentTimeMillis() - startDeployTaskTime);
			}
			warehouseJobStartEventMessageRecorder.deployTaskFinish(executionGraph.getGlobalModVersion());
			return null;
		};
	}

	private void notifyTaskRunning(Map.Entry<ResourceID, List<GatewayTaskDeployment>> entry) {
		CompletableFuture.runAsync(() -> {
				for (GatewayTaskDeployment gatewayTaskDeployment : entry.getValue()) {
					updateTaskExecutionState(new TaskExecutionState(getJobId(), gatewayTaskDeployment.getExecution().getAttemptId(), RUNNING));
				}
			},
			getMainThreadExecutor());
	}

	private void submitTaskList(Map.Entry<ResourceID, List<GatewayTaskDeployment>> entry, TaskManagerGateway gateway) {
		List<GatewayDeploymentListEntity> taskDescriptorsList = new ArrayList<>();
		List<TaskDeploymentDescriptor> descriptors = new ArrayList<>(batchTaskCount);
		List<GatewayTaskDeployment> gatewayTaskDeployments = new ArrayList<>(batchTaskCount);
		for (GatewayTaskDeployment deployment : entry.getValue()) {
			TaskDeploymentDescriptor deploymentDescriptor = deployment.getTaskDeploymentDescriptor();
			if (descriptors.size() >= batchTaskCount) {
				taskDescriptorsList.add(new GatewayDeploymentListEntity(descriptors, gatewayTaskDeployments));
				descriptors = new ArrayList<>(batchTaskCount);
				gatewayTaskDeployments = new ArrayList<>(batchTaskCount);
			}
			descriptors.add(deploymentDescriptor);
			gatewayTaskDeployments.add(deployment);
		}
		if (!descriptors.isEmpty()) {
			taskDescriptorsList.add(new GatewayDeploymentListEntity(descriptors, gatewayTaskDeployments));
		}
		for (GatewayDeploymentListEntity gatewayDeploymentListEntity : taskDescriptorsList) {
			gateway.submitTaskList(gatewayDeploymentListEntity.getDescriptorList(), getRpcTimeout())
				.whenCompleteAsync((acknowledge, failure) -> {
						if (null == failure) {
							for (GatewayTaskDeployment deployment : gatewayDeploymentListEntity.getGatewayTaskDeploymentList()) {
								Execution execution = deployment.getExecution();
								if (deployment.isUpdateConsumers()) {
									for (IntermediateResultPartition partition : execution.getVertex().getProducedPartitions().values()) {
										if (partition.getIntermediateResult().getResultType().isPipelined()) {
											for (ExecutionVertexID consumerVertexId : partition.getConsumers().get(0).getVertices()) {
												final ExecutionVertex consumerVertex = execution.getVertex().getExecutionGraph().getVertex(consumerVertexId);
												final Execution consumer = consumerVertex.getCurrentExecutionAttempt();
												final ExecutionState consumerState = consumer.getState();

												final PartitionInfo partitionInfo = Execution.createPartitionInfo(partition);

												if (consumerState == RUNNING) {
													consumer.sendUpdatePartitionInfoRpcCall(Collections.singleton(partitionInfo));
												} else {
													consumerVertex.cachePartitionInfo(partitionInfo);
												}
											}
										}
									}
								}
							}
						} else {
							for (GatewayTaskDeployment deployment : entry.getValue()) {
								Execution execution = deployment.getExecution();
								if (failure instanceof TimeoutException) {
									String taskname = execution.getVertex().getTaskNameWithSubtaskIndex() + " (" + execution.getAttemptId() + ')';

									execution.markFailed(new Exception(
										"Cannot deploy task " + taskname + " - TaskManager (" + execution.getAssignedResourceLocation()
											+ ") not responding after a rpcTimeout of " + execution.getRpcTimeout(), failure));
								} else {
									execution.markFailed(failure);
								}
							}
						}
					},
					getMainThreadExecutor());
		}
	}

	private static void propagateIfNonNull(final Throwable throwable) {
		if (throwable != null) {
			throw new CompletionException(throwable);
		}
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

		return (logicalSlot, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.debug("Refusing to assign slot to execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				releaseSlotIfPresent(logicalSlot);
				return null;
			}

			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption().sendScheduleOrUpdateConsumerMessage();
				executionVertex
					.getExecution(deploymentHandle.getDeploymentOption().isDeployCopy())
					.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
				executionVertex.getExecution(deploymentHandle.getDeploymentOption().isDeployCopy())
						.tryAssignResource(logicalSlot);
			} else {
				handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
			}
			return null;
		};
	}

	private void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
		if (logicalSlot != null) {
			logicalSlot.releaseSlot(null);
		}
	}

	private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
	}

	private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
		final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
		if (strippedThrowable instanceof TimeoutException) {
			return new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources.", failure);
		} else {
			return failure;
		}
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

		return (ignored, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.info("Refusing to deploy execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				return null;
			}

			if (throwable == null) {
				deployTaskSafe(executionVertexId, deploymentHandle.getDeploymentOption());
			} else {
				handleTaskDeploymentFailure(executionVertexId, throwable);
			}
			return null;
		};
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(
			final DeploymentHandle deploymentHandle,
			final GatewayDeploymentManager gatewayDeploymentManager) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

		return (ignored, throwable) -> {
			if (executionVertexVersioner.isModified(requiredVertexVersion)) {
				log.info("Refusing to deploy execution vertex {} because this deployment was " +
					"superseded by another deployment", executionVertexId);
				return null;
			}

			if (throwable == null) {
				deployTaskSafe(executionVertexId, deploymentHandle.getDeploymentOption(), gatewayDeploymentManager);
			} else {
				handleTaskDeploymentFailure(executionVertexId, throwable);
			}
			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexID executionVertexId, final DeploymentOption deploymentOption) {
		try {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			executionVertexOperations.deploy(executionVertex, deploymentOption);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}

	private void deployTaskSafe(
			final ExecutionVertexID executionVertexId,
			final DeploymentOption deploymentOption,
			final GatewayDeploymentManager gatewayDeploymentManager) {
		try {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			executionVertexOperations.deploy(executionVertex, deploymentOption, gatewayDeploymentManager);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}

	private void notifyCoordinatorOfCancellation(ExecutionVertex vertex) {
		// this method makes a best effort to filter out duplicate notifications, meaning cases where
		// the coordinator was already notified for that specific task
		// we don't notify if the task is already FAILED, CANCELLING, or CANCELED

		final ExecutionState currentState = vertex.getExecutionState();
		if (currentState == ExecutionState.FAILED ||
				currentState == ExecutionState.CANCELING ||
				currentState == ExecutionState.CANCELED) {
			return;
		}

		for (OperatorCoordinator coordinator : vertex.getJobVertex().getOperatorCoordinators()) {
			coordinator.subtaskFailed(vertex.getParallelSubtaskIndex(), null);
		}
	}
}
