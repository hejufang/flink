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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.ExecutionInfo;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several times, each of
 * which time it spawns an {@link Execution}.
 */
public class ExecutionVertex implements AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {

	private static final Logger LOG = ExecutionGraph.LOG;

	public static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

	// --------------------------------------------------------------------------------------------

	private final ExecutionJobVertex jobVertex;

	private final Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions;

	private final ExecutionEdge[][] inputEdges;

	private final int subTaskIndex;

	private final ExecutionVertexID executionVertexId;

	private final EvictingBoundedList<ArchivedExecution> priorExecutions;

	private final Time timeout;

	/** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
	private final String taskNameWithSubtask;

	private volatile CoLocationConstraint locationConstraint;

	/** The current or latest execution attempt of this vertex's task. */
	private volatile Execution mainExecution;	// this field must never be null

	private List<Execution> copyExecutions;

	private final ArrayList<InputSplit> totalInputSplits;

	private int attempt;

	// --------------------------------------------------------------------------------------------

	/**
	 * Convenience constructor for tests. Sets various fields to default values.
	 */
	@VisibleForTesting
	ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout) {

		this(
			jobVertex,
			subTaskIndex,
			producedDataSets,
			timeout,
			1L,
			System.currentTimeMillis(),
			JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE.defaultValue());
	}

	/**
	 * Creates an ExecutionVertex.
	 *
	 * @param timeout
	 *            The RPC timeout to use for deploy / cancel calls
	 * @param initialGlobalModVersion
	 *            The global modification version to initialize the first Execution with.
	 * @param createTimestamp
	 *            The timestamp for the vertex creation, used to initialize the first Execution with.
	 * @param maxPriorExecutionHistoryLength
	 *            The number of prior Executions (= execution attempts) to keep.
	 */
	public ExecutionVertex(
			ExecutionJobVertex jobVertex,
			int subTaskIndex,
			IntermediateResult[] producedDataSets,
			Time timeout,
			long initialGlobalModVersion,
			long createTimestamp,
			int maxPriorExecutionHistoryLength) {

		this.jobVertex = jobVertex;
		this.subTaskIndex = subTaskIndex;
		this.executionVertexId = new ExecutionVertexID(jobVertex.getJobVertexId(), subTaskIndex);
		this.taskNameWithSubtask = String.format("%s (%d/%d)",
				jobVertex.getJobVertex().getName(), subTaskIndex + 1, jobVertex.getParallelism());

		this.resultPartitions = new LinkedHashMap<>(producedDataSets.length, 1);

		for (IntermediateResult result : producedDataSets) {
			IntermediateResultPartition irp = new IntermediateResultPartition(result, this, subTaskIndex);
			result.setPartition(subTaskIndex, irp);

			resultPartitions.put(irp.getPartitionId(), irp);
		}

		this.inputEdges = new ExecutionEdge[jobVertex.getJobVertex().getInputs().size()][];

		this.priorExecutions = new EvictingBoundedList<>(maxPriorExecutionHistoryLength);

		this.attempt = 0;
		this.mainExecution = new Execution(
			getExecutionGraph().getFutureExecutor(),
			this,
			attempt,
			initialGlobalModVersion,
			createTimestamp,
			timeout);
		this.copyExecutions = new ArrayList<>();

		// create a co-location scheduling hint, if necessary
		CoLocationGroup clg = jobVertex.getCoLocationGroup();
		if (clg != null) {
			this.locationConstraint = clg.getLocationConstraint(subTaskIndex);
		}
		else {
			this.locationConstraint = null;
		}

		getExecutionGraph().registerExecution(mainExecution);

		this.timeout = timeout;
		this.totalInputSplits = new ArrayList<>();
	}


	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public JobID getJobId() {
		return this.jobVertex.getJobId();
	}

	public ExecutionJobVertex getJobVertex() {
		return jobVertex;
	}

	public JobVertexID getJobvertexId() {
		return this.jobVertex.getJobVertexId();
	}

	public String getTaskName() {
		return this.jobVertex.getJobVertex().getName();
	}

	/**
	 * Creates a simple name representation in the style 'taskname (x/y)', where
	 * 'taskname' is the name as returned by {@link #getTaskName()}, 'x' is the parallel
	 * subtask index as returned by {@link #getParallelSubtaskIndex()}{@code + 1}, and 'y' is the total
	 * number of tasks, as returned by {@link #getTotalNumberOfParallelSubtasks()}.
	 *
	 * @return A simple name representation in the form 'myTask (2/7)'
	 */
	@Override
	public String getTaskNameWithSubtaskIndex() {
		return this.taskNameWithSubtask;
	}

	public int getTotalNumberOfParallelSubtasks() {
		return this.jobVertex.getParallelism();
	}

	public int getMaxParallelism() {
		return this.jobVertex.getMaxParallelism();
	}

	public ResourceProfile getResourceProfile() {
		return this.jobVertex.getResourceProfile();
	}

	@Override
	public int getParallelSubtaskIndex() {
		return this.subTaskIndex;
	}

	public ExecutionVertexID getID() {
		return executionVertexId;
	}

	public int getNumberOfInputs() {
		return this.inputEdges.length;
	}

	public ExecutionEdge[] getInputEdges(int input) {
		if (input < 0 || input >= inputEdges.length) {
			throw new IllegalArgumentException(String.format("Input %d is out of range [0..%d)", input, inputEdges.length));
		}
		return inputEdges[input];
	}

	public ExecutionEdge[][] getAllInputEdges() {
		return inputEdges;
	}

	public CoLocationConstraint getLocationConstraint() {
		return locationConstraint;
	}

	public InputSplit getNextInputSplit(String host) {
		final int taskId = getParallelSubtaskIndex();
		synchronized (totalInputSplits) {
			final InputSplit nextInputSplit = jobVertex.getSplitAssigner().getNextInputSplit(host, taskId);
			if (nextInputSplit != null) {
				totalInputSplits.add(nextInputSplit);
			}
			return nextInputSplit;
		}
	}

	@Override
	public Execution getMainExecution() {
		return mainExecution;
	}

	@Override
	public List<Execution> getCopyExecutions() {
		return copyExecutions;
	}

	public Execution getCopyExecution() {
		return copyExecutions.get(0);
	}

	public ExecutionInfo getFinishedAttempt() {
		return new ExecutionInfo(subTaskIndex, getConsumableExecution(false, false).getAttemptNumber());
	}

	@Override
	public ExecutionState getExecutionState() {
		return mainExecution.getState();
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return mainExecution.getStateTimestamp(state);
	}

	/**
	 * Called when initializing InputGateDeploymentDescriptor of downstream exeuutions.
	 */
	public Execution getConsumableExecution(boolean isPipelined, boolean isCopy) {
		if (!isPipelined) {
			for (Execution exec : copyExecutions) {
				if (exec.getState().isFinished()) {
					return exec;
				}
			}
			return mainExecution;
		} else {
			return isCopy ? getCopyExecution() : mainExecution;
		}
	}

	@Nullable
	@Override
	public ArchivedExecution getPriorExecutionAttempt(int attemptNumber) {
		synchronized (priorExecutions) {
			if (attemptNumber >= 0 && attemptNumber < priorExecutions.size()) {
				return priorExecutions.get(attemptNumber);
			} else {
				throw new IllegalArgumentException("attempt does not exist");
			}
		}
	}

	public ArchivedExecution getLatestPriorExecution() {
		synchronized (priorExecutions) {
			final int size = priorExecutions.size();
			if (size > 0) {
				return priorExecutions.get(size - 1);
			}
			else {
				return null;
			}
		}
	}

	public ArrayList<InputSplit> getTotalInputSplits() {
		return totalInputSplits;
	}

	/**
	 * Gets the location where the latest completed/canceled/failed execution of the vertex's
	 * task happened.
	 *
	 * @return The latest prior execution location, or null, if there is none, yet.
	 */
	public TaskManagerLocation getLatestPriorLocation() {
		ArchivedExecution latestPriorExecution = getLatestPriorExecution();
		return latestPriorExecution != null ? latestPriorExecution.getAssignedResourceLocation() : null;
	}

	public AllocationID getLatestPriorAllocation() {
		ArchivedExecution latestPriorExecution = getLatestPriorExecution();
		return latestPriorExecution != null ? latestPriorExecution.getAssignedAllocationID() : null;
	}

	EvictingBoundedList<ArchivedExecution> getCopyOfPriorExecutionsList() {
		synchronized (priorExecutions) {
			return new EvictingBoundedList<>(priorExecutions);
		}
	}

	public ExecutionGraph getExecutionGraph() {
		return this.jobVertex.getGraph();
	}

	public Map<IntermediateResultPartitionID, IntermediateResultPartition> getProducedPartitions() {
		return resultPartitions;
	}

	public InputDependencyConstraint getInputDependencyConstraint() {
		return getJobVertex().getInputDependencyConstraint();
	}

	// --------------------------------------------------------------------------------------------
	//  Graph building
	// --------------------------------------------------------------------------------------------

	public void connectSource(int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber) {

		final DistributionPattern pattern = edge.getDistributionPattern();
		final IntermediateResultPartition[] sourcePartitions = source.getPartitions();

		ExecutionEdge[] edges;

		switch (pattern) {
			case POINTWISE:
				edges = connectPointwise(sourcePartitions, inputNumber);
				break;

			case ALL_TO_ALL:
				edges = connectAllToAll(sourcePartitions, inputNumber);
				break;

			default:
				throw new RuntimeException("Unrecognized distribution pattern.");

		}

		inputEdges[inputNumber] = edges;

		// add the consumers to the source
		// for now (until the receiver initiated handshake is in place), we need to register the
		// edges as the execution graph
		for (ExecutionEdge ee : edges) {
			ee.getSource().addConsumer(ee, consumerNumber);
		}
	}

	private ExecutionEdge[] connectAllToAll(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		ExecutionEdge[] edges = new ExecutionEdge[sourcePartitions.length];

		for (int i = 0; i < sourcePartitions.length; i++) {
			IntermediateResultPartition irp = sourcePartitions[i];
			edges[i] = new ExecutionEdge(irp, this, inputNumber);
		}

		return edges;
	}

	private ExecutionEdge[] connectPointwise(IntermediateResultPartition[] sourcePartitions, int inputNumber) {
		final int numSources = sourcePartitions.length;
		final int parallelism = getTotalNumberOfParallelSubtasks();

		// simple case same number of sources as targets
		if (numSources == parallelism) {
			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[subTaskIndex], this, inputNumber) };
		}
		else if (numSources < parallelism) {

			int sourcePartition;

			// check if the pattern is regular or irregular
			// we use int arithmetics for regular, and floating point with rounding for irregular
			if (parallelism % numSources == 0) {
				// same number of targets per source
				int factor = parallelism / numSources;
				sourcePartition = subTaskIndex / factor;
			}
			else {
				// different number of targets per source
				float factor = ((float) parallelism) / numSources;
				sourcePartition = (int) (subTaskIndex / factor);
			}

			return new ExecutionEdge[] { new ExecutionEdge(sourcePartitions[sourcePartition], this, inputNumber) };
		}
		else {
			if (numSources % parallelism == 0) {
				// same number of targets per source
				int factor = numSources / parallelism;
				int startIndex = subTaskIndex * factor;

				ExecutionEdge[] edges = new ExecutionEdge[factor];
				for (int i = 0; i < factor; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[startIndex + i], this, inputNumber);
				}
				return edges;
			}
			else {
				float factor = ((float) numSources) / parallelism;

				int start = (int) (subTaskIndex * factor);
				int end = (subTaskIndex == getTotalNumberOfParallelSubtasks() - 1) ?
						sourcePartitions.length :
						(int) ((subTaskIndex + 1) * factor);

				ExecutionEdge[] edges = new ExecutionEdge[end - start];
				for (int i = 0; i < edges.length; i++) {
					edges[i] = new ExecutionEdge(sourcePartitions[start + i], this, inputNumber);
				}

				return edges;
			}
		}
	}

	/**
	 * Gets the overall preferred execution location for this vertex's current execution.
	 * The preference is determined as follows:
	 *
	 * <ol>
	 *     <li>If the task execution has state to load (from a checkpoint), then the location preference
	 *         is the location of the previous execution (if there is a previous execution attempt).
	 *     <li>If the task execution has no state or no previous location, then the location preference
	 *         is based on the task's inputs.
	 * </ol>
	 *
	 * <p>These rules should result in the following behavior:
	 *
	 * <ul>
	 *     <li>Stateless tasks are always scheduled based on co-location with inputs.
	 *     <li>Stateful tasks are on their initial attempt executed based on co-location with inputs.
	 *     <li>Repeated executions of stateful tasks try to co-locate the execution with its state.
	 * </ul>
	 *
	 * @see #getPreferredLocationsBasedOnState()
	 * @see #getPreferredLocationsBasedOnInputs()
	 *
	 * @return The preferred execution locations for the execution attempt.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocations() {
		Collection<CompletableFuture<TaskManagerLocation>> basedOnState = getPreferredLocationsBasedOnState();
		return basedOnState != null ? basedOnState : getPreferredLocationsBasedOnInputs();
	}

	/**
	 * Gets the preferred location to execute the current task execution attempt, based on the state
	 * that the execution attempt will resume.
	 *
	 * @return A size-one collection with the location preference, or null, if there is no
	 *         location preference based on the state.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnState() {
		TaskManagerLocation priorLocation;
		if (mainExecution.getTaskRestore() != null && (priorLocation = getLatestPriorLocation()) != null) {
			return Collections.singleton(CompletableFuture.completedFuture(priorLocation));
		}
		else {
			return null;
		}
	}

	/**
	 * Gets the location preferences of the vertex's current task execution, as determined by the locations
	 * of the predecessors from which it receives input data.
	 * If there are more than MAX_DISTINCT_LOCATIONS_TO_CONSIDER different locations of source data, this
	 * method returns {@code null} to indicate no location preference.
	 *
	 * @return The preferred locations based in input streams, or an empty iterable,
	 *         if there is no input-based preference.
	 */
	public Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs() {
		// otherwise, base the preferred locations on the input connections
		if (inputEdges == null) {
			return Collections.emptySet();
		}
		else {
			Set<CompletableFuture<TaskManagerLocation>> locations = new HashSet<>(getTotalNumberOfParallelSubtasks());
			Set<CompletableFuture<TaskManagerLocation>> inputLocations = new HashSet<>(getTotalNumberOfParallelSubtasks());

			// go over all inputs
			for (int i = 0; i < inputEdges.length; i++) {
				inputLocations.clear();
				ExecutionEdge[] sources = inputEdges[i];
				if (sources != null) {
					// go over all input sources
					for (int k = 0; k < sources.length; k++) {
						// look-up assigned slot of input source
						CompletableFuture<TaskManagerLocation> locationFuture = sources[k].getSource().getProducer().getMainExecution().getTaskManagerLocationFuture();
						// add input location
						inputLocations.add(locationFuture);
						// inputs which have too many distinct sources are not considered
						if (inputLocations.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
							inputLocations.clear();
							break;
						}
					}
				}
				// keep the locations of the input with the least preferred locations
				if (locations.isEmpty() || // nothing assigned yet
						(!inputLocations.isEmpty() && inputLocations.size() < locations.size())) {
					// current input has fewer preferred locations
					locations.clear();
					locations.addAll(inputLocations);
				}
			}

			return locations.isEmpty() ? Collections.emptyList() : locations;
		}
	}

	// --------------------------------------------------------------------------------------------
	//   Actions
	// --------------------------------------------------------------------------------------------

	/**
	 * Archives the current Execution and creates a new Execution for this vertex.
	 *
	 * <p>This method atomically checks if the ExecutionGraph is still of an expected
	 * global mod. version and replaces the execution if that is the case. If the ExecutionGraph
	 * has increased its global mod. version in the meantime, this operation fails.
	 *
	 * <p>This mechanism can be used to prevent conflicts between various concurrent recovery and
	 * reconfiguration actions in a similar way as "optimistic concurrency control".
	 *
	 * @param timestamp
	 *             The creation timestamp for the new Execution
	 * @param originatingGlobalModVersion
	 *
	 * @return Returns the new created Execution.
	 *
	 * @throws GlobalModVersionMismatch Thrown, if the execution graph has a new global mod
	 *                                  version than the one passed to this message.
	 */
	public Execution resetForNewExecution(final long timestamp, final long originatingGlobalModVersion)
			throws GlobalModVersionMismatch {
		LOG.debug("Resetting execution vertex {} for new execution.", getTaskNameWithSubtaskIndex());

		synchronized (priorExecutions) {
			// check if another global modification has been triggered since the
			// action that originally caused this reset/restart happened
			final long actualModVersion = getExecutionGraph().getGlobalModVersion();
			if (actualModVersion > originatingGlobalModVersion) {
				// global change happened since, reject this action
				throw new GlobalModVersionMismatch(originatingGlobalModVersion, actualModVersion);
			}

			final List<Execution> unTerminalExecutions = new ArrayList<>();
			final List<Execution> terminalExecutions = new ArrayList<>();

			(mainExecution.getState().isTerminal()? terminalExecutions : unTerminalExecutions).add(mainExecution);
			for (Execution exec : copyExecutions) {
				(exec.getState().isTerminal()? terminalExecutions : unTerminalExecutions).add(exec);
			}

			if (terminalExecutions.size() == 0) {
				throw new IllegalStateException("Cannot reset a vertex that is in non-terminal state.");
			}

			// cancel unterminal execution
			for (Execution execution : unTerminalExecutions) {
				execution.cancel();
				execution.getReleaseFuture().whenComplete((ignore, throwable) -> {
					if (throwable != null) {
						LOG.error("Fail to cancel unterminal execution", throwable);
					} else {
						LOG.info("Successfully cancel unterminal execution.");
					}
					priorExecutions.add(execution.archive());
				});
			}

			// deal with terminal execution
			for (Execution execution : terminalExecutions) {
				if (execution.getState().isFinished()) {
					execution.handlePartitionCleanup(false, true);
					getExecutionGraph().getPartitionReleaseStrategy().vertexUnfinished(executionVertexId);
				}
				priorExecutions.add(execution.archive());
			}

			this.attempt += 1;
			final Execution newExecution = new Execution(
					getExecutionGraph().getFutureExecutor(),
					this,
					this.attempt,
					originatingGlobalModVersion,
					timestamp,
					timeout);

			mainExecution = newExecution;
			copyExecutions = new ArrayList<>();

			// reset vertex in SpeculationStrategy
			getExecutionGraph().getSpeculationStrategy().resetVertex(executionVertexId);

			synchronized (totalInputSplits) {
				InputSplitAssigner assigner = jobVertex.getSplitAssigner();
				if (assigner != null) {
					assigner.returnInputSplit(totalInputSplits, getParallelSubtaskIndex());
					totalInputSplits.clear();
				}
			}

			CoLocationGroup grp = jobVertex.getCoLocationGroup();
			if (grp != null) {
				locationConstraint = grp.getLocationConstraint(subTaskIndex);
			}

			// register this execution at the execution graph, to receive call backs
			getExecutionGraph().registerExecution(newExecution);

			// if the execution was 'FINISHED' before, tell the ExecutionGraph that
			// we take one step back on the road to reaching global FINISHED
			if (terminalExecutions.stream().anyMatch(x -> x.getState().isFinished())) {
				getExecutionGraph().vertexUnFinished();
			}

			// reset the intermediate results
			for (IntermediateResultPartition resultPartition : resultPartitions.values()) {
				resultPartition.resetForNewExecution();
			}

			return mainExecution;
		}
	}

	/**
	 * Schedules the current execution of this ExecutionVertex.
	 *
	 * @param slotProviderStrategy to allocate the slots from
	 * @param locationPreferenceConstraint constraint for the location preferences
	 * @param allPreviousExecutionGraphAllocationIds set with all previous allocation ids in the job graph.
	 *                                                 Can be empty if the allocation ids are not required for scheduling.
	 * @param isCopy whether schedule a copy execution.
	 * @return Future which is completed once the execution is deployed. The future
	 * can also completed exceptionally.
	 */
	public CompletableFuture<Void> scheduleForExecution(
			SlotProviderStrategy slotProviderStrategy,
			LocationPreferenceConstraint locationPreferenceConstraint,
			@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds,
			boolean isCopy) {
		if (isCopy) {
			return scheduleCopyExecution();
		} else {
			return mainExecution.scheduleForExecution(slotProviderStrategy, locationPreferenceConstraint, allPreviousExecutionGraphAllocationIds);
		}
	}

	public CompletableFuture<Void> scheduleForExecution(
		SlotProviderStrategy slotProviderStrategy,
		LocationPreferenceConstraint locationPreferenceConstraint,
		@Nonnull Set<AllocationID> allPreviousExecutionGraphAllocationIds) {
		return scheduleForExecution(
			slotProviderStrategy,
			locationPreferenceConstraint,
			allPreviousExecutionGraphAllocationIds,
			false);
	}

	@VisibleForTesting
	public void deployToSlot(LogicalSlot slot) throws JobException {
		if (mainExecution.tryAssignResource(slot)) {
			mainExecution.deploy();
		} else {
			throw new IllegalStateException("Could not assign resource " + slot + " to current execution " +
					mainExecution + '.');
		}
	}

	public Execution getAccumulatorExecution() {
		if (copyExecutions.size() > 0) {
			if (getCopyExecution().getState().isFinished()) {
				return getCopyExecution();
			} else {
				return mainExecution;
			}
		} else {
			return mainExecution;
		}
	}

	private void cancelMainExecution() {
		mainExecution.cancel();
		mainExecution.getReleaseFuture().whenComplete((ignore, throwable) -> {
			LOG.info("Successfully cancel main execution in {}.", mainExecution.getVertexWithAttempt());
		});
	}

	private void cancelCopyExecutions() {
		if (copyExecutions.size() > 0) {
			List<CompletableFuture<?>> cancelFutures = new ArrayList<>();
			for (Execution exec : copyExecutions) {
				exec.cancel();
				cancelFutures.add(exec.getReleaseFuture());
			}
			FutureUtils.combineAll(cancelFutures).whenComplete((ignore, throwable) -> {
				LOG.info("Successfully cancel copy executions in {}.", getTaskNameWithSubtaskIndex());
			});
		}
	}

	/**
	 * Cancels this ExecutionVertex.
	 *
	 * @return A future that completes once the execution has reached its final state.
	 */
	public CompletableFuture<?> cancel() {
		// to avoid any case of mixup in the presence of concurrent calls,
		// we copy a reference to the stack to make sure both calls go to the same Execution
		List<CompletableFuture<?>> cancelFutures = new ArrayList<>();
		mainExecution.cancel();
		cancelFutures.add(mainExecution.getReleaseFuture());

		for (Execution exec : copyExecutions) {
			exec.cancel();
			cancelFutures.add(exec.getReleaseFuture());
		}
		return FutureUtils.combineAll(cancelFutures);
	}

	public CompletableFuture<?> suspend() {
		List<CompletableFuture<?>> suspendFutures = new ArrayList<>();
		suspendFutures.add(mainExecution.suspend());

		for (Execution exec : copyExecutions) {
			suspendFutures.add(exec.suspend());
		}
		return FutureUtils.combineAll(suspendFutures);
	}

	public void fail(Throwable t) {
		mainExecution.fail(t);
		for (Execution exec : copyExecutions) {
			exec.fail(t);
		}
	}

	List<List<ExecutionEdge>> getAllConsumers() {
		final List<List<ExecutionEdge>> result = new ArrayList<>();
		for (IntermediateResultPartition partition : resultPartitions.values()) {
			if (partition.getIntermediateResult().getResultType().isPipelined()) {
				result.add(partition.getConsumers().get(0));
			}
		}

		return result;
	}

	/**
	 * Schedules or updates the consumer tasks of the result partition with the given ID.
	 */
	void scheduleOrUpdateConsumers(Execution execution, ResultPartitionID partitionId) {

		// Abort this request if there was a concurrent reset
		if (!partitionId.getProducerId().equals(execution.getAttemptId())) {
			return;
		}

		final IntermediateResultPartition partition = resultPartitions.get(partitionId.getPartitionId());

		if (partition == null) {
			throw new IllegalStateException("Unknown partition " + partitionId + ".");
		}

		partition.markDataProduced();

		if (partition.getIntermediateResult().getResultType().isPipelined()) {
			// Schedule or update receivers of this partition
			execution.scheduleOrUpdateConsumers(partition.getConsumers(), true);
		}
		else {
			throw new IllegalArgumentException("ScheduleOrUpdateConsumers msg is only valid for" +
					"pipelined partitions.");
		}
	}

	void cachePartitionInfo(PartitionInfo partitionInfo){
		getMainExecution().cachePartitionInfo(partitionInfo);
	}

	/**
	 * Returns all blocking result partitions whose receivers can be scheduled/updated.
	 */
	List<IntermediateResultPartition> finishAllBlockingPartitions() {
		List<IntermediateResultPartition> finishedBlockingPartitions = null;

		for (IntermediateResultPartition partition : resultPartitions.values()) {
			if (partition.getResultType().isBlocking() && partition.markFinished()) {
				if (finishedBlockingPartitions == null) {
					finishedBlockingPartitions = new LinkedList<IntermediateResultPartition>();
				}

				finishedBlockingPartitions.add(partition);
			}
		}

		if (finishedBlockingPartitions == null) {
			return Collections.emptyList();
		}
		else {
			return finishedBlockingPartitions;
		}
	}

	/**
	 * Check whether the InputDependencyConstraint is satisfied for this vertex.
	 *
	 * @return whether the input constraint is satisfied
	 */
	boolean checkInputDependencyConstraints() {
		if (getInputDependencyConstraint() == InputDependencyConstraint.ANY) {
			// InputDependencyConstraint == ANY
			return IntStream.range(0, inputEdges.length).anyMatch(this::isInputConsumable);
		} else {
			// InputDependencyConstraint == ALL
			return IntStream.range(0, inputEdges.length).allMatch(this::isInputConsumable);
		}
	}

	/**
	 * Get whether an input of the vertex is consumable.
	 * An input is consumable when when any partition in it is consumable.
	 *
	 * Note that a BLOCKING result partition is only consumable when all partitions in the result are FINISHED.
	 *
	 * @return whether the input is consumable
	 */
	boolean isInputConsumable(int inputNumber) {
		return Arrays.stream(inputEdges[inputNumber]).map(ExecutionEdge::getSource).anyMatch(
				IntermediateResultPartition::isConsumable);
	}

	// --------------------------------------------------------------------------------------------
	//   Notifications from the Execution Attempt
	// --------------------------------------------------------------------------------------------

	void executionFinished(Execution execution) {
		if (execution.isCopy()) {
			cancelMainExecution();
		} else {
			cancelCopyExecutions();
		}
		getExecutionGraph().vertexFinished();
	}

	// --------------------------------------------------------------------------------------------
	//   Miscellaneous
	// --------------------------------------------------------------------------------------------

	/**
	 * Simply forward this notification.
	 */
	void notifyStateTransition(Execution execution, ExecutionState newState, Throwable error) {
		// only forward this notification if the execution is still the current execution
		// otherwise we have an outdated execution
		if (mainExecution == execution || getCopyExecution() == execution) {
			getExecutionGraph().notifyExecutionChange(execution, newState, error);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return getTaskNameWithSubtaskIndex();
	}

	@Override
	public ArchivedExecutionVertex archive() {
		return new ArchivedExecutionVertex(this);
	}

	public CompletableFuture<Void> scheduleCopyExecution() {
		if (copyExecutions.size() > 0) {
			LOG.info("Only one copy execution is allowed in {}.", getTaskNameWithSubtaskIndex());
			return CompletableFuture.completedFuture(null);
		}
		final long timestamp = System.currentTimeMillis();
		this.attempt += 1;
		final Execution copyExecution = new Execution(
				getExecutionGraph().getFutureExecutor(),
				this,
				this.attempt,
				getExecutionGraph().getGlobalModVersion(),
				timestamp,
				timeout, true);

		this.copyExecutions.add(copyExecution);
		getExecutionGraph().registerExecution(copyExecution);

		LOG.info("Create copy execution {} attempt = {}", taskNameWithSubtask, attempt);
		CompletableFuture<Void> future = copyExecution.scheduleForExecution();

		future.whenComplete((ignore, failure) -> {
			// failure is handled in Execution, ignore here
			LOG.info("Successfully deploy copy execution {}, atttempt = {}", taskNameWithSubtask, attempt);
		});

		return future;
	}

	public Map<String, List<Integer>> getInputSubTasks() {

		Map<IntermediateDataSetID, DistributionPattern> distributionPatternMap =
			Optional.ofNullable(this.getJobVertex()).map(ExecutionJobVertex::getJobVertex).map(JobVertex::getInputs).orElse(new ArrayList<>()).stream().collect(
				Collectors.toMap(JobEdge::getSourceId, JobEdge::getDistributionPattern)
			);
		Map<String, List<Integer>> result = new HashMap<>();
		for (ExecutionEdge[] executionEdges : this.getAllInputEdges()) {
			if (executionEdges.length == 0) {
				continue;
			}

			IntermediateResultPartition source = executionEdges[0].getSource();
			DistributionPattern distributionPattern = distributionPatternMap.get(source.getIntermediateResult().getId());
			if (distributionPattern.equals(DistributionPattern.ALL_TO_ALL)) {
				result.put(source.getProducer().getTaskName(), Lists.newArrayList());
				continue;
			}

			//sourceIds contains two elements, sourceStartSubtaskIndex and sourceEndSubtaskIndex.
			List<Integer> sourceIds = new ArrayList<>(2);
			result.put(source.getProducer().getTaskName(), sourceIds);

			sourceIds.add(executionEdges[0].getSource().getProducer().getParallelSubtaskIndex());
			if (executionEdges.length > 1) {
				sourceIds.add(executionEdges[executionEdges.length - 1].getSource().getProducer().getParallelSubtaskIndex());
			}
		}

		return result;
	}

	public Map<String, List<Integer>> getOutputSubTasks() {

		Map<String, List<Integer>> result = new HashMap<>();
		for (IntermediateResultPartition intermediateResultPartition : this.resultPartitions.values()) {
			if (CollectionUtil.isNullOrEmpty(intermediateResultPartition.getConsumers())) {
				continue;
			}

			// NOTE: currently we support only one consumer per result.
			List<ExecutionEdge> executionEdges = intermediateResultPartition.getConsumers().get(0);

			ExecutionVertex target = executionEdges.get(0).getTarget();

			//targetIds contains two elements, targetStartSubtaskIndex and targetEndSubtaskIndex.
			List<Integer> targetIds = new ArrayList<>(2);
			result.put(target.getTaskName(), targetIds);

			targetIds.add(executionEdges.get(0).getTarget().getParallelSubtaskIndex());
			if (executionEdges.size() > 1) {
				targetIds.add(executionEdges.get(executionEdges.size() - 1).getTarget().getParallelSubtaskIndex());
			}
		}

		return result;
	}

}
