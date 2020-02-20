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
package org.apache.flink.runtime.executiongraph.speculation;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.MedianHeap;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default speculation strategy
 */
public class DefaultSpeculationStrategy extends SpeculationStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultSpeculationStrategy.class);

	private static final long MIN_TIME_TO_SPECULATION = 60_000;

	private static final int MIN_CREDITS = 3;

	private final long interval;

	private final double quantile;

	private final double multiplier;

	private final double creditPercentage;

	// 调用推测执行的额度
	private int credits;

	private final Map<ExecutionJobVertex, Set<Execution>> finishedExecutions;
	private final Map<ExecutionJobVertex, Set<Execution>> currentExecutions;
	private final Map<ExecutionJobVertex, MedianHeap> timeCostMedianHeap;
	private final Set<SpeculationRegion> runningRegions;

	private Map<ExecutionVertexID, SpeculationRegion> vertexToRegion;

	private ComponentMainThreadExecutor jobMasterMainThreadExecutor;

	private ExecutionGraph executionGraph;

	private int numberOfSpeculation = 0;

	private int numberOfSuccessfulSpeculation = 0;

	public DefaultSpeculationStrategy(ExecutionGraph executionGraph,
	                                  long interval,
	                                  double quantile,
	                                  double multiplier,
	                                  double creditPercentage) {
		super();
		this.executionGraph = executionGraph;
		this.interval = interval;
		this.quantile = quantile;
		this.multiplier = multiplier;
		this.creditPercentage = creditPercentage;
		this.vertexToRegion = new HashMap<>();
		this.finishedExecutions = new HashMap<>();
		this.currentExecutions = new HashMap<>();
		this.timeCostMedianHeap = new HashMap<>();
		this.runningRegions = new HashSet<>();
		this.credits = 0;
	}

	public int getQuantileNum(ExecutionJobVertex executionJobVertex) {
		return (int) (executionJobVertex.getTaskVertices().length * quantile);
	}

	public int getCredits() {
		return credits;
	}

	@Override
	public SpeculationRegion getSpeculationRegion(ExecutionVertex v) {
		return vertexToRegion.get(v.getID());
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// According to AdaptedRestartPipelinedRegionStrategyNG, notifyNewVertices will only be called once.
		checkState(vertexToRegion.size() == 0, "notifyNewVertices() must be called only once");

		// 计算推测执行的 Region
		Set<Set<FailoverVertex>> distinctRegions = executionGraph.getFailoverTopology().getDistinctRegions();
		for (Set<FailoverVertex> regionVertices : distinctRegions) {
			LOG.debug("Creating a speculation region with {} vertices.", regionVertices.size());
			final SpeculationRegion failoverRegion = new SpeculationRegion(regionVertices);
			for (FailoverVertex vertex : regionVertices) {
				vertexToRegion.put(vertex.getExecutionVertexID(), failoverRegion);
			}
		}

		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			finishedExecutions.put(ejv, new HashSet<>());
			timeCostMedianHeap.put(ejv, new MedianHeap());
		}

		int vertices = 0;
		for (final ExecutionJobVertex ejv : newJobVerticesTopological) {
			vertices += ejv.getParallelism();
		}
		this.credits = Math.max(MIN_CREDITS, (int) (vertices * creditPercentage));
	}

	@Override
	public void registerExecution(Execution execution) {
		if (execution.isCopy()) {
			credits--;
		}

		if (currentExecutions.containsKey(execution.getVertex().getJobVertex())) {
			currentExecutions.get(execution.getVertex().getJobVertex()).add(execution);
		} else {
			final Set<Execution> executions = new HashSet<>();
			executions.add(execution);
			currentExecutions.put(execution.getVertex().getJobVertex(), executions);
		}
	}

	@Override
	public void deregisterExecution(Execution execution) {
		if (execution.isCopy()) {
			credits++;
		}
		currentExecutions.get(execution.getVertex().getJobVertex()).remove(execution);
	}

	@Override
	public void onTaskSuccess(Execution execution) {
		assertRunningInJobMasterMainThread();

		final ExecutionJobVertex executionJobVertex = execution.getVertex().getJobVertex();
		Preconditions.checkArgument(finishedExecutions.containsKey(executionJobVertex), execution.getVertex().getTaskName() + " not exist in successfulExecutionVertexIDs!");

		if (execution.isCopy()) {
			numberOfSuccessfulSpeculation++;
		}

		finishedExecutions.get(executionJobVertex).add(execution);
		timeCostMedianHeap.get(executionJobVertex).insert(getRunningTime(execution));
	}

	@Override
	public void receiveRunningTaskMetrics(Execution execution, Accumulator taskMetric) {
		// TODO. speculate based on running tasks metrics if needed
		assertRunningInJobMasterMainThread();
	}

	@Override
	public void start(ComponentMainThreadExecutor mainThreadExecutor) {
		this.jobMasterMainThreadExecutor = mainThreadExecutor;
		jobMasterMainThreadExecutor.schedule(this, interval, TimeUnit.MILLISECONDS);
	}

	@Override
	public String getStrategyName() {
		return "Default SpeculationStrategy";
	}

	@Override
	public void run() {
		assertRunningInJobMasterMainThread();

		if (credits <= 0) {
			LOG.info("There is not enough credits for running speculation. Current credits = {}", credits);
		}

		int tempCredits = credits;
		boolean scheduled = false;

		final Set<SpeculationRegion> regions = checkSpeculationRegions(System.currentTimeMillis());

		for (final SpeculationRegion region : regions) {
			if (!runningRegions.contains(region)) {
				if (region.getConnectedVertices().size() <= tempCredits) {
					// every region has only one source vertex
					for (final FailoverVertex failoverVertex : region.getSourceVertices()) {
						// do not schedule if source execution vertex is finished
						final ExecutionVertex ev = getExecutionVertex(failoverVertex.getExecutionVertexID());
						if (!ev.getMainExecution().getState().isFinished()) {
							numberOfSpeculation++;
							ev.scheduleCopyExecution();
							scheduled = true;
						} else {
							LOG.info("Refuse to schedule copy execution because the source task is already finished.");
						}
					}
					if (scheduled) {
						runningRegions.add(region);
						tempCredits -= region.getConnectedVertices().size();
					}
				} else {
					LOG.debug("There is not enough credits for running speculation. Currently only {} credits left.", tempCredits);
				}
			}
		}
		jobMasterMainThreadExecutor.schedule(this, interval, TimeUnit.MILLISECONDS);
	}

	private long getRunningTime(Execution execution) {
		long finished = execution.getStateTimestamp(ExecutionState.FINISHED);
		return getRunningTime(execution, finished);
	}

	private long getRunningTime(Execution execution, long current) {
		long running = execution.getStateTimestamp(ExecutionState.RUNNING);
		return current - running;
	}

	private ExecutionVertex getExecutionVertex(final ExecutionVertexID vertexID) {
		return executionGraph.getAllVertices()
				.get(vertexID.getJobVertexId())
				.getTaskVertices()[vertexID.getSubtaskIndex()];
	}

	// 检查每个 JobVertex 返回需要推测的 region
	private Set<SpeculationRegion> checkSpeculationRegions(long currTimestamp) {
		assertRunningInJobMasterMainThread();

		final Set<SpeculationRegion> regions = new HashSet<>();

		for (Map.Entry<ExecutionJobVertex, Set<Execution>> entry : currentExecutions.entrySet()) {
			final ExecutionJobVertex executionJobVertex = entry.getKey();
			final Set<Execution> executions = entry.getValue();
			if (executions.size() > 0) {
				// 检查已完成的 task 是否符合推测执行的 quantile 参数要求
				if (finishedExecutions.get(executionJobVertex).size() > getQuantileNum(executionJobVertex)) {
					// 计算已完成 task 的时间中位数
					final double timeCostThreshold = Math.max(timeCostMedianHeap.get(executionJobVertex).median() * multiplier, MIN_TIME_TO_SPECULATION);
					for (final Execution execution : executions) {
						if (getRunningTime(execution, currTimestamp) > timeCostThreshold) {
							regions.add(vertexToRegion.get(execution.getVertex().getID()));
						}
					}
				}
			}
		}

		return regions;
	}

	@Override
	public void resetVertex(ExecutionVertexID evID) {
		final SpeculationRegion region = vertexToRegion.get(evID);
		if (region.resetVertex(evID)) {
			LOG.info("Reset and remove from running regions because all vertices are reset.");
			runningRegions.remove(region);
			region.reset();
		}
	}

	@Override
	public void reset() {
		for (final ExecutionJobVertex ejv : finishedExecutions.keySet()) {
			finishedExecutions.put(ejv, new HashSet<>());
			currentExecutions.put(ejv, new HashSet<>());
			timeCostMedianHeap.put(ejv, new MedianHeap());
		}
		for (final SpeculationRegion region : runningRegions) {
			region.reset();
		}
		runningRegions.clear();
	}

	private void assertRunningInJobMasterMainThread() {
		if (!(jobMasterMainThreadExecutor instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
			jobMasterMainThreadExecutor.assertRunningInMainThread();
		}
	}

	public void registerMetrics(MetricGroup metricGroup) {
		metricGroup.gauge("numberOfSpeculation", (Gauge<Integer>) () -> numberOfSpeculation);
		metricGroup.gauge("numberOfSuccessfulSpeculation", (Gauge<Integer>) () -> numberOfSuccessfulSpeculation);
	}

	public static class Factory implements SpeculationStrategy.Factory {

		private final long interval;
		private final double quantile;
		private final double multiplier;
		private final double percentage;

		public Factory(Configuration config) {
			interval = config.getLong(SpeculationOptions.SPECULATION_INTERVAL);
			quantile = config.getDouble(SpeculationOptions.SPECULATION_QUANTILE);
			multiplier = config.getDouble(SpeculationOptions.SPECULATION_MULTIPLIER);
			percentage = config.getDouble(SpeculationOptions.SPECULATION_CREDITS_PERCENTAGE);
		}

		@Override
		public SpeculationStrategy create(ExecutionGraph executionGraph) {
			final CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
			if (checkpointCoordinator != null) {
				throw new RuntimeException("Speculation is not supported in streaming application.");
			}
			return new DefaultSpeculationStrategy(executionGraph, interval, quantile, multiplier, percentage);
		}
	}
}
