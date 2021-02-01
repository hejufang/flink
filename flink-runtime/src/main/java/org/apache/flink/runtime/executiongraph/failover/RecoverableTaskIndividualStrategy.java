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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.SchedulingUtils;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple failover strategy that restarts each task individually. Used for recoverable failover strategy.
 */
public class RecoverableTaskIndividualStrategy extends FailoverStrategy {

	private static final Logger LOG = LoggerFactory.getLogger(RecoverableTaskIndividualStrategy.class);

	// ------------------------------------------------------------------------

	/** The execution graph to recover */
	private final ExecutionGraph executionGraph;

	private final SimpleCounter numTaskRecoveries;
	private final SimpleCounter numGlobalFailures;

	/**
	 * Creates a new failover strategy that recovers from failures by restarting only the failed task
	 * of the execution graph.
	 *
	 * @param executionGraph The execution graph to handle.
	 */
	public RecoverableTaskIndividualStrategy(ExecutionGraph executionGraph) {
		this.executionGraph = checkNotNull(executionGraph);
		this.numTaskRecoveries = new SimpleCounter();
		this.numGlobalFailures = new SimpleCounter();
	}

	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		LOG.error("TaskFailed, need recover job", cause);

		executionGraph.getJobMasterMainThreadExecutor().assertRunningInMainThread();

		// to better handle the lack of resources (potentially by a scale-in), we
		// make failures due to missing resources global failures
		if (cause instanceof NoResourceAvailableException) {
			LOG.info("Not enough resources to schedule {} - triggering full recovery.", taskExecution);
			executionGraph.failGlobal(new SuppressRestartsException(cause));
			return;
		}

		if (!executionGraph.getRestartStrategy().canRestart()) {
			LOG.info("Fail to pass the restart strategy validation in individual failover. Fallback to fail global.");
			executionGraph.failGlobal(cause);
			return;
		}

		LOG.info("Recovering task failure for {} (#{}) via recoverable individual restart.",
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber());

		numTaskRecoveries.inc();

		taskExecution.getReleaseFuture().whenCompleteAsync((ignore, error) -> {
			if (error != null) {
				executionGraph.failGlobal(error);
			}

			executionGraph.getRestartStrategy().restart(() -> performExecutionVertexRestart(
					taskExecution.getVertex(),taskExecution.getGlobalModVersion()), executionGraph.getJobMasterMainThreadExecutor());
		}, executionGraph.getJobMasterMainThreadExecutor());
	}

	protected void performExecutionVertexRestart(
			ExecutionVertex vertexToRecover,
			long globalModVersion) {
		try {
			long createTimestamp = System.currentTimeMillis();
			Execution newExecution = vertexToRecover.resetForNewExecution(createTimestamp, globalModVersion);
			recoverTaskState(vertexToRecover);
			CompletableFuture<Void> future = SchedulingUtils.scheduleRecoverableExecution(newExecution, executionGraph);
			future.whenComplete((Void ignore, Throwable t) -> {
				if (t != null) {
					executionGraph.failGlobal(new Exception("Error during fine grained recovery - triggering full recovery", t));
				}
			});
		} catch (GlobalModVersionMismatch e) {
			LOG.warn("Concurrent global recovery happens, ignore...");
		} catch (Exception e) {
			LOG.error("perform recover for task {} failed", vertexToRecover.getTaskNameWithSubtaskIndex(), e);
			executionGraph.failGlobal(new Exception("Error during fine grained recovery - triggering full recovery", e));
		}
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// do nothing by default
	}

	@Override
	public String getStrategyName() {
		return "Recoverable Task Individual Strategy";
	}

	@Override
	public void registerMetrics(MetricGroup metricGroup) {
		metricGroup.gauge("numberOfRecoverableJobs", () -> 1);
		metricGroup.counter("numberOfTaskRecoveries", numTaskRecoveries);
		metricGroup.counter("numberOfGlobalFailures", numGlobalFailures);
	}

	private void recoverTaskState(ExecutionVertex vertexToRecover) throws Exception {
		// if there is checkpointed state and we use fast mode, reload it into the execution
		if (executionGraph.getCheckpointCoordinator() != null && executionGraph.getCheckpointCoordinator().isUseFastMode()) {
			LOG.info("There is checkpointed state can be used to reload for vertex {}.", vertexToRecover.getTaskNameWithSubtaskIndex());
			// abort pending checkpoints to
			// i) enable new checkpoint triggering without waiting for last checkpoint expired.
			// ii) no need to trigger pending tasks
			executionGraph.getCheckpointCoordinator().onTaskFailure(Collections.singleton(vertexToRecover),
				new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

			executionGraph.getCheckpointCoordinator().restoreLatestCheckpointedState(
				Collections.singletonMap(vertexToRecover.getJobvertexId(), vertexToRecover.getJobVertex()),
				false,
				true);
		}
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public RecoverableTaskIndividualStrategy create(ExecutionGraph executionGraph) {
			return new RecoverableTaskIndividualStrategy(executionGraph);
		}
	}
}
