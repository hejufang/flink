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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.GlobalModVersionMismatch;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Simple failover strategy that restarts each task individually forever.
 * This strategy is only applicable if the entire job consists unconnected
 * tasks, meaning each task is its own component.
 */
public class RestartIndividualForeverStrategy extends FailoverStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(RestartIndividualForeverStrategy.class);

	/** The execution graph to recover. */
	private final ExecutionGraph executionGraph;

	/** The executor that executes restart callbacks. */
	private final Executor callbackExecutor;

	/** If task failed cause is has no enough resource, sleep tmLaunchWaitingTimeMs waiting new tm launched. */
	private final long tmLaunchWaitingTimeMs;

	/** Whether to restart a subtask in previous location. */
	private final boolean previousLocationFirstAlways;

	private RestartIndividualForeverStrategy(ExecutionGraph executionGraph, Configuration config) {
		this.executionGraph = executionGraph;
		this.callbackExecutor = executionGraph.getFutureExecutor();
		this.tmLaunchWaitingTimeMs = config.getInteger(JobManagerOptions.INDIVIDUAL_FOREVER_TM_LAUNCH_WAITING_TIME_MS);
		this.previousLocationFirstAlways =
			config.getBoolean(JobManagerOptions.FAILOVER_PREVIOUS_LOCATION_FIRST_ALWAYS);
	}

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		LOG.error("TaskFailed, need recover job", cause);

		if (cause instanceof NoResourceAvailableException) {
			LOG.info("Not enough resources to schedule {} - delay {} ms, wait new tm launched.",
				taskExecution, tmLaunchWaitingTimeMs);
			try {
				Thread.sleep(tmLaunchWaitingTimeMs);
			} catch (InterruptedException e) {
				LOG.warn("individual-forever restart wait new tm launched interrupted");
			}
		}

		LOG.info("Recovering task failure for {} (#{}) via individual-forever restart.",
			taskExecution.getVertex().getTaskNameWithSubtaskIndex(), taskExecution.getAttemptNumber());

		// trigger the restart once the task has reached its terminal state
		// Note: currently all tasks passed here are already in their terminal state,
		//       so we could actually avoid the future. We use it anyways because it is cheap and
		//       it helps to support better testing
		final CompletableFuture<ExecutionState> terminationFuture = taskExecution.getTerminalStateFuture();

		final ExecutionVertex vertexToRecover = taskExecution.getVertex();
		final long globalModVersion = taskExecution.getGlobalModVersion();

		terminationFuture.thenAcceptAsync(
			(ExecutionState value) -> {
				try {
					long createTimestamp = System.currentTimeMillis();
					Execution newExecution = vertexToRecover.resetForNewExecution(createTimestamp, globalModVersion);
					// TODO: Schedule to other taskmanger if failed too many times.
					newExecution.setPreviousLocationFirstAlways(previousLocationFirstAlways);
					newExecution.scheduleForExecution();
				}
				catch (GlobalModVersionMismatch e) {
					// this happens if a concurrent global recovery happens. simply do nothing.
				}
				catch (Exception e) {
					executionGraph.failGlobal(
						new Exception("Error during fine grained recovery - triggering full recovery", e));
				}
			},
			callbackExecutor);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// we validate here that the vertices are in fact not connected to
		// any other vertices, if any vertices not qualified, cancel job
		for (ExecutionJobVertex ejv : newJobVerticesTopological) {
			List<IntermediateResult> inputs = ejv.getInputs();
			IntermediateResult[] outputs = ejv.getProducedDataSets();

			if ((inputs != null && inputs.size() > 0) || (outputs != null && outputs.length > 0)) {
				throw new FlinkRuntimeException("Incompatible failover strategy - strategy '" +
					getStrategyName() + "' can only handle jobs with only disconnected tasks.");
			}
		}
	}

	@Override
	public String getStrategyName() {
		return "Individual-Forever Task Restart";
	}

	/**
	 * Factory that instantiates the RestartIndividualForeverStrategy.
	 */
	public static class Factory extends FailoverStrategy.AbstractFactory {
		public Factory() {
			this(null);
		}

		public Factory(Configuration config) {
			super(config);
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartIndividualForeverStrategy(executionGraph, getConfig());
		}
	}
}
