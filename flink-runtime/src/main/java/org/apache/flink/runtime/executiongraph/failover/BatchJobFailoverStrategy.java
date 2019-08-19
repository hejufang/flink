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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Failover strategy for batch job scenario, that aware throwable type and apply different logic
 * */
public class BatchJobFailoverStrategy extends RestartPipelinedRegionStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BatchJobFailoverStrategy.class);

	private int failLimit;

	/**
	 * Creates a new failover strategy for batch job scenario, that aware throwable type and apply different logic
	 *
	 * @param executionGraph The execution graph to handle.
	 * @param config The execution config
	 */
	public BatchJobFailoverStrategy(ExecutionGraph executionGraph, Configuration config) {
		super(executionGraph, true);

		if(!(this.executionGraph.getRestartStrategy() instanceof NoRestartStrategy)){
			throw new FlinkRuntimeException("BatchJobFailoverStrategy can only work with NoRestartStrategy");
		}

		this.failLimit = config.getInteger(JobManagerOptions.MAX_ATTEMPTS_EXECUTION_FAILURE_COUNT);
	}

	// ------------------------------------------------------------------------
	//  failover implementation
	// ------------------------------------------------------------------------

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		LOG.error("TaskFailed, need recover job", cause);

		final ExecutionVertex ev = taskExecution.getVertex();
		final FailoverRegion failoverRegion = vertexToRegion.get(ev);

		if (failoverRegion == null) {
			executionGraph.failGlobal(new FlinkException(
				"Can not find a failover region for the execution " + ev.getTaskNameWithSubtaskIndex(), cause));
		}
		else {
			// Other failures are recoverable, lets try to restart the task
			// Before restart, lets check if the failure exceed the limit
			if(failoverRegion.getFailCount() >= this.failLimit){
				executionGraph.failGlobal(new FlinkException(
					"Max fail recovering attempt achieved, region failed " + this.failLimit +" times", cause));
				return;
			}

			LOG.info("Recovering task failure for {} #{} ({}) via restart of failover region",
				taskExecution.getVertex().getTaskNameWithSubtaskIndex(),
				taskExecution.getAttemptNumber(),
				taskExecution.getAttemptId());

			failoverRegion.onExecutionFail(taskExecution, cause);
		}
	}

	@Override
	public String getStrategyName() {
		return "Batch Job Failover Strategy";
	}

	// ------------------------------------------------------------------------
	//  factory
	// ------------------------------------------------------------------------

	/**
	 * Factory that instantiates the BatchJobFailoverStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {
		Configuration config;
		public Factory(Configuration config) {
			this.config=config;
		}

		@Override
		public BatchJobFailoverStrategy create(ExecutionGraph executionGraph) {
			return new BatchJobFailoverStrategy(executionGraph, config);
		}
	}
}
