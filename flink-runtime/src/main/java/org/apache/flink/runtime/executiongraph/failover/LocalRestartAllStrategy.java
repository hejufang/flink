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
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Local Restart all.
 */
public class LocalRestartAllStrategy extends FailoverStrategy {
	private static final Logger LOGGER = LoggerFactory.getLogger(LocalRestartAllStrategy.class);

	/** The execution graph to recover. */
	private final ExecutionGraph executionGraph;
	// If job failed times > maxFailuresPerInterval between failureRateIntervalMS, degrade to full-restart
	private final long failureRateIntervalMS;
	private final int maxFailuresPerInterval;
	// Failed AttemptID and expired time
	private ConcurrentMap<Long/* AttemptID */, Long/* Expired Time MS */> failedExecutionAttempt = new ConcurrentHashMap<>();

	private LocalRestartAllStrategy(ExecutionGraph executionGraph, Configuration config) {
		this.executionGraph = executionGraph;
		this.failureRateIntervalMS = config.getLong(JobManagerOptions.LOCAL_RESTART_FAILURE_RATE_INTERVAL_MS);
		this.maxFailuresPerInterval = config.getInteger(JobManagerOptions.LOCAL_RESTART_MAX_FAILURES_PER_INTERVAL);
	}

	@Override
	public void onTaskFailure(Execution taskExecution, Throwable cause) {
		LOGGER.error("TaskFailed", cause);
		if (needDegrade(taskExecution)) {
			LOGGER.warn("Failed too many times, degrade to FullRestart");
			try {
				executionGraph.cleanupPYFlinkCache();
			} catch (Exception e) {
				LOGGER.error("cleanup pyFlink cache error", e);
			}
		} else {
			LOGGER.info("Recovering job via local restart");
		}
		executionGraph.failGlobal(cause);
	}

	@Override
	public void notifyNewVertices(List<ExecutionJobVertex> newJobVerticesTopological) {
		// nothing to do
	}

	@Override
	public String getStrategyName() {
		return "full graph local restart";
	}

	/**
	 * If job failed time > maxFailuresPerInterval between failureRateIntervalMS, degrade to full-restart.
	 * */
	private boolean needDegrade(Execution taskExecution) {
		if (failedExecutionAttempt.containsKey(taskExecution.getGlobalModVersion())) {
			LOGGER.info("Same ExecutionAttempt, do not need degrade");
			return false;
		}

		long now = System.currentTimeMillis();
		failedExecutionAttempt.put(taskExecution.getGlobalModVersion(), now + failureRateIntervalMS);
		List<Long> expiredAttemptIDS = new ArrayList<>();
		for (Entry<Long, Long> attempt : failedExecutionAttempt.entrySet()) {
			if (attempt.getValue() < now) {
				expiredAttemptIDS.add(attempt.getKey());
			}
		}

		for (Long attemptId : expiredAttemptIDS) {
			failedExecutionAttempt.remove(attemptId);
		}

		LOGGER.info("Failed {} times between {} ms", failedExecutionAttempt.size(), failureRateIntervalMS);
		return failedExecutionAttempt.size() > maxFailuresPerInterval;
	}


	/**
	 * Factory that instantiates the LocalRestartAllStrategy.
	 */
	public static class Factory implements FailoverStrategy.Factory {
		private Configuration config;

		public Factory(Configuration config) {
			this.config = config;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new LocalRestartAllStrategy(executionGraph, config);
		}
	}
}
