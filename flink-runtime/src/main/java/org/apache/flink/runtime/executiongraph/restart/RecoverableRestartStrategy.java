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

package org.apache.flink.runtime.executiongraph.restart;


import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 * Similar implementation with {@link FailureRateRestartStrategy} but much simpler.
 */
public class RecoverableRestartStrategy implements RestartStrategy {

	private final Time failuresInterval;
	private final int maxFailuresPerInterval;
	private final ArrayDeque<Long> restartTimestampsDeque;

	public RecoverableRestartStrategy(int maxFailuresPerInterval, Time failuresInterval) {
		Preconditions.checkNotNull(failuresInterval, "Failures interval cannot be null.");
		Preconditions.checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		Preconditions.checkArgument(failuresInterval.getSize() > 0, "Failures interval must be greater than 0 ms.");

		this.failuresInterval = failuresInterval;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.restartTimestampsDeque = new ArrayDeque<>(maxFailuresPerInterval);
	}

	@Override
	public boolean canRestart() {
		if (isRestartTimestampsQueueFull()) {
			Long now = System.currentTimeMillis();
			Long earliestFailure = restartTimestampsDeque.peek();

			return (now - earliestFailure) > failuresInterval.toMilliseconds();
		} else {
			return true;
		}
	}

	@Override
	public CompletableFuture<Void> restart(final RestartCallback restarter, ScheduledExecutor executor) {
		if (isRestartTimestampsQueueFull()) {
			restartTimestampsDeque.remove();
		}
		restartTimestampsDeque.add(System.currentTimeMillis());
		return FutureUtils.scheduleWithDelay(restarter::triggerFullRecovery, Time.seconds(10), executor);
	}

	private boolean isRestartTimestampsQueueFull() {
		return restartTimestampsDeque.size() >= maxFailuresPerInterval;
	}

	@Override
	public String toString() {
		return "RecoverableRestartStrategy(" +
				"failuresInterval=" + failuresInterval +
				"maxFailuresPerInterval=" + maxFailuresPerInterval +
				")";
	}

	public static RecoverableRestartStrategyFactory createFactory(Configuration configuration) throws Exception {
		int maxFailuresPerInterval = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_RECOVERABLE_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 50);
		String failuresIntervalString = configuration.getString(
				ConfigConstants.RESTART_STRATEGY_RECOVERABLE_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.apply(10, TimeUnit.MINUTES).toString()
		);

		Duration failuresInterval = Duration.apply(failuresIntervalString);

		return new RecoverableRestartStrategyFactory(maxFailuresPerInterval, Time.milliseconds(failuresInterval.toMillis()));
	}

	public static class RecoverableRestartStrategyFactory extends RestartStrategyFactory {

		private final int maxFailuresPerInterval;
		private final Time failuresInterval;

		public RecoverableRestartStrategyFactory(int maxFailuresPerInterval, Time failuresInterval) {
			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresInterval = Preconditions.checkNotNull(failuresInterval);
		}

		@Override
		public RestartStrategy createRestartStrategy() {
			return new RecoverableRestartStrategy(maxFailuresPerInterval, failuresInterval);
		}
	}
}

