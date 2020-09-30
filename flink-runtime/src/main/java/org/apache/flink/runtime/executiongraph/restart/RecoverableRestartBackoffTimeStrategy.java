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
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

/**
 *
 */
public class RecoverableRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(RecoverableRestartBackoffTimeStrategy.class);

	private final Time failuresInterval;
	private final int maxFailuresPerInterval;
	private final boolean fallbackToGlobalRestart;
	private final ArrayDeque<Long> restartTimestampsDeque;

	public RecoverableRestartBackoffTimeStrategy(int maxFailuresPerInterval, Time failuresInterval, boolean fallbackToGlobalRestart) {
		Preconditions.checkNotNull(failuresInterval, "Failures interval cannot be null.");
		Preconditions.checkArgument(maxFailuresPerInterval > 0, "Maximum number of restart attempts per time unit must be greater than 0.");
		Preconditions.checkArgument(failuresInterval.getSize() > 0, "Failures interval must be greater than 0 ms.");

		this.failuresInterval = failuresInterval;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.fallbackToGlobalRestart = fallbackToGlobalRestart;
		this.restartTimestampsDeque = new ArrayDeque<>(maxFailuresPerInterval);
	}

	@Override
	public boolean canRestart() {
		if (isRestartTimestampsQueueFull()) {
			Long now = System.currentTimeMillis();
			Long earliestFailure = restartTimestampsDeque.peek();

			boolean canRestart = (now - earliestFailure) > failuresInterval.toMilliseconds();
			if (!canRestart && fallbackToGlobalRestart) {
				reset();
			}
			return canRestart;
		} else {
			return true;
		}
	}

	@Override
	public long getBackoffTime() {
		return 1000L;
	}

	@Override
	public void notifyFailure(Throwable cause) {}

	private boolean isRestartTimestampsQueueFull() {
		return restartTimestampsDeque.size() >= maxFailuresPerInterval;
	}

	private void reset() {
		LOG.info("RecoverableRestartBackoffTimeStrategy is reset.");
		restartTimestampsDeque.clear();
	}

	public static RecoverableRestartBackoffTimeStrategy.RecoverableRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		int maxFailuresPerInterval = configuration.getInteger(ConfigConstants.RESTART_STRATEGY_RECOVERABLE_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 150);
		String failuresIntervalString = configuration.getString(
				ConfigConstants.RESTART_STRATEGY_RECOVERABLE_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.apply(10, TimeUnit.MINUTES).toString()
		);
		boolean fallbackToGlobalRestart = configuration.getBoolean(
				ConfigConstants.RESTART_STRATEGY_RECOVERABLE_FALLBACK_GLOBAL_RESTART, false);

		Duration failuresInterval = Duration.apply(failuresIntervalString);

		return new RecoverableRestartBackoffTimeStrategy.RecoverableRestartBackoffTimeStrategyFactory(
				maxFailuresPerInterval, Time.milliseconds(failuresInterval.toMillis()), fallbackToGlobalRestart);
	}

	public static class RecoverableRestartBackoffTimeStrategyFactory implements RestartBackoffTimeStrategy.Factory {

		private final int maxFailuresPerInterval;
		private final Time failuresInterval;
		private final boolean fallbackToGlobalRestart;

		public RecoverableRestartBackoffTimeStrategyFactory(int maxFailuresPerInterval, Time failuresInterval, boolean fallbackToGlobalRestart) {
			this.maxFailuresPerInterval = maxFailuresPerInterval;
			this.failuresInterval = Preconditions.checkNotNull(failuresInterval);
			this.fallbackToGlobalRestart = fallbackToGlobalRestart;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new RecoverableRestartBackoffTimeStrategy(maxFailuresPerInterval, failuresInterval, fallbackToGlobalRestart);
		}
	}
}
