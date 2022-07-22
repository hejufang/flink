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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Restart strategy which can restart when failure rate is not exceeded,
 * failures in backoff time will be aggregated to one failure.
 */
public class AggregatedFixedDelayRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

	private final long backoffTimeMS;

	private final int maxFailures;

	private final Deque<Long> failureTimestamps;

	private final String strategyString;

	private final Clock clock;

	AggregatedFixedDelayRestartBackoffTimeStrategy(Clock clock, int maxFailures, long backoffTimeMS) {

		checkArgument(maxFailures > 0, "Maximum number of restart attempts must be greater than 0.");
		checkArgument(backoffTimeMS >= 0, "Backoff time must be at least 0 ms.");

		this.backoffTimeMS = backoffTimeMS;
		this.maxFailures = maxFailures;
		this.failureTimestamps = new ArrayDeque<>(maxFailures);
		this.strategyString = generateStrategyString();
		this.clock = checkNotNull(clock);
	}

	@VisibleForTesting
	Deque<Long> getFailureTimestamps() {
		return failureTimestamps;
	}

	@Override
	public boolean canRestart() {
		return !isFailureTimestampsQueueFull();
	}

	@Override
	public long getBackoffTime() {
		return backoffTimeMS;
	}

	@Override
	public void notifyFailure(Throwable cause) {
		Long now = clock.absoluteTimeMillis();
		// Ignore frequent failures within backoffTimeMS.
		// Otherwise, a single TM failure may cause the job to fail directly in Region Failover Strategy.
		if (failureTimestamps.isEmpty() || now - failureTimestamps.getLast() >= backoffTimeMS) {
			failureTimestamps.add(now);
		}
	}

	@Override
	public String toString() {
		return strategyString;
	}

	private boolean isFailureTimestampsQueueFull() {
		return failureTimestamps.size() > maxFailures;
	}

	private String generateStrategyString() {
		StringBuilder str = new StringBuilder("AggregatedFailureFixedRestartBackoffTimeStrategy(");
		str.append("AggregatedFailureFixedRestartBackoffTimeStrategy(");
		str.append("backoffTimeMS=");
		str.append(backoffTimeMS);
		str.append(",maxFailures=");
		str.append(maxFailures);
		str.append(")");

		return str.toString();
	}

	public static AggregatedFailureFixedRestartBackoffTimeStrategyFactory createFactory(final Configuration configuration) {
		int maxFailures = configuration.getInteger(RestartStrategyOptions.RESTART_STRATEGY_AGGREGATED_FAILURE_FIXED_MAX_FAILURES);
		long delay = configuration.get(RestartStrategyOptions.RESTART_STRATEGY_AGGREGATED_FAILURE_FIXED_DELAY).toMillis();

		return new AggregatedFailureFixedRestartBackoffTimeStrategyFactory(maxFailures, delay);
	}

	/**
	 * The factory for creating {@link AggregatedFixedDelayRestartBackoffTimeStrategy}.
	 */
	public static class AggregatedFailureFixedRestartBackoffTimeStrategyFactory implements Factory {

		private final int maxFailuresPerInterval;

		private final long backoffTimeMS;

		public AggregatedFailureFixedRestartBackoffTimeStrategyFactory(
			int maxFailures,
			long backoffTimeMS) {

			this.maxFailuresPerInterval = maxFailures;
			this.backoffTimeMS = backoffTimeMS;
		}

		@Override
		public RestartBackoffTimeStrategy create() {
			return new AggregatedFixedDelayRestartBackoffTimeStrategy(
				SystemClock.getInstance(),
				maxFailuresPerInterval,
				backoffTimeMS);
		}
	}
}
