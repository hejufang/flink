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

import org.apache.flink.runtime.executiongraph.restart.RecoverableRestartBackoffTimeStrategy;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RecoverableRestartBackoffTimeStrategy}.
 */
public class AggregatedFailureRateRestartBackoffTimeStrategyTest extends TestLogger {

	private final Exception failure = new Exception();

	@Test
	public void testFailuresExceedingRate() {
		final int numFailures = 3;
		final long intervalMS = 10_000L;

		ManualClock clock = new ManualClock();
		int backoffTimeMS = 100;
		final AggregatedFailureRateRestartBackoffTimeStrategy restartStrategy =
			new AggregatedFailureRateRestartBackoffTimeStrategy(clock, numFailures, intervalMS, backoffTimeMS);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
			clock.advanceTime(backoffTimeMS, TimeUnit.MILLISECONDS);
		}

		assertFalse(restartStrategy.canRestart());
	}

	@Test
	public void testManyFailuresWithinRate() {
		final int numFailures = 3;
		final long intervalMS = 1L;

		ManualClock clock = new ManualClock();

		final AggregatedFailureRateRestartBackoffTimeStrategy restartStrategy =
			new AggregatedFailureRateRestartBackoffTimeStrategy(clock, 1, intervalMS, 100);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
			clock.advanceTime(intervalMS + 1, TimeUnit.MILLISECONDS);
		}

		assertTrue(restartStrategy.canRestart());
	}

	@Test
	public void testManyFailuresInBackoffTimeWithinRate() {
		final int numFailures = 3;
		final long intervalMS = 10_000L;

		ManualClock clock = new ManualClock();
		int backoffTimeMS = 100;
		final AggregatedFailureRateRestartBackoffTimeStrategy restartStrategy =
			new AggregatedFailureRateRestartBackoffTimeStrategy(clock, numFailures, intervalMS, backoffTimeMS);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
			clock.advanceTime(backoffTimeMS - 1, TimeUnit.MILLISECONDS);
		}

		assertTrue(restartStrategy.canRestart());
	}
}
