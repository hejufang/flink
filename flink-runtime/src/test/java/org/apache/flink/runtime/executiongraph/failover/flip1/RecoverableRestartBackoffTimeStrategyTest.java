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

import org.apache.flink.api.common.time.Time;
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
public class RecoverableRestartBackoffTimeStrategyTest extends TestLogger {

	private final Exception failure = new Exception();

	@Test
	public void testManyFailuresWithinRateFallBackToGlobalRestart() {
		final int numFailures = 3;
		final long intervalMS = 1L;

		ManualClock clock = new ManualClock();

		final RecoverableRestartBackoffTimeStrategy restartStrategy =
			new RecoverableRestartBackoffTimeStrategy(clock, numFailures, Time.milliseconds(intervalMS), true, false);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
			clock.advanceTime(intervalMS + 1, TimeUnit.MILLISECONDS);
		}

		assertTrue(restartStrategy.canRestart());
	}

	@Test
	public void testFailuresExceedingRate() {
		final int numFailures = 3;
		final long intervalMS = 10_000L;

		final RecoverableRestartBackoffTimeStrategy restartStrategy =
			new RecoverableRestartBackoffTimeStrategy(new ManualClock(), numFailures, Time.milliseconds(intervalMS), true, false);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
		}

		assertFalse(restartStrategy.canRestart());
	}

	@Test
	public void testFallbackToGlobalRestart() {
		final int numFailures = 3;
		final long intervalMS = 10_000L;

		final RecoverableRestartBackoffTimeStrategy restartStrategy =
			new RecoverableRestartBackoffTimeStrategy(new ManualClock(), numFailures, Time.milliseconds(intervalMS), true, true);

		for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
			assertTrue(restartStrategy.canRestart());
			restartStrategy.notifyFailure(failure);
		}

		assertTrue(restartStrategy.canRestart());
		assertTrue(restartStrategy.isFallbackToGlobalRestart());
	}
}
