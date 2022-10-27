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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.blacklist.reporter.FailureRateReportLimiter;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link FailureRateReportLimiter}.
 */
public class FailureRateReportLimiterTest {
	@Test
	public void testFailureRateLimiter() {
		ManualClock clock = new ManualClock();
		FailureRateReportLimiter limiter = new FailureRateReportLimiter(Duration.ofMinutes(20), 3, clock);
		Assert.assertTrue(limiter.canReport());
		limiter.notifyReport();
		limiter.notifyReport();
		limiter.notifyReport();
		Assert.assertFalse(limiter.canReport());

		clock.advanceTime(21, TimeUnit.MINUTES);
		limiter.notifyReport();
		Assert.assertTrue(limiter.canReport());
	}
}
