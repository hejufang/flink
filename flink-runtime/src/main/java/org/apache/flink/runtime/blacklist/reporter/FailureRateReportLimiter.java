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

package org.apache.flink.runtime.blacklist.reporter;

import org.apache.flink.util.clock.Clock;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Black list failure reporter limiter.
 */
public class FailureRateReportLimiter {
	private final Duration failureInterval;
	private final int maxFailuresPerInterval;
	private final Clock clock;
	private final Deque<Long> failureTimestamps;

	public FailureRateReportLimiter(Duration failureInterval, int maxFailuresPerInterval, Clock clock) {
		this.failureInterval = failureInterval;
		this.maxFailuresPerInterval = maxFailuresPerInterval;
		this.failureTimestamps = new ArrayDeque<>(maxFailuresPerInterval);
		this.clock = clock;
	}

	public boolean canReport() {
		if (isFailureTimestampsQueueFull()) {
			Long now = clock.absoluteTimeMillis();
			Long earliestFailure = failureTimestamps.peek();

			return (now - earliestFailure) > failureInterval.toMillis();
		} else {
			return true;
		}
	}

	public void notifyReport() {
		if (isFailureTimestampsQueueFull()) {
			failureTimestamps.remove();
		}
		failureTimestamps.add(clock.absoluteTimeMillis());
	}

	private boolean isFailureTimestampsQueueFull() {
		return failureTimestamps.size() >= maxFailuresPerInterval;
	}
}
