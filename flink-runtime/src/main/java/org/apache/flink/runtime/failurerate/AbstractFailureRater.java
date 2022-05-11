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

package org.apache.flink.runtime.failurerate;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.clock.Clock;

import java.util.ArrayDeque;
import java.util.function.Supplier;

/**
 * Abstract Failure Rater implementation.
 */
public abstract class AbstractFailureRater implements FailureRater {
	private static final int DEFAULT_TIMESTAMP_SIZE = 300;
	private static final int MILLISECONDS_PER_SECOND = 1000;
	private static final Supplier<Long> TIMESTAMP_SUPPLIER = System::currentTimeMillis;

	protected final Time failureInterval;
	private final ArrayDeque<Long> failureTimestamps = new ArrayDeque<>(DEFAULT_TIMESTAMP_SIZE);
	private final Supplier<Long> timestampSupplier;
	private long failureCounter = 0;

	protected AbstractFailureRater(Time failureInterval) {
		this(failureInterval, TIMESTAMP_SUPPLIER);
	}

	protected AbstractFailureRater(Time failureInterval, Supplier<Long> timestampSupplier) {
		this.failureInterval = failureInterval;
		this.timestampSupplier = timestampSupplier;
	}

	@Override
	public double getCurrentFailureRate() {
		Long currentTimeStamp = timestampSupplier.get();
		return getCurrentFailureRate(currentTimeStamp);
	}

	protected double getCurrentFailureRate(Long currentTimeStamp) {
		while (!failureTimestamps.isEmpty() &&
			currentTimeStamp - failureTimestamps.peek() > failureInterval.toMilliseconds()) {
			failureTimestamps.remove();
		}

		return failureTimestamps.size();
	}

	@Override
	public void markFailure(Clock clock) {
		failureTimestamps.add(clock.absoluteTimeMillis());
		failureCounter++;
	}

	@Override
	public long getCount() {
		return failureCounter;
	}

	@Override
	public void markEvent() {
		markEvent(System.currentTimeMillis());
	}

	@Override
	public void markEvent(long n) {
		failureTimestamps.add(n);
		failureCounter++;
	}

	@Override
	public double getRate() {
		return getCurrentFailureRate() / (failureInterval.toMilliseconds() / MILLISECONDS_PER_SECOND);
	}
}
