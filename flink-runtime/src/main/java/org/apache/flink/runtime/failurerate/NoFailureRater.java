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

import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.util.clock.Clock;

/**
 * No failure rater only records to the per second rate for metrics reporting. Thus, its functionality falls back to
 * a MeterView.
 */
public class NoFailureRater extends MeterView implements FailureRater  {

	public NoFailureRater(int timeSpanInSeconds) {
		super(timeSpanInSeconds);
	}

	@Override
	public boolean exceedsFailureRate() {
		return false;
	}

	@Override
	public void markFailure(Clock clock) {
		markEvent(clock.absoluteTimeMillis());
	}

	@Override
	public double getCurrentFailureRate() {
		return getRate();
	}
}
