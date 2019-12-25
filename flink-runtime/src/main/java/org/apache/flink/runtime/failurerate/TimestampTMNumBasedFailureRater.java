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
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayDeque;

/**
 * A timestamp queue based failure rater implementation. Also take task managers num into consideration
 */
public class TimestampTMNumBasedFailureRater extends AbstractFailureRater {

	private final double maximumFailureRateRatio;
	private int totalSlotNum = 0;
	private final int numTaskSlots;
	private final ArrayDeque<Tuple2<Long, Integer>> requiredSlots; // left is timestamp, right is required slot num at current timestamp

	public TimestampTMNumBasedFailureRater(double maximumFailureRateRatio, Time failureInterval, int numTaskSlots) {
		super(failureInterval);
		this.maximumFailureRateRatio = maximumFailureRateRatio;
		this.numTaskSlots = numTaskSlots;
		this.requiredSlots = new ArrayDeque<>();
	}

	@Override
	public void onRequiredSlotNumChanged(int num) {
		Long currentTimeStamp = System.currentTimeMillis();
		onRequiredSlotNumChanged(num, currentTimeStamp);
	}

	public void onRequiredSlotNumChanged(int num, Long currentTimeStamp) {
		// this makes sure that requiredSlots is in slot num desc order
		while (!requiredSlots.isEmpty() && requiredSlots.getLast().f1 <= totalSlotNum) {
			requiredSlots.pop();
		}
		requiredSlots.add(new Tuple2<>(currentTimeStamp, totalSlotNum));
		totalSlotNum += num;
	}

	@Override
	public boolean exceedsFailureRate() {
		Long currentTimeStamp = System.currentTimeMillis();
		return exceedsFailureRate(currentTimeStamp);
	}

	public boolean exceedsFailureRate(Long currentTimeStamp) {
		// use max slot num in interval instead of slot num at the end of interval.
		// because if there're a lot of failures in the interval in session mode,
		// after a job finished, exceedsFailureRate may return true immediately.
		double taskManagerNum = Math.ceil((double) getMaxSlotNumInInterval(currentTimeStamp) / numTaskSlots);
		return getCurrentFailureRate(currentTimeStamp) > maximumFailureRateRatio * taskManagerNum;
	}

	protected int getMaxSlotNumInInterval(Long currentTimeStamp) {
		while (!requiredSlots.isEmpty() &&
			currentTimeStamp - requiredSlots.peek().f0 > failureInterval.toMilliseconds()) {
			requiredSlots.remove();
		}

		if (requiredSlots.isEmpty()) {
			return totalSlotNum;
		}

		return Math.max(requiredSlots.peek().f1, totalSlotNum);
	}
}
