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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test time stamp and task manager num based failure rater.
 */
public class TimestampTMNumBasedFailureRaterTest {

	@Test
	public void testMaxSlotNum() {
		TimestampTMNumBasedFailureRater rater = new TimestampTMNumBasedFailureRater(2.0, Time.of(10, TimeUnit.SECONDS), 3);
		long currentTime = System.currentTimeMillis();
		rater.onRequiredSlotNumChanged(10, currentTime);
		currentTime += 1000;
		rater.onRequiredSlotNumChanged(-5, currentTime);
		currentTime += 1000;
		rater.onRequiredSlotNumChanged(30, currentTime);
		currentTime += 1000;
		rater.onRequiredSlotNumChanged(-15, currentTime);
		Assert.assertEquals(35, rater.getMaxSlotNumInInterval(currentTime));
		currentTime += 100000;
		Assert.assertEquals(20, rater.getMaxSlotNumInInterval(currentTime));
	}

	@Test
	public void testExceed() {
		TimestampTMNumBasedFailureRater rater = new TimestampTMNumBasedFailureRater(2.0, Time.of(10, TimeUnit.SECONDS), 3);
		long currentTime = System.currentTimeMillis();
		rater.onRequiredSlotNumChanged(10, currentTime);
		currentTime += 1000;
		rater.onRequiredSlotNumChanged(-5, currentTime);
		currentTime += 1000;
		for (int i = 0; i < 7; i++) {
			rater.markEvent(currentTime);
		}
		Assert.assertFalse(rater.exceedsFailureRate(currentTime));
		currentTime += 1000;
		rater.markEvent(currentTime);
		Assert.assertFalse(rater.exceedsFailureRate(currentTime));
		currentTime += 9000;
		Assert.assertTrue(rater.exceedsFailureRate(currentTime));
	}

}
