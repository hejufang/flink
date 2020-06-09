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

package org.apache.flink.runtime.checkpoint.scheduler;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

/**
 * Test class for hourly scheduler.
 */
public class HourlyCheckpointSchedulerTest {

	private final Calendar calendar;

	public HourlyCheckpointSchedulerTest() {
		calendar = Calendar.getInstance();
		calendar.setTimeInMillis(0); // set milliseconds to 0
	}

	@Test
	public void argCheckTest() {
		final long duration120Min = 120 * 60 * 1000L;
		final long duration60Min = 60 * 60 * 1000L;
		final long duration20Min = 15 * 60 * 1000L;
		final long duration7Min = 7 * 60 * 1000L;
		final long durationRandom = 283901L;

		Assert.assertTrue(constructorRaiseException(duration120Min));
		Assert.assertTrue(constructorRaiseException(duration7Min));
		Assert.assertTrue(constructorRaiseException(durationRandom));

		Assert.assertFalse(constructorRaiseException(duration20Min));
		Assert.assertFalse(constructorRaiseException(duration60Min));
	}

	private boolean constructorRaiseException(long interval) {
		try {
			new HourlyCheckpointScheduler(interval, 0, 0, 0, null, null);
		} catch (IllegalArgumentException exception) {
			return true;
		}
		return false;
	}

	@Test
	public void alignmentTest() {
		final long checkpointInterval = 15 * 60 * 1000L; // 15 minutes

		HourlyCheckpointScheduler hourlyScheduler = new HourlyCheckpointScheduler(
			checkpointInterval, 0, 0, 60000, null, null);

		calendar.set(2020, 1, 20, 10, 14, 59);
		long timestamp = calendar.getTimeInMillis();
		long scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 0) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(15, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1000) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(15, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1001) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(30, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		calendar.set(2020, 1, 20, 10, 42, 17);
		timestamp = calendar.getTimeInMillis();
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1001) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(45, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		// New hour
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 3 * 60 * 1000L) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(11, getHour(scheduleTime));
		Assert.assertEquals(0, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		// New day
		calendar.set(2020, 1, 20, 23, 48, 0);
		timestamp = calendar.getTimeInMillis();
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 0L) + timestamp;
		Assert.assertFalse(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(0, getHour(scheduleTime));
		Assert.assertEquals(0, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));
	}

	@Test
	public void offsetTest() {
		final long offset = 3 * 60 * 1000L;
		final long checkpointInterval = 15 * 60 * 1000L; // 15 minutes

		HourlyCheckpointScheduler hourlyScheduler = new HourlyCheckpointScheduler(
			checkpointInterval, offset, 0, 60000, null, null);
		offsetResultCheck(hourlyScheduler);

		// robustness test
		// negative offset
		hourlyScheduler = new HourlyCheckpointScheduler(
			checkpointInterval, offset - checkpointInterval, 0, 60000, null, null);
		offsetResultCheck(hourlyScheduler);

		// large offset
		hourlyScheduler = new HourlyCheckpointScheduler(
			checkpointInterval, offset + checkpointInterval * 73, 0, 60000, null, null);
		offsetResultCheck(hourlyScheduler);

		hourlyScheduler = new HourlyCheckpointScheduler(
			checkpointInterval, offset - checkpointInterval * 73, 0, 60000, null, null);
		offsetResultCheck(hourlyScheduler);
	}

	private void offsetResultCheck(HourlyCheckpointScheduler hourlyScheduler) {
		calendar.set(2020, 1, 20, 10, 14, 59);
		long timestamp = calendar.getTimeInMillis();
		long scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 0) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(18, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1000) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(18, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1001) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(18, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		calendar.set(2020, 1, 20, 10, 42, 17);
		timestamp = calendar.getTimeInMillis();
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1001) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(10, getHour(scheduleTime));
		Assert.assertEquals(48, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		// New hour
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 6 * 60 * 1000L) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(11, getHour(scheduleTime));
		Assert.assertEquals(3, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		// New day
		calendar.set(2020, 1, 20, 23, 48, 0);
		timestamp = calendar.getTimeInMillis();
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 0L) + timestamp;
		Assert.assertTrue(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(23, getHour(scheduleTime));
		Assert.assertEquals(48, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));

		calendar.set(2020, 1, 20, 23, 48, 0);
		timestamp = calendar.getTimeInMillis();
		scheduleTime = hourlyScheduler.calcNecessaryDelay(timestamp, 1L) + timestamp;
		Assert.assertFalse(inTheSameDay(timestamp, scheduleTime));
		Assert.assertEquals(0, getHour(scheduleTime));
		Assert.assertEquals(3, getMinute(scheduleTime));
		Assert.assertEquals(0, getSecond(scheduleTime));
	}

	private int getHour(long timestamp) {
		calendar.setTimeInMillis(timestamp);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	private int getMinute(long timestamp) {
		calendar.setTimeInMillis(timestamp);
		return calendar.get(Calendar.MINUTE);
	}

	private int getSecond(long timestamp) {
		calendar.setTimeInMillis(timestamp);
		return calendar.get(Calendar.SECOND);
	}

	private boolean inTheSameDay(long timestamp1, long timestamp2) {
		calendar.setTimeInMillis(timestamp1);

		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		int day = calendar.get(Calendar.DAY_OF_MONTH);

		calendar.setTimeInMillis(timestamp2);
		return year == calendar.get(Calendar.YEAR) &&
			month == calendar.get(Calendar.MONTH) &&
			day == calendar.get(Calendar.DAY_OF_MONTH);
	}

	private String timestampToString(long timestamp) {
		calendar.setTimeInMillis(timestamp);

		int mYear = calendar.get(Calendar.YEAR);
		int mMonth = calendar.get(Calendar.MONTH) + 1;
		int mDay = calendar.get(Calendar.DAY_OF_MONTH);
		int mHour = calendar.get(Calendar.HOUR_OF_DAY);
		int mMinute = calendar.get(Calendar.MINUTE);
		int mSecond = calendar.get(Calendar.SECOND);
		int mMillis = calendar.get(Calendar.MILLISECOND);
		return "" + mYear + ", " + mMonth + ", " + mDay + ", " + mHour + ", " + mMinute + ", " + mSecond + mMillis;
	}
}
