/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The checkpoint scheduler which aligns the checkpoint starting time to whole hours.
 * To align to whole hours, the checkpoint interval must divides an hour.
 */
public class HourlyCheckpointScheduler extends AbstractCheckpointScheduler {

	private static final Logger LOG = LoggerFactory.getLogger(HourlyCheckpointScheduler.class);

	/**
	 * An hour represented in milliseconds.
	 */
	private static final long hourInMillis = 60 * 60 * 1000L;

	/**
	 * The job whose checkpoint this coordinator coordinates.
	 */
	private final JobID job;

	/**
	 * The base checkpoint interval. Actual trigger time may be affected by the
	 * max concurrent checkpoints and minimum-pause values.
	 */
	private final long baseInterval;

	/**
	 * We align to every whole hour plus offset.
	 */
	private final long offsetMillis;

	/**
	 * The min time (in ms) to delay after a checkpoint could be triggered. Allows to
	 * enforce minimum processing time between checkpoint attempts.
	 */
	private final long minPauseMillis;

	/**
	 * The max time (in ms) that a checkpoint may take.
	 */
	private final long checkpointTimeout;

	/**
	 * The Runnable object to do regular checkpoint.
	 */
	private final Runnable regularCheckpointTask;

	/**
	 * A handle to the current periodic trigger, to cancel it when necessary.
	 */
	private ScheduledFuture<?> currentPeriodicTrigger;

	/**
	 * The checkpoint coordinator which this scheduler belongs to.
	 */
	private final CheckpointCoordinator coordinator;

	HourlyCheckpointScheduler(
			long baseInterval,
			long offsetMillis,
			long minPauseMillis,
			long checkpointTimeout,
			JobID job,
			CheckpointCoordinator coordinator) {

		this.baseInterval = baseInterval;
		this.offsetMillis = offsetMillis;
		this.minPauseMillis = minPauseMillis;
		this.checkpointTimeout = checkpointTimeout;
		this.job = job;
		this.coordinator = coordinator;

		this.regularCheckpointTask = new TriggerPeriodicCheckpoint();
		this.currentPeriodicTrigger = null;

		// Ensures checkpoint interval divides an hour
		if (hourInMillis % baseInterval != 0) {
			throw new IllegalArgumentException("Checkpoint interval does not divides an hour");
		}
	}

	@Override
	public void shutdownNow() {}

	@Override
	public void startScheduling() {
		super.startScheduling();
		final long alignedDelay = calcNecessaryDelay(System.currentTimeMillis(), minPauseMillis);
		currentPeriodicTrigger = timer.scheduleAtFixedRate(regularCheckpointTask, alignedDelay, baseInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stopScheduling() {
		super.stopScheduling();
		if (currentPeriodicTrigger != null) {
			currentPeriodicTrigger.cancel(false);
			currentPeriodicTrigger = null;
		}
	}

	@Override
	public ScheduledFuture<?> scheduleTimeoutCanceller(Runnable canceller) {
		return timer.schedule(canceller, checkpointTimeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public ScheduledFuture<?> scheduleTimeoutCanceller(long timeout, Runnable canceller) {
		return timer.schedule(canceller, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public void rescheduleNextCheckpointTrigger(long tillNextMillis) {
		if (currentPeriodicTrigger != null) {
			currentPeriodicTrigger.cancel(false);
		}
		// postpone next checkpoint
		final long alignedDelay = calcNecessaryDelay(System.currentTimeMillis(), tillNextMillis);
		currentPeriodicTrigger = timer.scheduleAtFixedRate(regularCheckpointTask, alignedDelay, baseInterval, TimeUnit.MILLISECONDS);
	}

	@Override
	public boolean isPeriodicCheckpointingConfigured() {
		return baseInterval != Long.MAX_VALUE;
	}

	@Override
	public boolean inPeriodicallyScheduling() {
		return currentPeriodicTrigger != null;
	}

	@Override
	public boolean isScheduling() {
		return currentPeriodicTrigger != null;
	}

	/**
	 * Task to do regular checkpoint, should be run in a single thread.
	 */
	private class TriggerPeriodicCheckpoint implements Runnable {
		@Override
		public void run() {
			try {
				coordinator.triggerCheckpoint(true);
			} catch (Exception e) {
				LOG.error("Exception while triggering checkpoint for job {}.", job, e);
			}
		}
	}

	/**
	 * Taking current time and a minimum, required delay into consideration, align a fixed rate
	 * scheduling strategy to cover all whole hours.
	 *
	 * @param originMillis       current time
	 * @param minimumDelayMillis minimum delay (in ms)
	 * @return delay time (in ms) to ensure that the scheduled checkpoints cover all whole hours.
	 */
	@VisibleForTesting
	long calcNecessaryDelay(long originMillis, long minimumDelayMillis) {
		return nextIntervalMillis(originMillis + minimumDelayMillis) - originMillis;
	}

	/**
	 * @param timeMillis origin time
	 * @return nearest time (in the future) to start our fixed rate scheduling so that we trigger on whole hours
	 */
	private long nextIntervalMillis(long timeMillis) {
		long remainder = (timeMillis - offsetMillis) % baseInterval;

		if (remainder == 0) {
			return timeMillis;
		} else if (remainder < 0) {
			return -remainder + timeMillis;
		} else {
			return baseInterval - remainder + timeMillis;
		}
	}
}