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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * The default checkpoint scheduler, which triggers checkpoints at fixed rate checkpoint
 * strategy. For some job, checkpoint decides when to commit writing, and a late checkpoint
 * will block the writing process so long that the downstream might considered the job abnormal.
 */
public class DefaultCheckpointScheduler extends AbstractCheckpointScheduler {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultCheckpointScheduler.class);

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

	DefaultCheckpointScheduler(
			long baseInterval,
			long minPauseMillis,
			long checkpointTimeout,
			JobID job,
			CheckpointCoordinator coordinator) {

		this.baseInterval = baseInterval;
		this.minPauseMillis = minPauseMillis;
		this.checkpointTimeout = checkpointTimeout;
		this.job = job;
		this.coordinator = coordinator;

		this.regularCheckpointTask = new TriggerPeriodicCheckpoint();
		this.currentPeriodicTrigger = null;
	}

	@Override
	public void shutdownNow() {}

	@Override
	public void startScheduling() {
		super.startScheduling();
		currentPeriodicTrigger = timer.scheduleAtFixedRate(regularCheckpointTask, getRandomInitDelay(), baseInterval, TimeUnit.MILLISECONDS);
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
	public void checkMinPauseSinceLastCheckpoint(long lastCompletionNanos) throws CheckpointException {
		final long elapsedTimeMillis = (System.nanoTime() - lastCompletionNanos) / 1_000_000L;

		// this will never be triggered by early checkpoint, so we just check regular ones
		if (elapsedTimeMillis < minPauseMillis) {
			// ensure that there is enough delay
			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel(false);
			}
			// postpone next checkpoint
			currentPeriodicTrigger = timer.scheduleAtFixedRate(regularCheckpointTask, minPauseMillis - elapsedTimeMillis, baseInterval, TimeUnit.MILLISECONDS);

			// abort current checkpoint
			throw new CheckpointException(CheckpointFailureReason.MINIMUM_TIME_BETWEEN_CHECKPOINTS);
		}
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
	 * @return random delay in milliseconds, with minimum pause included
	 */
	private long getRandomInitDelay() {
		return ThreadLocalRandom.current().nextLong(minPauseMillis, baseInterval + 1L);
	}

	/**
	 * Task to do regular checkpoint, should be run in a single thread. If early checkpoint is enabled, this
	 * task should only be run after all thread of early checkpoint terminates.
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
}
