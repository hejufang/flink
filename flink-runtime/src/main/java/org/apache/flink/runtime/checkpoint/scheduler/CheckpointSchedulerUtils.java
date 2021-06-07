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
import org.apache.flink.api.common.checkpointstrategy.CheckpointSchedulingStrategies;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.scheduler.savepoint.SimplePeriodicSavepointScheduler;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities static function for checkpoint schedulers.
 */
public final class CheckpointSchedulerUtils {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointSchedulerUtils.class);

	/**
	 * Creates a checkpoint scheduler based on the configuration.
	 *
	 * @return checkpoint scheduler
	 */
	public static CheckpointScheduler createCheckpointScheduler(
		JobID job,
		CheckpointCoordinator coordinator,
		CheckpointCoordinatorConfiguration chkConfig) {

		// max "in between duration" can be one year - this is to prevent numeric overflows
		long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
		if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
			minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
		}

		// time between checkpoints
		long baseInterval = chkConfig.getCheckpointInterval();

		switch (chkConfig.checkpointSchedulerConfiguration.strategy) {
			case DEFAULT:
				if (chkConfig.checkpointSchedulerConfiguration instanceof CheckpointSchedulingStrategies.DefaultSchedulerConfiguration) {

					final CheckpointSchedulingStrategies.DefaultSchedulerConfiguration schedulerConfiguration;
					schedulerConfiguration = (CheckpointSchedulingStrategies.DefaultSchedulerConfiguration) chkConfig.checkpointSchedulerConfiguration;

					// Override global configuration
					if (schedulerConfiguration.isIntervalSet()) {
						baseInterval = schedulerConfiguration.interval;
					}

					if (baseInterval < minPauseBetweenCheckpoints) {
						baseInterval = minPauseBetweenCheckpoints;
					}

					return new DefaultCheckpointScheduler(
						baseInterval,
						minPauseBetweenCheckpoints,
						chkConfig.getCheckpointTimeout(),
						chkConfig.checkpointSchedulerConfiguration.earlyCheckpointConfig,
						job,
						coordinator);
				} else {
					LOG.warn("Inconsistent checkpoint scheduler configuration. A configuration with class {} has been recognized as Default strategy.",
						chkConfig.checkpointSchedulerConfiguration.getClass());
					throw new IllegalArgumentException("Inconsistent checkpoint scheduler configuration.");
				}
			case HOURLY:
				if (chkConfig.checkpointSchedulerConfiguration instanceof CheckpointSchedulingStrategies.HourlySchedulerConfiguration) {

					final CheckpointSchedulingStrategies.HourlySchedulerConfiguration schedulerConfiguration;
					schedulerConfiguration = (CheckpointSchedulingStrategies.HourlySchedulerConfiguration) chkConfig.checkpointSchedulerConfiguration;

					// Override global configuration
					if (schedulerConfiguration.isIntervalSet()) {
						baseInterval = schedulerConfiguration.interval;
					}

					if (baseInterval < minPauseBetweenCheckpoints) {
						baseInterval = minPauseBetweenCheckpoints;
					}

					return new HourlyCheckpointScheduler(
						baseInterval,
						schedulerConfiguration.offsetMillis,
						minPauseBetweenCheckpoints,
						chkConfig.getCheckpointTimeout(),
						chkConfig.checkpointSchedulerConfiguration.earlyCheckpointConfig,
						job,
						coordinator);
				} else {
					LOG.warn("Inconsistent checkpoint scheduler configuration. A configuration with class {} has been recognized as HOURLY strategy.",
						chkConfig.checkpointSchedulerConfiguration.getClass());
					throw new IllegalArgumentException("Inconsistent checkpoint scheduler configuration.");
				}
			default:
				LOG.warn("Invalid checkpoint scheduling strategy: {}", chkConfig.checkpointSchedulerConfiguration);
				throw new IllegalArgumentException("Unsupported checkpoint scheduling strategy.");
		}
	}

	/**
	 * Factory method for early checkpoint task.
	 *
	 * @param maxRetry      maximum attempts to do checkpoint
	 * @param retryInterval retry interval between early checkpoint attempts
	 * @param coordinator   the checkpoint coordinator which is in charge of the checkpoint
	 * @return a runnable task to do early checkpoint
	 */
	static Runnable createEarlyCheckpointTask(long maxRetry, long retryInterval, CheckpointCoordinator coordinator) {
		return new TriggerFirstCheckpoint(maxRetry, retryInterval, coordinator);
	}

	/**
	 * Task to do early checkpoint, should be run in a single thread. Early checkpoint usually fails because
	 * not all operators to trigger are in RUNNING state. Therefore retry is necessary. To ensure that the
	 * first checkpoint is early enough, retry should be carried out with small interval (at least much
	 * smaller than the regular checkpoint interval).
	 */
	private static class TriggerFirstCheckpoint implements Runnable {

		/**
		 * Maximum number of trials.
		 */
		private final long maxRetry;

		/**
		 * The time (in ms) we wait to start another early checkpoint trial, if previous one fails.
		 */
		private final long retryInterval;

		/**
		 * The checkpoint coordinator which this scheduler belongs to.
		 */
		private final CheckpointCoordinator coordinator;

		private TriggerFirstCheckpoint(long maxRetry, long retryInterval, CheckpointCoordinator coordinator) {
			this.maxRetry = maxRetry;
			this.retryInterval = retryInterval;
			this.coordinator = coordinator;
		}

		@Override
		public void run() {
			for (int i = 0; i < maxRetry; i++) {
				// 'isPeriodic' arg is set to true because this invocation is logically part of periodical scheduling
				boolean success = coordinator.triggerCheckpoint(System.currentTimeMillis(), true);

				if (success) {
					LOG.info("Early Checkpoint successfully triggered in its {}'s trial.", i + 1);
					return;
				} else {
					LOG.warn("Early Checkpoint failed in its {}'s trial.", i + 1);
				}

				try {
					Thread.sleep(retryInterval);
				} catch (InterruptedException e) {
					LOG.error("Early checkpoint unexpectedly interrupted. Aborting early checkpoint.");
					break;
				}
			}

			LOG.warn("Early checkpoint failed.");
		}
	}


	public static void setupSavepointScheduler(
		CheckpointScheduler scheduler,
		String jobName,
		CheckpointCoordinator coordinator,
		CheckpointCoordinatorConfiguration chkConfig) {

		// time between savepoints
		long baseInterval;

		// max "in between duration" can be one year - this is to prevent numeric overflows
		long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
		if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
			minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
		}

		// setup periodic savepoint scheduler
		switch (chkConfig.getSavepointSchedulerConfiguration().strategy) {
			case DEFAULT:
				if (chkConfig.getSavepointSchedulerConfiguration() instanceof CheckpointSchedulingStrategies.DefaultSavepointSchedulerConfiguration) {

					final CheckpointSchedulingStrategies.DefaultSavepointSchedulerConfiguration savepointSchedulerConfiguration;
					savepointSchedulerConfiguration = (CheckpointSchedulingStrategies.DefaultSavepointSchedulerConfiguration) chkConfig.getSavepointSchedulerConfiguration();

					baseInterval = savepointSchedulerConfiguration.interval;

					if (baseInterval < minPauseBetweenCheckpoints) {
						baseInterval = minPauseBetweenCheckpoints;
					}

					scheduler.setPeriodSavepointScheduler(new SimplePeriodicSavepointScheduler(jobName, chkConfig.getSavepointLocationPrefix(), baseInterval, minPauseBetweenCheckpoints, coordinator));
					LOG.info("Setup savepoint scheduler with interval {}, minPause {}, prefix {}", baseInterval, minPauseBetweenCheckpoints, chkConfig.getSavepointLocationPrefix());
				} else {
					LOG.warn("Inconsistent savepoint scheduler configuration. A configuration with class {} has been recognized as Default strategy.",
						chkConfig.getSavepointSchedulerConfiguration().getClass());
					throw new IllegalArgumentException("Inconsistent savepoint scheduler configuration.");
				}
				break;
			default:
				LOG.warn("Invalid savepoint scheduling strategy: {}", chkConfig.getSavepointSchedulerConfiguration());
				throw new IllegalArgumentException("Unsupported savepoint scheduling strategy.");
		}
	}
}
