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

		switch (chkConfig.getCheckpointSchedulerConfiguration().strategy) {
			case DEFAULT:
				if (chkConfig.getCheckpointSchedulerConfiguration() instanceof CheckpointSchedulingStrategies.DefaultSchedulerConfiguration) {

					final CheckpointSchedulingStrategies.DefaultSchedulerConfiguration schedulerConfiguration;
					schedulerConfiguration = (CheckpointSchedulingStrategies.DefaultSchedulerConfiguration) chkConfig.getCheckpointSchedulerConfiguration();

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
						job,
						coordinator);
				} else {
					LOG.warn("Inconsistent checkpoint scheduler configuration. A configuration with class {} has been recognized as Default strategy.",
						chkConfig.getCheckpointSchedulerConfiguration().getClass());
					throw new IllegalArgumentException("Inconsistent checkpoint scheduler configuration.");
				}
			case HOURLY:
				if (chkConfig.getCheckpointSchedulerConfiguration() instanceof CheckpointSchedulingStrategies.HourlySchedulerConfiguration) {

					final CheckpointSchedulingStrategies.HourlySchedulerConfiguration schedulerConfiguration;
					schedulerConfiguration = (CheckpointSchedulingStrategies.HourlySchedulerConfiguration) chkConfig.getCheckpointSchedulerConfiguration();

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
						job,
						coordinator);
				} else {
					LOG.warn("Inconsistent checkpoint scheduler configuration. A configuration with class {} has been recognized as HOURLY strategy.",
						chkConfig.getCheckpointSchedulerConfiguration().getClass());
					throw new IllegalArgumentException("Inconsistent checkpoint scheduler configuration.");
				}
			default:
				LOG.warn("Invalid checkpoint scheduling strategy: {}", chkConfig.getCheckpointSchedulerConfiguration());
				throw new IllegalArgumentException("Unsupported checkpoint scheduling strategy.");
		}
	}
}
