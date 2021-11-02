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

package org.apache.flink.runtime.checkpoint.scheduler.savepoint;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default savepoint scheduler.
 */
public class SimplePeriodicSavepointScheduler implements PeriodicSavepointScheduler{
	private static final Logger LOG = LoggerFactory.getLogger(SimplePeriodicSavepointScheduler.class);
	/**
	 * Job unique id to identify a job. The job whose checkpoint this coordinator coordinates.
	 */
	private final String jobUID;

	/**
	 * The namespace for savepoints of a job, managed by platform.
	 */
	private final String namespace;

	/**
	 * Detach savepoint location prefix. Set by dynamic property state.savepoint.location-prefix.
	 */
	@Nullable
	private final String savepointLocationPrefix;

	/**
	 * The base savepoint interval. Actual trigger time may be affected by the
	 * max concurrent savepoints and minimum-pause values.
	 */
	private final long baseInterval;

	/**
	 * The min time (in ms) to delay after a savepoint could be triggered. Allows to
	 * enforce minimum processing time between savepoint attempts.
	 */
	private final long minPauseMillis;

	/**
	 * The savepoint coordinator which this scheduler belongs to.
	 */
	private final CheckpointCoordinator coordinator;

	public SimplePeriodicSavepointScheduler(
			@Nullable String namespace,
			String jobUID,
			@Nullable String savepointLocationPrefix,
			long baseInterval,
			long minPauseMillis,
			CheckpointCoordinator coordinator) {
		this.namespace = namespace;
		this.jobUID = jobUID;
		this.savepointLocationPrefix = savepointLocationPrefix;
		this.baseInterval = baseInterval;
		this.minPauseMillis = minPauseMillis;
		this.coordinator = coordinator;
	}

	@Override
	public ScheduledFuture<?> schedule(ScheduledExecutor timer) {
		return timer.scheduleWithFixedDelay(() -> {
			checkNotNull(savepointLocationPrefix, "savepoint directory prefix for periodic savepoint is not set, " +
				"set this value in config state.savepoint.location-prefix.");
			LocalDate currentDate = LocalDate.now();
			String dateSubDir = String.format("%04d%02d%02d", currentDate.getYear(), currentDate.getMonthValue(), currentDate.getDayOfMonth());
			String periodicSavepointPath;
			if (namespace != null) {
				periodicSavepointPath = String.format("%s/%s/%s/%s", savepointLocationPrefix, dateSubDir, jobUID, namespace);
			} else {
				periodicSavepointPath = String.format("%s/%s/%s", savepointLocationPrefix, dateSubDir, jobUID);
			}
			try {
				LOG.info("On triggering periodic savepoint at {}", periodicSavepointPath);
				coordinator.triggerSavepoint(periodicSavepointPath);
			} catch (Exception e) {
				LOG.error("Exception while triggering savepoint for job {}.", jobUID, e);
			}
		}, baseInterval, baseInterval, TimeUnit.MILLISECONDS);
	}
}
