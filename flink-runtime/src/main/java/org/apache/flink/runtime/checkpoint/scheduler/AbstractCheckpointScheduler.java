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

import org.apache.flink.runtime.checkpoint.scheduler.savepoint.PeriodicSavepointScheduler;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Abstract checkpoint scheduler implementation. Inject savepoint scheduler if it is defined.
 */
public abstract class AbstractCheckpointScheduler implements CheckpointScheduler {
	ScheduledFuture<?> periodSavepointTrigger = null;

	/**
	 * The timer that handles the checkpoint timeouts and triggers periodic checkpoints.
	 */
	protected ScheduledThreadPoolExecutor timer;

	private PeriodicSavepointScheduler periodSavepointScheduler;

	public AbstractCheckpointScheduler() {
		// initialize the thread pool of parallelism one to trigger checkpoint
		this.timer = new ScheduledThreadPoolExecutor(1,
			new DispatcherThreadFactory(Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

		// make sure the timer internally cleans up and does not hold onto stale scheduled tasks
		this.timer.setRemoveOnCancelPolicy(true);
		this.timer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.timer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
	}

	@Override
	public void startScheduling() {
		if (periodSavepointScheduler != null) {
			periodSavepointTrigger = periodSavepointScheduler.schedule(timer);
		}
	}

	@Override
	public void stopScheduling() {
		if (periodSavepointTrigger != null) {
			periodSavepointTrigger.cancel(false);
			periodSavepointTrigger = null;
		}
	}

	@Override
	public void setPeriodSavepointScheduler(PeriodicSavepointScheduler periodSavepointScheduler) {
		this.periodSavepointScheduler = periodSavepointScheduler;
	}
}
