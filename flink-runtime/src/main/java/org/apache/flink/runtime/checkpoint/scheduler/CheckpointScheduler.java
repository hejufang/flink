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
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import java.util.concurrent.ScheduledFuture;

/**
 * Interface for checkpoint scheduler.
 * Mutators should be called with the lock of {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator}.
 * Therefore none of them should block or carry out time-consuming operations. Also, after method invocation, the
 * state of scheduler must change synchronously.
 */
public interface CheckpointScheduler {

	void shutdownNow();

	/**
	 * Start the periodically scheduler and transfer from stopped state to running state.
	 * Should be only called once after the coordinator started.
	 */
	void startScheduling();

	/**
	 * stop periodically scheduling, quit periodically scheduling mode.
	 * Should be only called once before the job quit.
	 */
	void stopScheduling();

	/**
	 * Schedule a cleanup thread in case the checkpoint expires before it completes.
	 */
	ScheduledFuture<?> scheduleTimeoutCanceller(Runnable canceller);

	/**
	 * Schedule a cleanup thread in case the checkpoint expires before it completes.
	 */
	default ScheduledFuture<?> scheduleTimeoutCanceller(long timeout, Runnable canceller) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Adjust scheduling time to ensure the time between the last and next checkpoint is
	 * greater than a predefined value (in ns for exact measurement of time).
	 *
	 * @param tillNextMillis should wait for the tillNextMillis to trigger the next checkpoint (in ms)
	 */
	void rescheduleNextCheckpointTrigger(long tillNextMillis);

	/**
	 * Returns whether periodic checkpointing has been configured.
	 *
	 * @return <code>true</code> if periodic checkpoints have been configured.
	 */
	boolean isPeriodicCheckpointingConfigured();

	/**
	 * @return True if the scheduler is still scheduling tasks continuously (not in paused mode)
	 */
	boolean isScheduling();

	/**
	 * For test.
	 * @return True if the scheduler is scheduling periodically now (in running state).
	 */
	boolean inPeriodicallyScheduling();

	void setTimer(ScheduledExecutor timer);

	void setPeriodSavepointScheduler(PeriodicSavepointScheduler savepointScheduler);
}