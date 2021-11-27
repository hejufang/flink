/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.blacklist.reporter.NoOpBlacklistReporterImpl;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TokenExpirationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint failure manager which centralized manage checkpoint failure processing logic.
 */
public class CheckpointFailureManager {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointFailureManager.class);

	public static final int UNLIMITED_TOLERABLE_FAILURE_NUMBER = Integer.MAX_VALUE;

	private static final String CONTINUOUS_FAILURE_CHECKPOINT_COUNT = "numberOfContinuousCheckpointFailure";

	private final int tolerableCpFailureNumber;
	private final FailJobCallback failureCallback;
	private final AtomicInteger continuousFailureCounter;
	private final Set<Long> countedCheckpointIds;

	private final RemoteBlacklistReporter reporter;

	private final MetricGroup metricGroup;

	// only count failures defined in bytedance
	private final Set<Long> failureCheckpointIdsInByte;
	private final AtomicInteger continuousFailureCheckpointCountInByte = new AtomicInteger(0);

	@VisibleForTesting
	public CheckpointFailureManager(int tolerableCpFailureNumber, FailJobCallback failureCallback) {
		this(tolerableCpFailureNumber, failureCallback, new NoOpBlacklistReporterImpl(), new UnregisteredMetricsGroup());
	}

	public CheckpointFailureManager(
			int tolerableCpFailureNumber,
			FailJobCallback failureCallback,
			RemoteBlacklistReporter reporter,
			MetricGroup metricGroup) {
		checkArgument(tolerableCpFailureNumber >= 0,
				"The tolerable checkpoint failure number is illegal, " +
						"it must be greater than or equal to 0 .");
		this.tolerableCpFailureNumber = tolerableCpFailureNumber;
		this.continuousFailureCounter = new AtomicInteger(0);
		this.failureCallback = checkNotNull(failureCallback);
		this.countedCheckpointIds = ConcurrentHashMap.newKeySet();
		this.reporter = reporter;
		this.failureCheckpointIdsInByte = ConcurrentHashMap.newKeySet();
		this.metricGroup = metricGroup;
		this.metricGroup.gauge(CONTINUOUS_FAILURE_CHECKPOINT_COUNT, () -> continuousFailureCheckpointCountInByte);
	}

	/**
	 * Handle job level checkpoint exception with a handler callback.
	 *
	 * @param exception the checkpoint exception.
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence. In trigger phase, we may not get the checkpoint id when the failure
	 *                     happens before the checkpoint id generation. In this case, it will be specified a negative
	 *                      latest generated checkpoint id as a special flag.
	 */
	public void handleJobLevelCheckpointException(CheckpointException exception, long checkpointId) {
		// always fail the job if token expired happens on JM's side
		checkTokenProblemInTraces(exception);

		checkContinuousCheckpointCount(exception, checkpointId);
		checkFailureCounter(exception, checkpointId);
		if (continuousFailureCounter.get() > tolerableCpFailureNumber) {
			clearCount();
			failureCallback.failJob(new FlinkRuntimeException("Exceeded checkpoint tolerable failure threshold."));
		}
	}

	/**
	 * Handle task level checkpoint exception with a handler callback.
	 *
	 * @param exception the checkpoint exception.
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence. In trigger phase, we may not get the checkpoint id when the failure
	 *                     happens before the checkpoint id generation. In this case, it will be specified a negative
	 *                      latest generated checkpoint id as a special flag.
	 * @param executionAttemptID the execution attempt id, as a safe guard.
	 */
	public void handleTaskLevelCheckpointException(
			CheckpointException exception,
			long checkpointId,
			ExecutionAttemptID executionAttemptID) {

		// report failure if token problem
		if (TokenExpirationUtils.isTokenProblemInTraces(exception)) {
			reporter.reportFailure(executionAttemptID, exception, System.currentTimeMillis());
		}

		checkContinuousCheckpointCount(exception, checkpointId);
		checkFailureCounter(exception, checkpointId);
		if (continuousFailureCounter.get() > tolerableCpFailureNumber) {
			clearCount();
			failureCallback.failJobDueToTaskFailure(new FlinkRuntimeException("Exceeded checkpoint tolerable failure threshold."), executionAttemptID);
		}
	}

	/**
	 * If the throwable is caused, directly or indirectly, by expired tokens, we need to for the whole job to restart.
	 */
	private void checkTokenProblemInTraces(Throwable throwable) {
		if (TokenExpirationUtils.isTokenProblemInTraces(throwable)) {
			LOG.error("Temporary fix to invalid token problem: kill the job and force it to restart.", throwable);
			System.exit(1); // anything but zero
		}
	}

	private void checkContinuousCheckpointCount(CheckpointException exception, long checkpointId) {
		switch (exception.getCheckpointFailureReason()) {
			case CHECKPOINT_ASYNC_EXCEPTION:
			case CHECKPOINT_EXPIRED:
			case FINALIZE_CHECKPOINT_FAILURE:
			case CHECKPOINT_DECLINED:
				if (failureCheckpointIdsInByte.add(checkpointId)) {
					continuousFailureCheckpointCountInByte.incrementAndGet();
				}
				break;
		}
	}

	public void checkFailureCounter(
			CheckpointException exception,
			long checkpointId) {
		if (tolerableCpFailureNumber == UNLIMITED_TOLERABLE_FAILURE_NUMBER) {
			return;
		}

		CheckpointFailureReason reason = exception.getCheckpointFailureReason();
		switch (reason) {
			case PERIODIC_SCHEDULER_SHUTDOWN:
			case TOO_MANY_CONCURRENT_CHECKPOINTS:
			case TOO_MANY_CHECKPOINT_REQUESTS:
			case MINIMUM_TIME_BETWEEN_CHECKPOINTS:
			case NOT_ALL_REQUIRED_TASKS_RUNNING:
			case CHECKPOINT_SUBSUMED:
			case CHECKPOINT_COORDINATOR_SUSPEND:
			case CHECKPOINT_COORDINATOR_SHUTDOWN:
			case JOB_FAILURE:
			case JOB_FAILOVER_REGION:
			//for compatibility purposes with user job behavior
			case CHECKPOINT_DECLINED_TASK_NOT_READY:
			case CHECKPOINT_DECLINED_TASK_CLOSING:
			case CHECKPOINT_DECLINED_TASK_NOT_CHECKPOINTING:
			case CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED:
			case CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER:
			case CHECKPOINT_DECLINED_SUBSUMED:
			case CHECKPOINT_DECLINED_INPUT_END_OF_STREAM:

			case EXCEPTION:
			case CHECKPOINT_ASYNC_EXCEPTION:
			case TASK_FAILURE:
			case TASK_CHECKPOINT_FAILURE:
			case UNKNOWN_TASK_CHECKPOINT_NOTIFICATION_FAILURE:
			case TRIGGER_CHECKPOINT_FAILURE:
			case FINALIZE_CHECKPOINT_FAILURE:
				//ignore
				break;

			case CHECKPOINT_DECLINED:
			case CHECKPOINT_EXPIRED:
				//we should make sure one checkpoint only be counted once
				if (countedCheckpointIds.add(checkpointId)) {
					continuousFailureCounter.incrementAndGet();
				}

				break;

			default:
				throw new FlinkRuntimeException("Unknown checkpoint failure reason : " + reason.name());
		}
	}

	/**
	 * Handle checkpoint success.
	 *
	 * @param checkpointId the failed checkpoint id used to count the continuous failure number based on
	 *                     checkpoint id sequence.
	 */
	public void handleCheckpointSuccess(@SuppressWarnings("unused") long checkpointId) {
		clearCount();
	}

	private void clearCount() {
		continuousFailureCounter.set(0);
		continuousFailureCheckpointCountInByte.set(0);
		countedCheckpointIds.clear();
		failureCheckpointIdsInByte.clear();
	}

	/**
	 * Fails the whole job graph in case an in-progress synchronous savepoint is discarded.
	 *
	 * <p>If the checkpoint was cancelled at the checkpoint coordinator, i.e. before
	 * the synchronous savepoint barrier was sent to the tasks, then we do not cancel the job
	 * as we do not risk having a deadlock.
	 *
	 * @param cause The reason why the job is cancelled.
	 * */
	void handleSynchronousSavepointFailure(final Throwable cause) {
		if (!isPreFlightFailure(cause)) {
			failureCallback.failJob(cause);
		}
	}

	private static boolean isPreFlightFailure(final Throwable cause) {
		return ExceptionUtils.findThrowable(cause, CheckpointException.class)
				.map(CheckpointException::getCheckpointFailureReason)
				.map(CheckpointFailureReason::isPreFlight)
				.orElse(false);
	}

	/**
	 * A callback interface about how to fail a job.
	 */
	public interface FailJobCallback {

		/**
		 * Fails the whole job graph.
		 *
		 * @param cause The reason why the synchronous savepoint fails.
		 */
		void failJob(final Throwable cause);

		/**
		 * Fails the whole job graph due to task failure.
		 *
		 * @param cause The reason why the job is cancelled.
		 * @param failingTask The id of the failing task attempt to prevent failing the job multiple times.
		 */
		void failJobDueToTaskFailure(final Throwable cause, final ExecutionAttemptID failingTask);

	}

}
