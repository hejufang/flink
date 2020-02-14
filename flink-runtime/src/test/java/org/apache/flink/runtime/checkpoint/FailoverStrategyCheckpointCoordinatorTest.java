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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.AdaptedRestartPipelinedRegionStrategyNG;
import org.apache.flink.runtime.executiongraph.failover.FailoverRegion;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the interaction between the {@link FailoverStrategy} and the {@link CheckpointCoordinator}.
 */
public class FailoverStrategyCheckpointCoordinatorTest extends TestLogger {
	private ManuallyTriggeredScheduledExecutor manualThreadExecutor;

	@Before
	public void setUp() {
		manualThreadExecutor = new ManuallyTriggeredScheduledExecutor();
	}

	/**
	 * Tests that {@link CheckpointCoordinator#abortPendingCheckpoints(CheckpointException)}
	 * called by {@link AdaptedRestartPipelinedRegionStrategyNG} or {@link FailoverRegion} could handle
	 * the {@code currentPeriodicTrigger} null situation well.
	 */
	@Test
	public void testAbortPendingCheckpointsWithTriggerValidation() {
		final int maxConcurrentCheckpoints = ThreadLocalRandom.current().nextInt(10) + 1;
		ExecutionVertex executionVertex = mockExecutionVertex();
		CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = new CheckpointCoordinatorConfiguration(
			Integer.MAX_VALUE,
			Integer.MAX_VALUE,
			0,
			maxConcurrentCheckpoints,
			CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
			true,
			false,
			0);
		CheckpointCoordinator checkpointCoordinator = new CheckpointCoordinator(
			new JobID(),
			checkpointCoordinatorConfiguration,
			new ExecutionVertex[] { executionVertex },
			new ExecutionVertex[] { executionVertex },
			new ExecutionVertex[] { executionVertex },
			new StandaloneCheckpointIDCounter(),
			new StandaloneCompletedCheckpointStore(1),
			new MemoryStateBackend(),
			Executors.directExecutor(),
			SharedStateRegistry.DEFAULT_FACTORY,
			mock(CheckpointFailureManager.class));

		// switch current execution's state to running to allow checkpoint could be triggered.
		mockExecutionRunning(executionVertex);

		// use manual checkpoint timer to trigger period checkpoints as we expect.
		ManualCheckpointTimer manualCheckpointTimer = new ManualCheckpointTimer(manualThreadExecutor);
		// set the init delay as 0 to ensure first checkpoint could be triggered once we trigger the manual executor
		// this is used to avoid the randomness of when to trigger the first checkpoint (introduced via FLINK-9352)
		manualCheckpointTimer.setManualDelay(0L);
		Whitebox.setInternalState(checkpointCoordinator, "timer", manualCheckpointTimer);

		checkpointCoordinator.startCheckpointScheduler();
		assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		manualThreadExecutor.triggerAll();
		manualThreadExecutor.triggerScheduledTasks();
		assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

		for (int i = 1; i < maxConcurrentCheckpoints; i++) {
			checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), false);
			assertEquals(i + 1, checkpointCoordinator.getNumberOfPendingCheckpoints());
			assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		}

		// as we only support limited concurrent checkpoints, after checkpoint triggered more than the limits,
		// the currentPeriodicTrigger would been assigned as null.
		checkpointCoordinator.triggerCheckpoint(System.currentTimeMillis(), false);
		assertFalse(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		assertEquals(maxConcurrentCheckpoints, checkpointCoordinator.getNumberOfPendingCheckpoints());

		checkpointCoordinator.abortPendingCheckpoints(
			new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));
		// after aborting checkpoints, we ensure currentPeriodicTrigger still available.
		assertTrue(checkpointCoordinator.isCurrentPeriodicTriggerAvailable());
		assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
	}

	private ExecutionVertex mockExecutionVertex() {
		ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
		ExecutionVertex executionVertex = mock(ExecutionVertex.class);
		Execution execution = Mockito.mock(Execution.class);
		when(execution.getAttemptId()).thenReturn(executionAttemptID);
		when(executionVertex.getMainExecution()).thenReturn(execution);
		return executionVertex;
	}

	private void mockExecutionRunning(ExecutionVertex executionVertex) {
		when(executionVertex.getMainExecution().getState()).thenReturn(ExecutionState.RUNNING);
	}

	public static class ManualCheckpointTimer extends ScheduledThreadPoolExecutor {
		private final ScheduledExecutor scheduledExecutor;
		private long manualDelay = 0;

		ManualCheckpointTimer(final ScheduledExecutor scheduledExecutor) {
			super(0);
			this.scheduledExecutor = scheduledExecutor;
		}

		void setManualDelay(long manualDelay) {
			this.manualDelay = manualDelay;
		}

		@Override
		public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
			// used as checkpoint canceller, as we don't want pending checkpoint cancelled, this should never be scheduled.
			return new NeverCompleteFuture(delay);
		}

		@Override
		public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
			// used to schedule periodic checkpoints.
			// this would use configured 'manualDelay' to let the task schedule with the wanted delay.
			return scheduledExecutor.scheduleWithFixedDelay(command, manualDelay, period, unit);
		}

		@Override
		public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void execute(Runnable command) {
			scheduledExecutor.execute(command);
		}
	}
}
