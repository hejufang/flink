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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporterImpl;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for {@link RemoteBlacklistReporterImpl}.
 */
public class BlacklistReporterTest {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistReporterTest.class);

	@Test
	public void testReportFailure() throws Exception {
		ManualClock clock = new ManualClock();
		RemoteBlacklistReporterImpl blacklistReporter = new RemoteBlacklistReporterImpl(new JobID(), Time.seconds(10), Duration.ofMinutes(10), 10, clock);

		// throw IllegalStateException when reportFailure before ExecutionGraph is set.
		try {
			blacklistReporter.reportFailure(new ExecutionAttemptID(), new Exception("test"), clock.absoluteTimeMillis());
		} catch (IllegalStateException e) {}

		ExecutionAttemptID attemptId = new ExecutionAttemptID();
		Execution execution = mock(Execution.class);
		Map<ExecutionAttemptID, Execution> registeredExecutions = Collections.singletonMap(attemptId, execution);
		ExecutionGraph executionGraph = mock(ExecutionGraph.class);
		doReturn(registeredExecutions).when(executionGraph).getRegisteredExecutions();
		doReturn(new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true)).when(execution).getAssignedResourceLocation();

		blacklistReporter.setExecutionGraph(executionGraph);
		// will not throw any exception after ExecutionGraph is set.
		blacklistReporter.reportFailure(attemptId, new Exception("test"), clock.absoluteTimeMillis());
	}

	@Test
	public void testOnFailureWithLimiter() throws Exception {
		ManualClock clock = new ManualClock();
		RemoteBlacklistReporterImpl blacklistReporter = new RemoteBlacklistReporterImpl(new JobID(), Time.seconds(10), Duration.ofMinutes(10), 3, clock);
		ClassLoader userCodeLoader = ClassLoader.getSystemClassLoader();
		blacklistReporter.start(JobMasterId.generate(), ComponentMainThreadExecutorServiceAdapter.forMainThread());
		ResourceManagerGateway resourceManagerGateway = mock(ResourceManagerGateway.class);
		blacklistReporter.connectToResourceManager(resourceManagerGateway);

		blacklistReporter.onFailureWithLimiter(
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true),
				new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FAILED),
				userCodeLoader);

		// exception is null, will not reporter.
		verify(resourceManagerGateway, never()).onTaskFailure(
				any(JobID.class),
				any(JobMasterId.class),
				any(BlacklistUtil.FailureType.class),
				anyString(),
				any(ResourceID.class),
				any(Throwable.class),
				anyLong(),
				any(Time.class));

		// only reporter 2 times. the third report will be blocked by rate limiter.
		blacklistReporter.onFailureWithLimiter(
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true),
				new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FAILED, new Exception("test")),
				userCodeLoader);
		blacklistReporter.onFailureWithLimiter(
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true),
				new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FAILED, new Exception("test")),
				userCodeLoader);
		blacklistReporter.onFailureWithLimiter(
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true),
				new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FAILED, new Exception("test")),
				userCodeLoader);
		verify(resourceManagerGateway, times(2)).onTaskFailure(
				any(JobID.class),
				any(JobMasterId.class),
				any(BlacklistUtil.FailureType.class),
				anyString(),
				any(ResourceID.class),
				any(Throwable.class),
				anyLong(),
				any(Time.class));

		// rate limiter timeout.
		clock.advanceTime(11, TimeUnit.MINUTES);
		blacklistReporter.onFailureWithLimiter(
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), 1, true),
				new TaskExecutionState(new JobID(), new ExecutionAttemptID(), ExecutionState.FAILED, new Exception("test")),
				userCodeLoader);
		verify(resourceManagerGateway, times(3)).onTaskFailure(
				any(JobID.class),
				any(JobMasterId.class),
				any(BlacklistUtil.FailureType.class),
				anyString(),
				any(ResourceID.class),
				any(Throwable.class),
				anyLong(),
				any(Time.class));
	}
}
