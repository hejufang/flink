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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blacklist.reporter.BlacklistReporter;
import org.apache.flink.runtime.blacklist.reporter.LocalBlacklistReporterImpl;
import org.apache.flink.runtime.blacklist.tracker.BlackedExceptionAccuracy;
import org.apache.flink.runtime.blacklist.tracker.BlacklistTrackerImpl;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Test for blacklist tracker.
 */
public class BlacklistTrackerImplTest {
	@Test
	public void testBlacklistAdd() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				2, 2, 3, 3,
				Time.seconds(60), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(mainThreadExecutor, createTestingBlacklistActions());
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new RuntimeException("exception2"), clock.absoluteTimeMillis());
		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertTrue(blacklistRecord.getBlackedHosts().contains("host1"));
		Assert.assertEquals(1, blacklistRecord.getBlackedHosts().size());
		Assert.assertEquals(2, blacklistRecord.getBlackedResources().size());
		blacklistTracker.close();
	}

	@Test
	public void testFailureOutdatedExcess() throws Exception {
		ManualClock clock = new ManualClock();
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				2, 2, 3, 3,
				Time.milliseconds(1000), Time.milliseconds(250), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new RuntimeException("exception2"), clock.absoluteTimeMillis() + 2000);
		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertTrue(blacklistRecord.getBlackedHosts().contains("host1"));
		Assert.assertEquals(1, blacklistRecord.getBlackedHosts().size());
		clock.advanceTime(2000, TimeUnit.MILLISECONDS);

		executor.triggerNonPeriodicScheduledTask();
		blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertTrue(blacklistRecord.getBlackedHosts().isEmpty());
		blacklistTracker.close();
	}

	@Test
	public void testBlacklistLengthExcess() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, 100, 5, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource3"), new RuntimeException("exception2"), clock.absoluteTimeMillis());
		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertThat(blacklistRecord.getBlackedHosts(), containsInAnyOrder("host1", "host2"));
		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(2, blacklistRecord.getBlackedHosts().size());
		Assert.assertThat(blacklistRecord.getBlackedHosts(), containsInAnyOrder("host2", "host3"));
		blacklistTracker.close();
	}

	@Test
	public void testMultiTypeFailure() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, 100, 10, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource3"), new RuntimeException("exception2"), clock.absoluteTimeMillis());
		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(2, blacklistRecord.getBlackedHosts().size());
		Assert.assertThat(blacklistRecord.getBlackedHosts(), containsInAnyOrder("host1", "host2"));

		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(2, blacklistRecord.getBlackedHosts().size());
		Assert.assertThat(blacklistRecord.getBlackedHosts(), containsInAnyOrder("host2", "host3"));

		BlacklistReporter taskBlacklistReporter = createBlacklistReporter(blacklistTracker, BlacklistUtil.FailureType.UNKNOWN);

		taskBlacklistReporter.onFailure("host4", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		BlacklistRecord taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK, blacklistTracker);
		Assert.assertEquals(1, taskBlacklistRecord.getBlackedHosts().size());
		Assert.assertThat(taskBlacklistRecord.getBlackedHosts(), containsInAnyOrder("host4"));

		clock.advanceTime(1, TimeUnit.MILLISECONDS);
		taskBlacklistReporter.onFailure("host5", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK, blacklistTracker);
		Assert.assertEquals(1, taskBlacklistRecord.getBlackedHosts().size());
		Assert.assertThat(taskBlacklistRecord.getBlackedHosts(), containsInAnyOrder("host5"));

		blacklistTracker.close();
	}

	@Test
	public void testCriticalFailure() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
			3, 3, 1, 2,
			Time.seconds(60), Time.seconds(1), true, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(mainThreadExecutor, createTestingBlacklistActions());

		BlacklistReporter blacklistReporter = createBlacklistReporter(blacklistTracker, BlacklistUtil.FailureType.UNKNOWN);

		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new CriticalExceptionTest("exception1"), clock.absoluteTimeMillis());

		BlacklistRecord taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.CRITICAL_EXCEPTION, blacklistTracker);
		Assert.assertEquals(BlacklistUtil.FailureActionType.RELEASE_BLACKED_HOST, taskBlacklistRecord.getActionType());
		Assert.assertEquals(1, taskBlacklistRecord.getBlackedHosts().size());
		Assert.assertThat(taskBlacklistRecord.getBlackedHosts(), containsInAnyOrder("host1"));

		blacklistTracker.close();
	}

	@Test
	public void testIgnoreClass() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.addIgnoreExceptionClass(RuntimeException.class);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());

		BlacklistRecord taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(0, taskBlacklistRecord.getBlackedHosts().size());

		blacklistReporter.onFailure("host2", new ResourceID("resource1"), new Exception("exception1"), clock.absoluteTimeMillis());
		taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(1, taskBlacklistRecord.getBlackedHosts().size());
		blacklistTracker.close();
	}

	@Test
	public void testUpdateExceptionFilterThreshold() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		TestingBlacklistActions testingBlacklistActions = new TestingBlacklistActions.TestingBlacklistActionsBuilder()
				.setRegisteredWorkerNumberSupplier(
						() -> 100)
				.build();

		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				testingBlacklistActions);

		Assert.assertEquals(blacklistTracker.getMaxHostPerExceptionNumber().size(), 2);
		Assert.assertEquals(3, (long) blacklistTracker.getMaxHostPerExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));
		Assert.assertEquals(3, (long) blacklistTracker.getMaxHostPerExceptionNumber().get(BlacklistUtil.FailureType.TASK));

		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();

		Assert.assertEquals(blacklistTracker.getMaxHostPerExceptionNumber().size(), 2);
		Assert.assertEquals(5, (long) blacklistTracker.getMaxHostPerExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));
		Assert.assertEquals(5, (long) blacklistTracker.getMaxHostPerExceptionNumber().get(BlacklistUtil.FailureType.TASK));
		blacklistTracker.close();
	}

	@Test
	public void testReportBlackedRecordAccuracy() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);
		TestingBlacklistActions testingBlacklistActions = new TestingBlacklistActions.TestingBlacklistActionsBuilder()
				.setRegisteredWorkerNumberSupplier(
						() -> 100)
				.build();

		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 2, 2, 3,
				Time.seconds(600), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				testingBlacklistActions);

		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		// two host is blacked for RuntimeException and Exception.
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource3"), new Exception("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource4"), new Exception("exception1"), clock.absoluteTimeMillis());

		BlacklistRecord taskBlacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(2, taskBlacklistRecord.getBlackedHosts().size());
		Assert.assertThat(taskBlacklistRecord.getBlackedHosts(), containsInAnyOrder("host1", "host2"));

		// check these two exception is unknown.
		BlackedExceptionAccuracy blackedExceptionAccuracy = blacklistTracker.getBlackedExceptionAccuracies().get(BlacklistUtil.FailureType.TASK_MANAGER);
		Assert.assertEquals(2, blackedExceptionAccuracy.getUnknownBlackedException().size());

		// RuntimeException occur after 65s, check RuntimeException will be marked as Wrong.
		clock.advanceTime(65_000L, TimeUnit.MILLISECONDS);
		blacklistReporter.onFailure("host3", new ResourceID("resource5"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blackedExceptionAccuracy = blacklistTracker.getBlackedExceptionAccuracies().get(BlacklistUtil.FailureType.TASK_MANAGER);
		Assert.assertEquals(1, blackedExceptionAccuracy.getWrongBlackedException().size());
		Assert.assertEquals(1, blackedExceptionAccuracy.getUnknownBlackedException().size());

		blacklistReporter.onFailure("host3", new ResourceID("resource5"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blackedExceptionAccuracy = blacklistTracker.getBlackedExceptionAccuracies().get(BlacklistUtil.FailureType.TASK_MANAGER);
		Assert.assertEquals(1, blackedExceptionAccuracy.getWrongBlackedException().size());
		Assert.assertEquals(1, blackedExceptionAccuracy.getUnknownBlackedException().size());

		// Nothing happen in 365s, check Exception will be marked as Right.
		clock.advanceTime(365_000L, TimeUnit.MILLISECONDS);
		blackedExceptionAccuracy = blacklistTracker.getBlackedExceptionAccuracies().get(BlacklistUtil.FailureType.TASK_MANAGER);
		Assert.assertEquals(1, blackedExceptionAccuracy.getWrongBlackedException().size());
		Assert.assertEquals(1, blackedExceptionAccuracy.getRightBlackedException().size());

		blacklistTracker.close();
	}

	@Test
	public void testExceptionFiltered() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);

		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 2, 1, 2,
				Time.seconds(600), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());

		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		// two host is blacked for RuntimeException and Exception.
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host4", new ResourceID("resource4"), new RuntimeException("exception1"), clock.absoluteTimeMillis());

		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(0, blacklistRecord.getBlackedHosts().size());
		Assert.assertEquals(1, (long) blacklistTracker.getFilteredExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));

		// failure timeout.
		clock.advanceTime(601_000L, TimeUnit.MILLISECONDS);
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();

		blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(0, blacklistRecord.getBlackedHosts().size());
		Assert.assertEquals(0, (long) blacklistTracker.getFilteredExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));

		blacklistTracker.close();
	}

	@Test
	public void testExceptionFilteredRenew() throws Exception {
		ManualClock clock = new ManualClock();
		ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
		ComponentMainThreadExecutor mainThreadExecutor = createManuallyTriggeredMainThreadExecutor(executor);

		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 2, 1, 2,
				Time.seconds(600), Time.seconds(1), false, 100, 3, 0.05, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(), clock);
		blacklistTracker.start(
				mainThreadExecutor,
				createTestingBlacklistActions());

		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		// two host is blacked for RuntimeException and Exception.
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		blacklistReporter.onFailure("host4", new ResourceID("resource4"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		BlacklistRecord blacklistRecord = getBlacklistRecordByType(BlacklistUtil.FailureType.TASK_MANAGER, blacklistTracker);
		Assert.assertEquals(0, blacklistRecord.getBlackedHosts().size());
		Assert.assertEquals(1, (long) blacklistTracker.getFilteredExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));

		// renew filtered exception.
		clock.advanceTime(301_000L, TimeUnit.MILLISECONDS);
		blacklistReporter.onFailure("host5", new ResourceID("resource5"), new RuntimeException("exception1"), clock.absoluteTimeMillis());
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		Assert.assertEquals(1, (long) blacklistTracker.getFilteredExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));

		// failure(resource1~4) timeout, left failure(resource5)
		clock.advanceTime(301_000L, TimeUnit.MILLISECONDS);
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		executor.triggerNonPeriodicScheduledTask();
		Assert.assertEquals(1, (long) blacklistTracker.getFilteredExceptionNumber().get(BlacklistUtil.FailureType.TASK_MANAGER));

		blacklistTracker.close();
	}

	public TestingBlacklistActions createTestingBlacklistActions() {
		return new TestingBlacklistActions.TestingBlacklistActionsBuilder().build();
	}

	public ComponentMainThreadExecutor createManuallyTriggeredMainThreadExecutor(ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor) {
		final Thread main = Thread.currentThread();
		return new ComponentMainThreadExecutorServiceAdapter(
				manuallyTriggeredScheduledExecutor,
				main);
	}

	public BlacklistReporter createBlacklistReporter(BlacklistTrackerImpl blacklistTracker, BlacklistUtil.FailureType failureType) {
		return new BlacklistReporter() {
			@Override
			public void onFailure(String hostname, ResourceID resourceID, Throwable t, long timestamp) {
				blacklistTracker.onFailure(failureType, hostname, resourceID, t, timestamp);
			}

			@Override
			public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) { }
		};
	}

	public BlacklistRecord getBlacklistRecordByType(BlacklistUtil.FailureType failureType, BlacklistTrackerImpl blacklistTracker) {
		return blacklistTracker.getBlackedRecords().stream().filter(r -> r.getFailureType().equals(failureType)).findFirst().get();
	}

}
