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
import org.apache.flink.runtime.blacklist.tracker.BlacklistTrackerImpl;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for blacklist tracker.
 */
public class BlacklistTrackerImplTest {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistTrackerImplTest.class);

	@Test
	public void testBlacklistAdd() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				2, 2, 3, 3,
				Time.seconds(60), Time.seconds(1), false, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
				ComponentMainThreadExecutorServiceAdapter.forMainThread(),
				() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new RuntimeException("exception2"), System.currentTimeMillis());
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host1"));
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 1);
		blacklistTracker.close();
	}

	@Test
	public void testFailureOutdatedExcess() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				2, 2, 3, 3,
				Time.milliseconds(1000), Time.milliseconds(250), false, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
				ComponentMainThreadExecutorServiceAdapter.forMainThread(),
				() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new RuntimeException("exception2"), System.currentTimeMillis() + 2000);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host1"));
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 1);
		Thread.sleep(2000);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().isEmpty());
		blacklistTracker.close();
	}

	@Test
	public void testBlacklistLengthExcess() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
				ComponentMainThreadExecutorServiceAdapter.forMainThread(),
				() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource3"), new RuntimeException("exception2"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 2);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host1") && blacklistTracker.getBlackedHosts().containsKey("host2"));
		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 2);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host2") && blacklistTracker.getBlackedHosts().containsKey("host3"));
		blacklistTracker.close();
	}

	@Test
	public void testMultiTypeFailure() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
				ComponentMainThreadExecutorServiceAdapter.forMainThread(),
				() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource2"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host2", new ResourceID("resource3"), new RuntimeException("exception2"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 2);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host1") && blacklistTracker.getBlackedHosts().containsKey("host2"));
		blacklistReporter.onFailure("host3", new ResourceID("resource3"), new RuntimeException("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 2);
		Assert.assertTrue(blacklistTracker.getBlackedHosts().containsKey("host2") && blacklistTracker.getBlackedHosts().containsKey("host3"));

		BlacklistReporter taskBlacklistReporter = new BlacklistReporter() {
			@Override
			public void onFailure(String hostname, ResourceID resourceID, Throwable t, long timestamp) {
				blacklistTracker.onFailure(BlacklistUtil.FailureType.TASK, hostname, resourceID, t, timestamp);
			}

			@Override
			public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) { }
		};
		taskBlacklistReporter.onFailure("host4", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 3);
		taskBlacklistReporter.onFailure("host5", new ResourceID("resource2"), new RuntimeException("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 3);

		blacklistTracker.close();
	}

	@Test
	public void testCriticalFailure() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
			3, 3, 1, 2,
			Time.seconds(60), Time.seconds(1), true, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
			ComponentMainThreadExecutorServiceAdapter.forMainThread(),
			() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		blacklistReporter.onFailure("host1", new ResourceID("resource2"), new CriticalExceptionTest("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 1);

		blacklistTracker.close();
	}

	@Test
	public void testIgnoreClass() throws Exception {
		BlacklistTrackerImpl blacklistTracker = new BlacklistTrackerImpl(
				1, 1, 1, 2,
				Time.seconds(60), Time.seconds(1), false, UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());
		blacklistTracker.start(
				ComponentMainThreadExecutorServiceAdapter.forMainThread(),
				() -> LOG.info("Blacklist Actions received blacklist updated."));
		BlacklistReporter blacklistReporter = new LocalBlacklistReporterImpl(blacklistTracker);
		blacklistReporter.addIgnoreExceptionClass(RuntimeException.class);
		blacklistReporter.onFailure("host1", new ResourceID("resource1"), new RuntimeException("exception1"), System.currentTimeMillis());
		Assert.assertTrue(blacklistTracker.getBlackedHosts().isEmpty());
		blacklistReporter.onFailure("host2", new ResourceID("resource1"), new Exception("exception1"), System.currentTimeMillis());
		Assert.assertEquals(blacklistTracker.getBlackedHosts().size(), 1);
		blacklistTracker.close();
	}
}
