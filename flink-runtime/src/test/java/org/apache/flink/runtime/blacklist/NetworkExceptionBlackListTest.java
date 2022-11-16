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
import org.apache.flink.runtime.blacklist.tracker.BlacklistTrackerImpl;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.io.network.NetworkAddress;
import org.apache.flink.runtime.io.network.NetworkTraceable;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Class for testing blacklist mechanism when having network exceptions.
 */
public class NetworkExceptionBlackListTest {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkExceptionBlackListTest.class);

	private NetworkExceptionSimulator networkExceptionSimulator;
	private BlacklistTrackerImpl blacklistTracker;
	private final ManualClock clock = new ManualClock();
	private final ManuallyTriggeredScheduledExecutor executor = new ManuallyTriggeredScheduledExecutor();
	private final AlwaysReturnsTaskManagerAliveAction alwaysReturnsTaskManagerAliveAction = new AlwaysReturnsTaskManagerAliveAction();

	@Before
	public void setup() {
		blacklistTracker = new BlacklistTrackerImpl(
				2,
				2,
				3,
				3,
				Time.seconds(60),
				Time.seconds(1),
				false,
				100,
				3,
				0.05,
				Collections.emptyList(),
				Time.seconds(30),
				Time.minutes(30),
				Time.seconds(60),
				0.5f,
				0.5f,
				5,
				0.5f,
				UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
				clock);
		blacklistTracker.start(
				executor,
				BlacklistTestUtils.createManuallyTriggeredMainThreadExecutor(executor),
				alwaysReturnsTaskManagerAliveAction);
		networkExceptionSimulator = new NetworkExceptionSimulator(blacklistTracker, clock);
	}

	@Test
	public void testSingleHostCorrupted() {
		networkExceptionSimulator.start();
		String badHost = networkExceptionSimulator.triggerNetworkExceptionInOneHost();
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(1, blacklist.size());
		Assert.assertTrue(blacklist.contains(badHost));
	}

	@Test
	public void testSpecificHostCorrupted() {
		networkExceptionSimulator.start();
		networkExceptionSimulator.triggerNetworkExceptionInHost("host8");
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(1, blacklist.size());
		Assert.assertTrue(blacklist.contains("host8"));
	}

	@Test
	public void testMultipleHostCorrupted() {
		networkExceptionSimulator.start();
		networkExceptionSimulator.triggerNetworkExceptionInHost("host8", "host2");
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(2, blacklist.size());
		Assert.assertTrue(blacklist.contains("host8"));
		Assert.assertTrue(blacklist.contains("host2"));
	}

	@Test
	public void testNetworkCorruptedHappenedAgainAfterBlackedOneHost() {
		networkExceptionSimulator.start();
		networkExceptionSimulator.triggerNetworkExceptionInHost("host8");
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist1 = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(1, blacklist1.size());
		Assert.assertTrue(blacklist1.contains("host8"));
		// exception happened in another host
		networkExceptionSimulator.triggerNetworkExceptionInHost("host2");
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist2 = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(2, blacklist2.size());
		Assert.assertTrue(blacklist2.contains("host8"));
		Assert.assertTrue(blacklist2.contains("host2"));
	}

	@Test
	public void testRandomMultipleHostsCorrupted() {
		networkExceptionSimulator.start();
		String[] chosenHosts = networkExceptionSimulator.triggerNetworkExceptionInMultipleHost(4);
		networkExceptionSimulator.triggerNetworkExceptionInHost(chosenHosts);
		networkExceptionSimulator.triggerNetworkExceptionInHost(chosenHosts);
		clock.advanceTime(30, TimeUnit.SECONDS);
		executor.triggerNonPeriodicScheduledTask();
		Set<String> blacklist = getAllBlacklist(blacklistTracker.getBlackedRecords());
		Assert.assertEquals(4, blacklist.size());
	}

	@Test
	public void testAllHostCorrupted() {
		networkExceptionSimulator.start(4);
		networkExceptionSimulator.triggerNetworkExceptionInMultipleHost(4);
		networkExceptionSimulator.triggerNetworkExceptionInMultipleHost(4);
		networkExceptionSimulator.triggerNetworkExceptionInMultipleHost(4);
		clock.advanceTime(30, TimeUnit.SECONDS);
		// trigger periodic tasks to update total number of hosts.
		alwaysReturnsTaskManagerAliveAction.setNumberOfHosts(4);
		BlacklistTestUtils.triggerPeriodicScheduledTasksInMainThreadExecutor(executor);
		// trigger calculating blacklist.
		executor.triggerNonPeriodicScheduledTask();
		Assert.assertEquals(4, getAllBlacklist(blacklistTracker.getBlackedRecords()).size());
	}

	@Test
	public void testResolveHostNameFromSocketAddress() {
		Assert.assertEquals("test-host", NetworkTraceable.extractHostNameOrIP(
				"test-host/fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593"));
		Assert.assertEquals("fdbd:dc03:ff:100:53b3:6d47:8366:32e8", NetworkTraceable.extractHostNameOrIP(
				"fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593"));
		Assert.assertEquals("10.185.40.181", NetworkTraceable.extractHostNameOrIP(
				"10.185.40.181:14593"));
	}

	@Test
	public void testResolvePortFromSocketAddress() {
		Assert.assertEquals(14593,
				NetworkTraceable.extractPort("test-host/fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593"));
		Assert.assertEquals(14593,
				NetworkTraceable.extractPort("fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593"));
		Assert.assertEquals(14593,
				NetworkTraceable.extractPort("10.185.40.181:14593"));
	}

	@Test
	public void testResolveInetAddressFromSocketAddress1() {
		NetworkAddress networkAddress = NetworkTraceable.parseFromString("test-host/fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593");
		Assert.assertNotNull(networkAddress);
		Assert.assertEquals("test-host", networkAddress.getHostName());
		Assert.assertEquals(14593, networkAddress.getPort());
	}

	@Test
	public void testResolveInetAddressFromSocketAddress2() {
		NetworkAddress networkAddress = NetworkTraceable.parseFromString("/fdbd:dc03:ff:100:53b3:6d47:8366:32e8:14593");
		Assert.assertNotNull(networkAddress);
		Assert.assertEquals("fdbd:dc03:ff:100:53b3:6d47:8366:32e8", networkAddress.getHostName());
		Assert.assertEquals(14593, networkAddress.getPort());
	}

	@Test
	public void testResolveInetAddressFromSocketAddress3() {
		NetworkAddress networkAddress = NetworkTraceable.parseFromString("/10.185.40.181:14593");
		Assert.assertNotNull(networkAddress);
		Assert.assertEquals("10.185.40.181", networkAddress.getHostName());
		Assert.assertEquals(14593, networkAddress.getPort());
	}

	@Test
	public void testResolveInetAddressFromSocketAddress4() {
		NetworkAddress networkAddress = NetworkTraceable.parseFromString("test-host:14593");
		Assert.assertNotNull(networkAddress);
		Assert.assertEquals("test-host", networkAddress.getHostName());
		Assert.assertEquals(14593, networkAddress.getPort());
	}

	private Set<String> getAllBlacklist(Set<BlacklistRecord> blacklistRecordSet) {
		Set<String> blackedHosts = new HashSet<>();
		for (BlacklistRecord blacklistRecord : blacklistRecordSet) {
			blackedHosts.addAll(blacklistRecord.getBlackedHosts());
		}
		return blackedHosts;
	}

	private static class AlwaysReturnsTaskManagerAliveAction implements BlacklistActions {

		private int numberOfHosts;

		public void setNumberOfHosts(int numberOfHosts) {
			this.numberOfHosts = numberOfHosts;
		}

		@Override
		public void notifyBlacklistUpdated() {
			LOG.info("notifyBlacklistUpdated");
		}

		@Override
		public int queryNumberOfHosts() {
			return numberOfHosts;
		}

		@Nullable
		@Override
		public ResourceID queryTaskManagerID(NetworkAddress networkAddress) {
			return new ResourceID("id");
		}

		@Override
		public boolean isTaskManagerOffline(ResourceID taskManagerID) {
			return false;
		}

		@Override
		public int getRegisteredWorkerNumber() {
			return 0;
		}
	}

}
