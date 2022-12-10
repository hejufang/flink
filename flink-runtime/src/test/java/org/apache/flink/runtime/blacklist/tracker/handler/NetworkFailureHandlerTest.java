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

package org.apache.flink.runtime.blacklist.tracker.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.TestingBlacklistActions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for NetworkFailureHandler.
 */
public class NetworkFailureHandlerTest {

	private ManualClock clock;
	private TestingNetworkFailureHandler networkFailureHandler;
	private TestingBlacklistActions blacklistActions;

	@BeforeEach
	public void setup() {
		this.clock = new ManualClock();
		blacklistActions = new TestingBlacklistActions.TestingBlacklistActionsBuilder().build();
		this.networkFailureHandler = new TestingNetworkFailureHandler(
				Time.seconds(30),
				Time.minutes(30),
				Time.seconds(60),
				10,
				0.5f,
				0.5f,
				1,
				0.5f,
				5,
				clock
		);
		this.networkFailureHandler.start(null, blacklistActions);
	}

	@Test
	public void testFailureWithSrcTaskManagerOffline() {
		RemoteTransportException remoteTransportException = new RemoteTransportException("network error", new InetSocketAddress("host2", 1025));
		HostFailure hostFailure = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		// should return false because the src task manager doesn't exist.
		Assertions.assertFalse(networkFailureHandler.addFailure(hostFailure));

		// add src task manager c1 to alive task manager list
		blacklistActions.addNewTaskManagerWith(new ResourceID("c1"), "host1", 10001);
		Assertions.assertTrue(networkFailureHandler.addFailure(hostFailure));
	}

	@Test
	public void testFailureWithRemoteTaskManagerOffline() {
		// add c1 to alive task manager list
		blacklistActions.addNewTaskManagerWith(new ResourceID("c1"), "host1", 10001);
		// record c2 as an offline task manager
		blacklistActions.recordHistoryTaskManagerWith(new ResourceID("c2"), "host2", 10002);
		RemoteTransportException remoteTransportException = new RemoteTransportException("network error", new InetSocketAddress("host2", 10002));
		HostFailure hostFailure = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		// should return false because the remote task manager is offline.
		Assertions.assertFalse(networkFailureHandler.addFailure(hostFailure));

		// add remote task manager c2 to alive task manager list
		blacklistActions.addNewTaskManagerWith(new ResourceID("c2"), "host2", 10002);
		Assertions.assertTrue(networkFailureHandler.addFailure(hostFailure));
	}

	@Test
	public void testAddRepeatableFailures() {
		// add c1 to alive task manager list
		blacklistActions.addNewTaskManagerWith(new ResourceID("c1"), "host1", 10001);
		RemoteTransportException remoteTransportException = new RemoteTransportException("network error", new InetSocketAddress("host2", 1025));
		HostFailure hostFailure = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		Assertions.assertTrue(networkFailureHandler.addFailure(hostFailure));

		// should skip the failure happened in a very close time from last failure with same src and dest.
		clock.advanceTime(10, TimeUnit.SECONDS);
		HostFailure hostFailure2 = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		Assertions.assertFalse(networkFailureHandler.addFailure(hostFailure2));

		clock.advanceTime(50, TimeUnit.SECONDS);
		HostFailure hostFailure3 = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		// should not skip this failure because the timestamp record for last failure should expire (60s).
		Assertions.assertTrue(networkFailureHandler.addFailure(hostFailure3));
	}

	@Test
	public void testFailureWithHostInBlacklist() {
		// add c1 to alive task manager list
		blacklistActions.addNewTaskManagerWith(new ResourceID("c1"), "host1", 10001);
		RemoteTransportException remoteTransportException = new RemoteTransportException("network error", new InetSocketAddress("host2", 1025));
		HostFailure hostFailure = new HostFailure(
				BlacklistUtil.FailureType.NETWORK, "host1", new ResourceID("c1"), remoteTransportException, clock.absoluteTimeMillis());
		networkFailureHandler.addHostToMockedBlacklist("host1");
		Assertions.assertFalse(networkFailureHandler.addFailure(hostFailure));
	}

	private static class TestingNetworkFailureHandler extends NetworkFailureHandler {

		private final List<String> mockedBlacklist = new ArrayList<>();

		public TestingNetworkFailureHandler(
				Time failureEffectiveTime,
				Time networkFailureExpireTime,
				Time timestampRecordExpireTime,
				int blacklistMaxLength,
				float meanSdRatioAllBlockedThreshold,
				float meanSdRatioSomeBlockedThreshold,
				int expectedMinHost,
				float expectedBlockedHostRatio,
				int maxTaskFailureNumPerHost,
				Clock clock) {
			super(
					failureEffectiveTime,
					networkFailureExpireTime,
					timestampRecordExpireTime,
					blacklistMaxLength,
					meanSdRatioAllBlockedThreshold,
					meanSdRatioSomeBlockedThreshold,
					expectedMinHost,
					expectedBlockedHostRatio,
					maxTaskFailureNumPerHost,
					clock);
		}

		public void addHostToMockedBlacklist(String host) {
			mockedBlacklist.add(host);
		}

		protected boolean checkHostInBlacklist(String hostName) {
			return mockedBlacklist.contains(hostName);
		}
	}
}
