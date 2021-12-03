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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiConsumerWithException;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.state.ConnectionStateListener;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test behaviors of {@link ZooKeeperLeaderElectionDriver} when losing the connection to ZooKeeper.
 */
public class ZooKeeperLeaderElectionConnectionHandlingTest extends TestLogger {

	private static final String LATCH_PATH = "/latch_path";

	private static final String LEADER_PATH = "/leader_path";

	private TestingServer testingServer;

	private Configuration config;

	private CuratorFramework zooKeeperClient;

	@Rule
	public final TestingFatalErrorHandlerResource fatalErrorHandlerResource =
		new TestingFatalErrorHandlerResource();

	@Before
	public void before() throws Exception {
		testingServer = new TestingServer();

		config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config.set(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 1000);
		config.set(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT, 1000);
		config.setString(
			HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());

		zooKeeperClient = ZooKeeperUtils.startCuratorFramework(config);
		zooKeeperClient.blockUntilConnected();
	}

	@After
	public void after() throws Exception {
		closeTestServer();

		if (zooKeeperClient != null) {
			zooKeeperClient.close();
			zooKeeperClient = null;
		}
	}

	@Test
	public void testKeepLeadershipOnSuspendedConnection()
		throws Exception {
		runTestWithBrieflySuspendedZooKeeperConnection(
			(connectionStateListener, contender) -> {
				connectionStateListener.awaitSuspendedConnection();
				connectionStateListener.awaitReconnectedConnection();
				assertFalse(contender.hasRevokeLeadershipBeenTriggered());
			});
	}

	@Test
	public void testLoseLeadershipOnLostConnection()
		throws Exception {
		runTestWithLostZooKeeperConnection(
			(connectionStateListener, contender) -> {
				connectionStateListener.awaitLostConnection();
				contender.awaitRevokeLeadership(Duration.ofSeconds(1L));
				assertTrue(contender.hasRevokeLeadershipBeenTriggered());
			});
	}

	private void runTestWithLostZooKeeperConnection(
		BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
			validationLogic)
		throws Exception {
		runTestWithZooKeeperConnectionProblem(validationLogic, Problem.LOST_CONNECTION);
	}

	private void runTestWithBrieflySuspendedZooKeeperConnection(
		BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
			validationLogic)
		throws Exception {
		runTestWithZooKeeperConnectionProblem(validationLogic, Problem.SUSPENDED_CONNECTION);
	}

	private enum Problem {
		LOST_CONNECTION,
		SUSPENDED_CONNECTION
	}

	private void runTestWithZooKeeperConnectionProblem(
		BiConsumerWithException<TestingConnectionStateListener, TestingContender, Exception>
			validationLogic,
		Problem problem)
		throws Exception {
		LeaderElectionDriverFactory leaderElectionDriverFactory =
			new ZooKeeperLeaderElectionDriverFactory(zooKeeperClient, LATCH_PATH, LEADER_PATH);
		DefaultLeaderElectionService leaderElectionService =
			new DefaultLeaderElectionService(leaderElectionDriverFactory);

		try {
			final TestingConnectionStateListener connectionStateListener =
				new TestingConnectionStateListener();
			zooKeeperClient.getConnectionStateListenable().addListener(connectionStateListener);

			final TestingContender contender = new TestingContender();
			leaderElectionService.start(contender);

			contender.awaitGrantLeadership();

			switch (problem) {
				case SUSPENDED_CONNECTION:
					restartTestServer();
					break;
				case LOST_CONNECTION:
					closeTestServer();
					break;
				default:
					throw new IllegalArgumentException(
						String.format("Unknown problem type %s.", problem));
			}

			validationLogic.accept(connectionStateListener, contender);
		} finally {
			leaderElectionService.stop();

			if (problem == Problem.LOST_CONNECTION) {
				// in case of lost connections we accept that some unhandled error can occur
				fatalErrorHandlerResource.getFatalErrorHandler().clearError();
			}
		}
	}

	private void closeTestServer() throws IOException {
		if (testingServer != null) {
			testingServer.close();
			testingServer = null;
		}
	}

	private void restartTestServer() throws Exception {
		Preconditions.checkNotNull(testingServer, "TestingServer needs to be initialized.")
			.restart();
	}

	private final class TestingContender implements LeaderContender {

		private final OneShotLatch grantLeadershipLatch;
		private final OneShotLatch revokeLeadershipLatch;

		private TestingContender() {
			this.grantLeadershipLatch = new OneShotLatch();
			this.revokeLeadershipLatch = new OneShotLatch();
		}

		@Override
		public void grantLeadership(UUID leaderSessionID) {
			grantLeadershipLatch.trigger();
		}

		@Override
		public void revokeLeadership() {
			revokeLeadershipLatch.trigger();
		}

		@Override
		public void handleError(Exception exception) {
			fatalErrorHandlerResource.getFatalErrorHandler().onFatalError(exception);
		}

		public void awaitGrantLeadership() throws InterruptedException {
			grantLeadershipLatch.await();
		}

		public boolean hasRevokeLeadershipBeenTriggered() {
			return revokeLeadershipLatch.isTriggered();
		}

		public void awaitRevokeLeadership(Duration timeout)
			throws InterruptedException, TimeoutException {
			revokeLeadershipLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
		}
	}

	private static final class TestingConnectionStateListener implements ConnectionStateListener {
		private final OneShotLatch connectionSuspendedLatch;
		private final OneShotLatch reconnectedLatch;
		private final OneShotLatch connectionLostLatch;

		public TestingConnectionStateListener() {
			this.connectionSuspendedLatch = new OneShotLatch();
			this.reconnectedLatch = new OneShotLatch();
			this.connectionLostLatch = new OneShotLatch();
		}

		@Override
		public void stateChanged(
			CuratorFramework curatorFramework, ConnectionState connectionState) {
			if (connectionState == ConnectionState.SUSPENDED) {
				connectionSuspendedLatch.trigger();
			}

			if (connectionState == ConnectionState.RECONNECTED) {
				reconnectedLatch.trigger();
			}

			if (connectionState == ConnectionState.LOST) {
				connectionLostLatch.trigger();
			}
		}

		public void awaitSuspendedConnection() throws InterruptedException {
			connectionSuspendedLatch.await();
		}

		public void awaitReconnectedConnection() throws InterruptedException {
			reconnectedLatch.await();
		}

		public void awaitLostConnection() throws InterruptedException {
			connectionLostLatch.await();
		}
	}
}
