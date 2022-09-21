/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ZooKeeperMultipleComponentLeaderElectionDriver}. */
@ExtendWith(TestLoggerExtension.class)
public class ZooKeeperMultipleComponentLeaderElectionDriverTest {

	private static final long timeoutMills = 200L;
	private static final Duration timeout = Duration.of(timeoutMills, ChronoUnit.MILLIS);
	private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

	@RegisterExtension
	public final EachCallbackWrapper<ZooKeeperExtension> eachWrapper = new EachCallbackWrapper<>(zooKeeperExtension);

	@Test
	public void testElectionDriverGainsLeadershipAtStartup() throws Exception {
		new Context() {
			{
				runTest(
					() ->
						leaderElectionListener.await(
							LeaderElectionEvent.IsLeaderEvent.class, timeout));
			}
		};
	}

	@Test
	public void testElectionDriverLosesLeadership() throws Exception {
		new Context() {
			{
				runTest(
					() -> {
						leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class, timeout);
						zooKeeperExtension.stop();
						leaderElectionListener.await(LeaderElectionEvent.NotLeaderEvent.class, timeout);
					});
			}
		};
	}

	@Test
	public void testPublishLeaderInformation() throws Exception {
		new Context() {
			{
				runTest(
					() -> {
						leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class, timeout);

						final String componentId = "retrieved-component";
						final DefaultLeaderRetrievalService defaultLeaderRetrievalService =
							new DefaultLeaderRetrievalService(
								new ZooKeeperLeaderRetrievalDriverFactory(
									curatorFramework,
									componentId,
									ZooKeeperLeaderRetrievalDriver
										.LeaderInformationClearancePolicy
										.ON_LOST_CONNECTION));

						final TestingListener leaderRetrievalListener = new TestingListener();
						defaultLeaderRetrievalService.start(leaderRetrievalListener);

						final LeaderInformation leaderInformation =
							LeaderInformation.known(UUID.randomUUID(), "foobar");
						leaderElectionDriver.publishLeaderInformation(
							componentId, leaderInformation);

						leaderRetrievalListener.waitForNewLeader(timeoutMills);

						assertThat(leaderRetrievalListener.getLeader())
							.isEqualTo(leaderInformation);
					});
			}
		};
	}

//	@Test
//	public void testPublishEmptyLeaderInformation() throws Exception {
//		new Context() {
//			{
//				runTest(
//					() -> {
//						leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class, timeout);
//
//						final String componentId = "retrieved-component";
//						final DefaultLeaderRetrievalService defaultLeaderRetrievalService =
//							new DefaultLeaderRetrievalService(
//								new ZooKeeperLeaderRetrievalDriverFactory(
//									curatorFramework,
//									componentId,
//									ZooKeeperLeaderRetrievalDriver
//										.LeaderInformationClearancePolicy
//										.ON_LOST_CONNECTION));
//
//						final TestingListener leaderRetrievalListener = new TestingListener();
//						defaultLeaderRetrievalService.start(leaderRetrievalListener);
//
//						leaderElectionDriver.publishLeaderInformation(
//							componentId,
//							LeaderInformation.known(UUID.randomUUID(), "foobar"));
//
//						leaderRetrievalListener.waitForNewLeader(timeoutMills);
//
//						leaderElectionDriver.publishLeaderInformation(
//							componentId, LeaderInformation.empty());
//
//						leaderRetrievalListener.waitForEmptyLeaderInformation(timeoutMills);
//
//						assertThat(leaderRetrievalListener.getLeader())
//							.isEqualTo(LeaderInformation.empty());
//					});
//			}
//		};
//	}

	@Test
	public void testNonLeaderCannotPublishLeaderInformation() throws Exception {
		new Context() {
			{
				runTest(
					() -> {
						ElectionDriver otherLeaderElectionDriver = null;
						try {
							leaderElectionListener.await(
								LeaderElectionEvent.IsLeaderEvent.class, timeout);

							otherLeaderElectionDriver =
								createLeaderElectionDriver(
									curatorFramework);

							assertThat(otherLeaderElectionDriver.hasLeadership()).isFalse();

							otherLeaderElectionDriver.publishLeaderInformation(
								"componentId",
								LeaderInformation.known(UUID.randomUUID(), "localhost"));

							assertThat(
								leaderElectionListener.await(
									LeaderElectionEvent
										.LeaderInformationChangeEvent.class,
									timeout))
								.isEmpty();
						} finally {
							if (otherLeaderElectionDriver != null) {
								otherLeaderElectionDriver.close();
							}
						}
					});
			}
		};
	}

	@Test
	public void testLeaderInformationChange() throws Exception {
		new Context() {
			{
				runTest(
					() -> {
						leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class, timeout);

						final LeaderInformation leaderInformation =
							LeaderInformation.known(UUID.randomUUID(), "foobar");
						final String componentId = "componentId";
						final String path =
							ZooKeeperUtils.generateConnectionInformationPath(componentId);

						ZooKeeperUtils.writeLeaderInformationToZooKeeper(
							leaderInformation,
							curatorFramework,
							() -> true,
							path);

						final Optional<LeaderElectionEvent.LeaderInformationChangeEvent>
							leaderInformationChangeEvent =
							leaderElectionListener.await(
								LeaderElectionEvent.LeaderInformationChangeEvent
									.class, timeout);

						if (leaderInformationChangeEvent.isPresent()) {
							assertThat(leaderInformationChangeEvent.get().getComponentId())
								.isEqualTo(componentId);
							assertThat(leaderInformationChangeEvent.get().getLeaderInformation())
								.isEqualTo(leaderInformation);
						}
					});
			}
		};
	}

	@Test
	public void testLeaderElectionWithMultipleDrivers() throws Exception {
		final CuratorFramework curatorFramework = startCuratorFramework();

		try {
			Set<ElectionDriver> electionDrivers =
				Stream.generate(
						() ->
							createLeaderElectionDriver(curatorFramework))
					.limit(3)
					.collect(Collectors.toSet());

			while (!electionDrivers.isEmpty()) {
				final CompletableFuture<Object> anyLeader =
					CompletableFuture.anyOf(
						electionDrivers.stream()
							.map(ElectionDriver::getLeadershipFuture)
							.toArray(CompletableFuture[]::new));

				// wait for any leader
				anyLeader.join();

				final Map<Boolean, Set<ElectionDriver>> leaderAndRest =
					electionDrivers.stream()
						.collect(
							Collectors.partitioningBy(
								ElectionDriver::hasLeadership, Collectors.toSet()));

				assertThat(leaderAndRest.get(true)).hasSize(1);
				Iterables.getOnlyElement(leaderAndRest.get(true)).close();

				electionDrivers = leaderAndRest.get(false);
			}
		} finally {
			curatorFramework.close();
		}
	}

	@Test
	public void testLeaderConnectionInfoNodeRemovalLeadsToLeaderChangeWithEmptyLeaderInformation()
		throws Exception {
		new Context() {
			{
				runTest(
					() -> {
						leaderElectionListener.await(LeaderElectionEvent.IsLeaderEvent.class, timeout);

						final LeaderInformation leaderInformation =
							LeaderInformation.known(UUID.randomUUID(), "foobar");
						final String componentId = "componentId";
						final String path =
							ZooKeeperUtils.generateConnectionInformationPath(componentId);

						ZooKeeperUtils.writeLeaderInformationToZooKeeper(
							leaderInformation,
							curatorFramework,
							() -> true,
							path);

						// wait for the publishing of the leader information
						leaderElectionListener.await(
							LeaderElectionEvent.LeaderInformationChangeEvent.class, timeout);

						curatorFramework.delete().forPath(path);

						// wait for the removal of the leader information
						final Optional<LeaderElectionEvent.LeaderInformationChangeEvent>
							leaderInformationChangeEvent =
							leaderElectionListener.await(LeaderElectionEvent.LeaderInformationChangeEvent.class, timeout);

						if (leaderInformationChangeEvent.isPresent()) {
							assertThat(leaderInformationChangeEvent.get().getComponentId())
								.isEqualTo(componentId);
							assertThat(leaderInformationChangeEvent.get().getLeaderInformation())
								.isEqualTo(LeaderInformation.empty());
						}
					});
			}
		};
	}

	private static ElectionDriver createLeaderElectionDriver(CuratorFramework curatorFramework) {
		final SimpleLeaderElectionListener leaderElectionListener =
			new SimpleLeaderElectionListener();

		try {
			final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver =
				createLeaderElectionDriver(leaderElectionListener, curatorFramework);
			return new ElectionDriver(leaderElectionDriver, leaderElectionListener);
		} catch (Exception e) {
			ExceptionUtils.rethrow(e);
			return null;
		}
	}

	private static final class ElectionDriver {
		private final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver;
		private final SimpleLeaderElectionListener leaderElectionListener;

		private ElectionDriver(
			ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver,
			SimpleLeaderElectionListener leaderElectionListener) {
			this.leaderElectionDriver = leaderElectionDriver;
			this.leaderElectionListener = leaderElectionListener;
		}

		void close() throws Exception {
			leaderElectionDriver.close();
		}

		boolean hasLeadership() {
			return leaderElectionDriver.hasLeadership();
		}

		CompletableFuture<Void> getLeadershipFuture() {
			return leaderElectionListener.getLeadershipFuture();
		}

		void publishLeaderInformation(String componentId, LeaderInformation leaderInformation)
			throws Exception {
			leaderElectionDriver.publishLeaderInformation(componentId, leaderInformation);
		}
	}

	private static final class SimpleLeaderElectionListener
		implements MultipleComponentLeaderElectionDriver.Listener {

		private final CompletableFuture<Void> leadershipFuture = new CompletableFuture<>();

		CompletableFuture<Void> getLeadershipFuture() {
			return leadershipFuture;
		}

		@Override
		public void isLeader() {
			leadershipFuture.complete(null);
		}

		@Override
		public void notLeader() {}

		@Override
		public void notifyLeaderInformationChange(
			String componentId, LeaderInformation leaderInformation) {}

		@Override
		public void notifyAllKnownLeaderInformation(
			Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {}
	}

	private static ZooKeeperMultipleComponentLeaderElectionDriver createLeaderElectionDriver(
		MultipleComponentLeaderElectionDriver.Listener leaderElectionListener,
		CuratorFramework curatorFramework)
		throws Exception {
		return new ZooKeeperMultipleComponentLeaderElectionDriver(
			curatorFramework, leaderElectionListener);
	}

	private CuratorFramework startCuratorFramework() {
		final Configuration configuration = new Configuration();
		configuration.set(
			HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
		return ZooKeeperUtils.startCuratorFramework(configuration);
	}

	private class Context {
		protected final TestingLeaderElectionListener leaderElectionListener;
		protected final CuratorFramework curatorFramework;
		protected final ZooKeeperMultipleComponentLeaderElectionDriver leaderElectionDriver;

		private Context() throws Exception {
			this.leaderElectionListener = new TestingLeaderElectionListener();
			this.curatorFramework = startCuratorFramework();
			this.leaderElectionDriver =
				createLeaderElectionDriver(
					leaderElectionListener, curatorFramework);
		}

		protected final void runTest(RunnableWithException test) throws Exception {
			try {
				test.run();
			} finally {
				close();
			}
		}

		private void close() throws Exception {
			this.leaderElectionDriver.close();
			this.curatorFramework.close();
		}
	}
}
