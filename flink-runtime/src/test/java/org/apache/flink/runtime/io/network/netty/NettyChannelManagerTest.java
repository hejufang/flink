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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple netty channel manager test.
 */
public class NettyChannelManagerTest extends TestLogger {
	private static final int SERVER_PORT = NetUtils.getAvailablePort();
	private static final int NUM_THREAD = 16;
	private static final int NUM_MULTITHREADING_TEST_ROUND = 500;
	private static final long IDLE_CHANNEL_RELEASE_TIME_MS = 10000;

	private NettyChannelManager nettyChannelManager = null;
	private NetworkClientHandler clientHandler = null;

	@Before
	public void setUp() {
		nettyChannelManager = createNettyChannelManager();
		clientHandler = mockClientHandler();
	}

	@After
	public void tearDown() {
		if (nettyChannelManager != null) {
			nettyChannelManager.shutdown();
		}
		nettyChannelManager = null;
		clientHandler = null;
	}

	@Test
	public void testRetrieveChannelWithNoChannel() {
		ConnectionID connectionID = null;

		try {
			connectionID = createConnectionID(0);
			nettyChannelManager = createNettyChannelManager();

			nettyChannelManager.addReference(connectionID);
			NettyChannelManager.ChannelInfo result = nettyChannelManager.getChannel(connectionID);
			NettyChannelManager.ChannelInfo expect = new NettyChannelManager.ChannelInfo();

			assertEquals(expect, result);
			assertEquals(0, nettyChannelManager.getTotalPoolSize());
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
	}

	@Test
	public void testRegisterChannel() {
		ConnectionID connectionID = null;
		EmbeddedChannel channel = null;

		try {
			connectionID = createConnectionID(0);
			nettyChannelManager = createNettyChannelManager();
			channel = new EmbeddedChannel(clientHandler);

			// Must call getChannel() before register channel.
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);

			assertEquals(0, nettyChannelManager.getNumIdleChannels(connectionID));
		}
		catch (Throwable t) {
			t.printStackTrace();
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testRetrieveChannelWithExistConnectionID() {
		ConnectionID connectionID = null;
		EmbeddedChannel channel = null;

		try {
			connectionID = createConnectionID(0);
			channel = new EmbeddedChannel(clientHandler);

			// Register channel at first.
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);

			// Another subtask with same connectionID try to get channel.
			NettyChannelManager.ChannelInfo result = nettyChannelManager.getChannel(connectionID);
			NettyChannelManager.ChannelInfo expect = new NettyChannelManager.ChannelInfo(channel, clientHandler, connectionID);

			assertEquals(expect, result);
			assertEquals(0, nettyChannelManager.getNumIdleChannels(connectionID));
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testRetrieveChannelWithAllBusyChannel() {
		ConnectionID connectionID = null;
		ConnectionID anotherConnectionID = null;
		EmbeddedChannel channel = null;

		try {
			connectionID = createConnectionID(0);
			anotherConnectionID = createConnectionID(1);
			channel = new EmbeddedChannel(clientHandler);

			// Register a channel first.
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);

			// Request idle channel but can't find.
			NettyChannelManager.ChannelInfo result = nettyChannelManager.getChannel(anotherConnectionID);
			NettyChannelManager.ChannelInfo expect = new NettyChannelManager.ChannelInfo();

			assertEquals(expect, result);
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}

			if (anotherConnectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testReleaseChannel() {
		ConnectionID connectionID = null;
		EmbeddedChannel channel = null;

		try {
			channel = new EmbeddedChannel(clientHandler);
			connectionID = createConnectionID(0);

			// Register and release channel immediately.
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);
			nettyChannelManager.decreaseRequestCount(connectionID);
			nettyChannelManager.releaseChannel(connectionID);

			assertEquals(1, nettyChannelManager.getTotalPoolSize());
			assertEquals(1, nettyChannelManager.getNumIdleChannels(connectionID));
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testResourceReleaseAfterShutdown() {
		ConnectionID connectionID = null;
		EmbeddedChannel channel = null;

		try {
			channel = new EmbeddedChannel(clientHandler);
			connectionID = createConnectionID(0);

			// Register and release channel immediately.
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);
			nettyChannelManager.decreaseRequestCount(connectionID);
			nettyChannelManager.releaseChannel(connectionID);

			nettyChannelManager.shutdown();

			assertEquals(0, nettyChannelManager.getTotalPoolSize());
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testRetrieveChannelWithIdleChannel() {
		ConnectionID connectionID = null;
		ConnectionID anotherConnectionID = null;
		EmbeddedChannel channel = null;

		try {
			channel = new EmbeddedChannel(clientHandler);
			connectionID = createConnectionID(0);
			anotherConnectionID = createConnectionID(1);

			// Register and release channel immediately.
			{
				nettyChannelManager.addReference(connectionID);
				nettyChannelManager.getChannel(connectionID);
				nettyChannelManager.addChannel(connectionID, channel, clientHandler);
				nettyChannelManager.decreaseRequestCount(connectionID);
				nettyChannelManager.releaseChannel(connectionID);

				assertEquals(1, nettyChannelManager.getTotalPoolSize());
				assertEquals(1, nettyChannelManager.getNumIdleChannels(connectionID));
			}

			NettyChannelManager.ChannelInfo result = nettyChannelManager.getChannel(anotherConnectionID);
			NettyChannelManager.ChannelInfo expect = new NettyChannelManager.ChannelInfo(channel, clientHandler, anotherConnectionID);

			assertEquals(expect, result);
			assertEquals(0, nettyChannelManager.getNumIdleChannels(connectionID));
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}

			if (anotherConnectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testDestroyChannelManager() {
		ConnectionID connectionID = null;
		EmbeddedChannel channel = null;

		try {
			// destroy when all channel are released
			{
				channel = new EmbeddedChannel(clientHandler);
				connectionID = createConnectionID(0);

				// Register and release channel immediately.
				nettyChannelManager.addReference(connectionID);
				nettyChannelManager.getChannel(connectionID);
				nettyChannelManager.addChannel(connectionID, channel, clientHandler);
				nettyChannelManager.decreaseRequestCount(connectionID);
				nettyChannelManager.releaseChannel(connectionID);

				nettyChannelManager.shutdown();

				assertEquals(0, nettyChannelManager.getTotalPoolSize());

				channel.close();
			}
			// destroy without release channel;
			{
				channel = new EmbeddedChannel(clientHandler);
				connectionID = createConnectionID(0);

				// Register channel
				nettyChannelManager.addReference(connectionID);
				nettyChannelManager.getChannel(connectionID);
				nettyChannelManager.addChannel(connectionID, channel, clientHandler);

				nettyChannelManager.shutdown();

				assertEquals(0, nettyChannelManager.getTotalPoolSize());

				channel.close();
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testDestroyIdleTooLongChannel() {
		ConnectionID connectionID = null;
		ConnectionID anotherConnectionID = null;
		EmbeddedChannel channel = null;

		try {
			channel = new EmbeddedChannel(clientHandler);
			connectionID = createConnectionID(0);
			anotherConnectionID = createConnectionID(1);

			// Register a channel, and do sleep
			nettyChannelManager.addReference(connectionID);
			nettyChannelManager.getChannel(connectionID);
			nettyChannelManager.addChannel(connectionID, channel, clientHandler);
			Thread.sleep(IDLE_CHANNEL_RELEASE_TIME_MS / 2);

			// Register and release another channel immediately
			{
				nettyChannelManager.addReference(anotherConnectionID);
				nettyChannelManager.getChannel(anotherConnectionID);
				nettyChannelManager.addChannel(anotherConnectionID, channel, clientHandler);
				nettyChannelManager.decreaseRequestCount(connectionID);
				nettyChannelManager.releaseChannel(connectionID);

				assertEquals(1, nettyChannelManager.getNumIdleChannels(anotherConnectionID));
			}

			// Sleep for a while until first netty channel idle too long and try to release channel
			Thread.sleep(IDLE_CHANNEL_RELEASE_TIME_MS / 2);
			nettyChannelManager.decreaseRequestCount(anotherConnectionID);
			nettyChannelManager.releaseChannel(anotherConnectionID);

			assertTrue(1 <= nettyChannelManager.getTotalPoolSize());
			assertFalse(nettyChannelManager.containsRequest(connectionID));
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}

			if (anotherConnectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}
		}
	}

	@Test
	public void testConcurrentRetrieveChannelWithSameConnectionID() {
		ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREAD);
		EmbeddedChannel channel = null;
		ConnectionID connectionID = null;

		try {
			connectionID = createConnectionID(0);

			for (int round = 1; round <= NUM_MULTITHREADING_TEST_ROUND; ++round) {
				if (nettyChannelManager == null) {
					nettyChannelManager = createNettyChannelManager();
				}
				channel = new EmbeddedChannel(clientHandler);
				AtomicInteger number = new AtomicInteger(0);
				final CountDownLatch sync = new CountDownLatch(NUM_THREAD);

				for (int i = 0; i < NUM_THREAD; ++i) {
					ConnectionID finalConnectionID = connectionID;
					EmbeddedChannel finalChannel = channel;
					executorService.execute(new Runnable() {
						@Override
						public void run() {
							try {
								nettyChannelManager.addReference(finalConnectionID);
								NettyChannelManager.ChannelInfo channelInfo =
									nettyChannelManager.getChannel(finalConnectionID);
								// Simulate build netty channel which should handle by upper layer
								if (!channelInfo.isAvailable()) {
									if (number.getAndIncrement() == 0) {
										nettyChannelManager.addChannel(finalConnectionID, finalChannel, clientHandler);
									}
								}
								sync.countDown();
							} catch (Throwable t) {
								Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
							}
						}
					});
				}

				sync.await();

				NettyChannelManager.ChannelInfo expect = new NettyChannelManager.ChannelInfo(channel, clientHandler, connectionID);
				Object result = nettyChannelManager.getChannelRequest(connectionID);

				assertEquals(expect, result);

				if (nettyChannelManager != null) {
					nettyChannelManager.shutdown();
				}
				nettyChannelManager = null;
			}
		}
		catch (Throwable t) {
			if (connectionID != null) {
				Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
			} else {
				t.printStackTrace();
				fail("Could not create RemoteAddress for server.");
			}
		}
		finally {
			if (channel != null) {
				channel.close();
			}

			executorService.shutdown();
			executorService.shutdownNow();
		}
	}

	@Test
	public void testConcurrentRetrieveChannelWithDifferentConnectionIDWithDifferentServer() {
		ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREAD);

		try {
			for (int round = 1; round <= NUM_MULTITHREADING_TEST_ROUND; ++round) {
				if (nettyChannelManager == null) {
					nettyChannelManager = createNettyChannelManager();
				}
				AtomicInteger number = new AtomicInteger(0);
				final CountDownLatch sync = new CountDownLatch(NUM_THREAD);

				for (int i = 0; i < NUM_THREAD; ++i) {
					executorService.execute(new Runnable() {
						@Override
						public void run() {
							try {
								int id = number.getAndIncrement();
								String ip = "192.168.0." + id;
								ConnectionID connectionID = createConnectionID(ip, id);
								EmbeddedChannel channel = new EmbeddedChannel(clientHandler);
								nettyChannelManager.addReference(connectionID);
								NettyChannelManager.ChannelInfo channelInfo =
									nettyChannelManager.getChannel(connectionID);
								// Simulate build netty channel which should handle by upper layer
								if (!channelInfo.isAvailable()) {
									nettyChannelManager.addChannel(connectionID, channel, clientHandler);
								}
								sync.countDown();
							} catch (Throwable t) {
								Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
							}
						}
					});
				}

				sync.await();

				if (nettyChannelManager != null) {
					nettyChannelManager.shutdown();
				}
				nettyChannelManager = null;
			}
		}
		catch (Throwable t) {
			Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
		}
		finally {
			executorService.shutdown();
			executorService.shutdownNow();
		}
	}

	@Test
	public void testConcurrentRegisterAndReleaseChannelInSameServer() {
		ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREAD);
		EmbeddedChannel channel = null;

		try {
			int[] statistic = new int[NUM_THREAD + 1];
			Arrays.fill(statistic, 0);
			channel = new EmbeddedChannel(clientHandler);

			for (int round = 1; round <= NUM_MULTITHREADING_TEST_ROUND; ++round) {
				if (nettyChannelManager == null) {
					nettyChannelManager = createNettyChannelManager();
				}
				AtomicInteger number = new AtomicInteger(0);
				for (int i = 0; i < NUM_THREAD; ++i) {
					if (i % 2 == 0) {
						ConnectionID connectionID = createConnectionID(i);
						nettyChannelManager.addReference(connectionID);
						nettyChannelManager.getChannel(connectionID);
						nettyChannelManager.addChannel(connectionID, channel, clientHandler);
					}
				}

				final CountDownLatch sync = new CountDownLatch(NUM_THREAD);
				for (int i = 0; i < NUM_THREAD; ++i) {
					EmbeddedChannel finalChannel = channel;
					executorService.execute(new Runnable() {
						@Override
						public void run() {
							try {
								int id = number.getAndIncrement();
								ConnectionID connectionID = createConnectionID(id);
								if (id % 2 == 0) {
									nettyChannelManager.decreaseRequestCount(connectionID);
									nettyChannelManager.releaseChannel(connectionID);
								} else {
									nettyChannelManager.addReference(connectionID);
									NettyChannelManager.ChannelInfo channelInfo =
										nettyChannelManager.getChannel(connectionID);
									// Simulate build netty channel which should handle by upper layer
									if (!channelInfo.isAvailable()) {
										nettyChannelManager.addChannel(connectionID, finalChannel, clientHandler);
									}
								}

								sync.countDown();
							} catch (InterruptedException e) {
								log.info("Stop thread" + e.getMessage());
							} catch (Throwable t) {
								t.printStackTrace();
								Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
							}
						}
					});
				}

				sync.await();

				statistic[nettyChannelManager.getTotalPoolSize()]++;
				if (nettyChannelManager != null) {
					nettyChannelManager.shutdown();
				}
				nettyChannelManager = null;
			}

			for (int i = 0; i <= NUM_THREAD; ++i) {
				log.info("Total channel count " + i + " frequency: " + statistic[i]);
			}
		}
		catch (Throwable t) {
			t.printStackTrace();
			Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
		}
		finally {
			if (channel != null) {
				channel.close();
			}

			executorService.shutdown();
			executorService.shutdownNow();
		}
	}

	private NettyChannelManager createNettyChannelManager() {
		return createNettyChannelManager(IDLE_CHANNEL_RELEASE_TIME_MS);
	}

	private NettyChannelManager createNettyChannelManager(long channelIdleReleaseTimeMs) {
		return new NettyChannelManager(channelIdleReleaseTimeMs);
	}

	private ConnectionID createConnectionID(int connectionIndex) throws UnknownHostException {
		return new ConnectionID(new InetSocketAddress(InetAddress.getLocalHost(), SERVER_PORT), connectionIndex);
	}

	private static ConnectionID createConnectionID(String ip, int connectionIndex) {
		return new ConnectionID(new InetSocketAddress(ip, SERVER_PORT), connectionIndex);
	}

	private NetworkClientHandler mockClientHandler() {
		return new NetworkClientHandler() {
			@Override
			public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {
			}

			@Override
			public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {
			}

			@Override
			public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
			}

			@Override
			public void addInputChannel(RemoteInputChannel inputChannel) throws IOException {
			}

			@Override
			public void removeInputChannel(RemoteInputChannel inputChannel) {
			}

			@Nullable
			public RemoteInputChannel getInputChannel(InputChannelID inputChannelId) {
				return null;
			}

			@Override
			public void cancelRequestFor(InputChannelID inputChannelId) {
			}

			@Override
			public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
			}

			@Override
			public void resumeConsumption(RemoteInputChannel inputChannel) {
			}
		};
	}
}
