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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Factory for {@link NettyPartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);

	private final NettyClient nettyClient;

	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();

	private final NettyChannelManager nettyChannelManager;

	private final boolean channelReuseEnable;

	PartitionRequestClientFactory(NettyClient nettyClient) {
		this(nettyClient, false, 0L);
	}

	PartitionRequestClientFactory(
			NettyClient nettyClient,
			boolean channelReuseEnable,
			long channelIdleReleaseTimeMs) {
		this.nettyClient = nettyClient;
		this.channelReuseEnable = channelReuseEnable;
		if (channelReuseEnable) {
			this.nettyChannelManager = new NettyChannelManager(channelIdleReleaseTimeMs);
		} else {
			this.nettyChannelManager = null;
		}
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link NettyPartitionRequestClient} instance for this connection.
	 */
	NettyPartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException, InterruptedException {
		Object entry;
		NettyPartitionRequestClient client = null;

		if (channelReuseEnable) {
			assert nettyChannelManager != null;
			nettyChannelManager.addReference(connectionId);
		}

		while (client == null) {
			entry = clients.get(connectionId);

			if (entry != null) {
				// Existing channel or connecting channel
				if (entry instanceof NettyPartitionRequestClient) {
					client = (NettyPartitionRequestClient) entry;
				}
				else {
					ConnectingChannel future = (ConnectingChannel) entry;
					client = future.waitForChannel();

					clients.replace(connectionId, future, client);
				}
			}
			else {
				if (channelReuseEnable) {
					// Multiple subtasks might request idle channel at the same time.
					// If there is no available channel can be used, it will go to original
					// channel construction flow.
					NettyChannelManager.ChannelInfo channelInfo = nettyChannelManager.getChannel(connectionId);
					if (channelInfo.isAvailable() && clients.get(connectionId) == null) {
						NettyPartitionRequestClient reuseClient = new NettyPartitionRequestClient(
							channelInfo.getChannel(),
							channelInfo.getClientHandler(),
							connectionId,
							this,
							true);
						// 1. update request client bookkeeping table with reused channel
						// 2. go back to beginning and follow original workflow
						// Note: It will not go back to this branch again because bookkeeping has this
						//       connectionID record now.
						if (channelInfo.getConnectionID() == connectionId) {
							clients.putIfAbsent(connectionId, reuseClient);
							continue;
						}
					}
				}

				// No channel yet. Create one, but watch out for a race.
				// We create a "connecting future" and atomically add it to the map.
				// Only the thread that really added it establishes the channel.
				// The others need to wait on that original establisher's future.
				ConnectingChannel connectingChannel = new ConnectingChannel(
						nettyClient, connectionId, this, channelReuseEnable, nettyChannelManager);
				Object old = clients.putIfAbsent(connectionId, connectingChannel);

				if (old == null) {
					nettyClient.connect(connectionId.getAddress()).addListener(connectingChannel);
					LOG.debug("NettyClient starts to connect {}.", connectionId);

					client = connectingChannel.waitForChannel();

					clients.replace(connectionId, connectingChannel, client);
				}
				else if (old instanceof ConnectingChannel) {
					client = ((ConnectingChannel) old).waitForChannel();

					clients.replace(connectionId, old, client);
				}
				else {
					client = (NettyPartitionRequestClient) old;
				}
			}

			// Make sure to increment the reference count before handing a client
			// out to ensure correct bookkeeping for channel closing.
			if (!client.incrementReferenceCounter()) {
				destroyPartitionRequestClient(connectionId, client);
				client = null;
			}
		}

		return client;
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		Object entry = clients.get(connectionId);

		if (entry instanceof ConnectingChannel) {
			ConnectingChannel channel = (ConnectingChannel) entry;

			if (channel.dispose()) {
				clients.remove(connectionId, channel);
			}
		}
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		if (channelReuseEnable) {
			nettyChannelManager.releaseChannel(connectionId);
		}
		clients.remove(connectionId, client);
	}

	void shutdownChannelManager() {
		if (channelReuseEnable) {
			nettyChannelManager.shutdown();
		}
	}

	public void decreaseRequestCount(ConnectionID connectionId) {
		nettyChannelManager.decreaseRequestCount(connectionId);
	}

	static final class ConnectingChannel implements ChannelFutureListener {

		private final Object connectLock = new Object();

		private final ConnectionID connectionId;

		private final PartitionRequestClientFactory clientFactory;

		private boolean disposeRequestClient = false;

		private NettyClient nettyClient;

		private boolean channelReuseEnable;

		private NettyChannelManager nettyChannelManager;

		private int retryTimes = 0;

		public ConnectingChannel(
				NettyClient nettyClient,
				ConnectionID connectionId,
				PartitionRequestClientFactory clientFactory,
				boolean channelReuseEnable,
				NettyChannelManager nettyChannelManager) {
			this.nettyClient = nettyClient;
			this.connectionId = connectionId;
			this.clientFactory = clientFactory;
			this.channelReuseEnable = channelReuseEnable;
			this.nettyChannelManager = nettyChannelManager;
		}

		private boolean dispose() {
			boolean result;
			synchronized (connectLock) {
				if (partitionRequestClient != null) {
					result = partitionRequestClient.disposeIfNotUsed();
				}
				else {
					disposeRequestClient = true;
					result = true;
				}

				connectLock.notifyAll();
			}

			return result;
		}

		private void handInChannel(Channel channel) {
			synchronized (connectLock) {
				try {
					NetworkClientHandler clientHandler = channel.pipeline().get(NetworkClientHandler.class);
					partitionRequestClient = new NettyPartitionRequestClient(
						channel, clientHandler, connectionId, clientFactory, channelReuseEnable);

					if (disposeRequestClient) {
						partitionRequestClient.disposeIfNotUsed();
					}

					if (channelReuseEnable) {
						nettyChannelManager.addChannel(connectionId, channel, clientHandler);
					}
					connectLock.notifyAll();
				}
				catch (Throwable t) {
					notifyOfError(t);
				}
			}
		}

		private volatile NettyPartitionRequestClient partitionRequestClient;

		private volatile Throwable error;

		private NettyPartitionRequestClient waitForChannel() throws IOException, InterruptedException {
			synchronized (connectLock) {
				while (error == null && partitionRequestClient == null) {
					connectLock.wait(2000);
				}
			}

			if (error != null) {
				throw new IOException("Connecting the channel failed: " + error.getMessage(), error);
			}

			return partitionRequestClient;
		}

		private void notifyOfError(Throwable error) {
			synchronized (connectLock) {
				this.error = error;
				connectLock.notifyAll();
			}
		}

		@VisibleForTesting
		int getRetryTimes() {
			return retryTimes;
		}

		/**
		 * retry to connect to TM.
		 */
		private void doConnect() {
			LOG.info("Try to connect to {} again.", connectionId);
			this.nettyClient.doConnect(connectionId.getAddress()).addListener(this);
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				LOG.debug("Connect to {} success.", connectionId);
				// retry times if success
				nettyClient.connectSuccessRetryTimesInc(retryTimes);
				// notify success
				handInChannel(future.channel());
			} else {
				if (retryTimes < this.nettyClient.getMaxRetryTimes() - 1) {
					retryTimes++;

					String msg = String.format("Connecting to remote task manager %s failed, retryTimes-[%s], leftRetryTimes-[%s].",
						connectionId, retryTimes + 1, this.nettyClient.getMaxRetryTimes() - retryTimes - 1);
					if (future.cause() != null) {
						LOG.warn(msg, future.cause());
					} else {
						LOG.warn(msg);
					}

					// retry
					future.channel().eventLoop().schedule(() -> {
						doConnect();
					}, 1000, TimeUnit.MILLISECONDS);
					return;
				}
				// retry times if fails
				nettyClient.connectFailRetryTimesInc(retryTimes);
				if (future.cause() != null) {
					// notify the error, and notify all objects which call waitForChannel method
					notifyOfError(new RemoteTransportException(
						"Connecting to remote task manager + '" + connectionId.getAddress() +
							"' has failed. This might indicate that the remote task " +
							"manager has been lost.",
						connectionId.getAddress(), future.cause()));
				}
				else {
					// notify the error, and notify all objects which call waitForChannel method
					notifyOfError(new LocalTransportException(
						String.format(
							"Connecting to remote task manager '%s' has been cancelled.",
							connectionId.getAddress()),
						null));
				}
			}
		}
	}
}
