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
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Netty channel manager manages connection pool.
 */
class NettyChannelManager {
	private static final Logger LOG = LoggerFactory.getLogger(NettyChannelManager.class);
	private final long channelIdleReleaseTimeMs;
	private final ConcurrentMap<ConnectionID, Object> requests = new ConcurrentHashMap<>();
	private final ConcurrentMap<ConnectionID, AtomicInteger> requestsRefCount = new ConcurrentHashMap<>();
	private final ConcurrentMap<SocketAddress, Queue<ChannelInfo>> channelPool = new ConcurrentHashMap<>();

	public NettyChannelManager(long channelIdleReleaseTimeMs) {
		this.channelIdleReleaseTimeMs = channelIdleReleaseTimeMs;
		Runnable destroyRunnable = this::destroyInactiveChannel;
		ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
		schedule.scheduleWithFixedDelay(destroyRunnable, 1, 5, TimeUnit.MINUTES);
	}

	public ChannelInfo getChannel(ConnectionID connectionID) throws InterruptedException {
		Object entry;
		ChannelInfo channelRequestResult = null;

		while (channelRequestResult == null) {
			entry = requests.get(connectionID);

			if (entry != null) {
				if (entry instanceof ChannelInfo) {
					channelRequestResult = (ChannelInfo) entry;
				} else {
					ChannelRequest request = (ChannelRequest) entry;
					channelRequestResult = request.waitForChannel();
					requests.replace(connectionID, request, channelRequestResult);
				}
			} else {
				ChannelRequest request = new ChannelRequest(this, connectionID);
				Object old = requests.putIfAbsent(connectionID, request);
				if (old == null) {
					channelRequestResult = request.findIdleChannel();
					requests.replace(connectionID, request, channelRequestResult);
				} else if (old instanceof ChannelRequest) {
					channelRequestResult = ((ChannelRequest) old).waitForChannel();
					requests.replace(connectionID, old, channelRequestResult);
				} else {
					channelRequestResult = (ChannelInfo) old;
				}
			}
		}

		return channelRequestResult;
	}

	public void addChannel(ConnectionID connectionID, Channel channel, NetworkClientHandler clientHandler) {
		synchronized (NettyChannelManager.class) {
			Preconditions.checkArgument(requests.containsKey(connectionID));
			ChannelInfo old = (ChannelInfo) requests.get(connectionID);
			ChannelInfo channelInfo = new ChannelInfo(channel, clientHandler, connectionID, channelIdleReleaseTimeMs);
			channelPool.putIfAbsent(connectionID.getAddress(), new ConcurrentLinkedQueue<>());
			requests.replace(connectionID, checkNotNull(old), channelInfo);
		}
	}

	public void releaseChannel(ConnectionID connectionID) {
		synchronized (NettyChannelManager.class) {
			Object old = requests.get(connectionID);
			if (old instanceof ChannelInfo) {
				ChannelInfo releasedChannel = (ChannelInfo) old;
				AtomicInteger refCount = requestsRefCount.get(connectionID);
				if (refCount.get() == 0) {
					LOG.debug("[netty-reuse-debug] connection id: {} remove from requests map", connectionID);
					requests.remove(connectionID);
					requestsRefCount.remove(connectionID);
				}
				if (!releasedChannel.isIdle()) {
					releasedChannel.setIdle();
					channelPool.get(connectionID.getAddress()).add(releasedChannel);
				}
			}
		}
	}

	public void shutdown() {
		// Close the TCP connection. Send a close request msg to ensure
		// that outstanding backwards task events are not discarded.
		for (Map.Entry< SocketAddress, Queue<ChannelInfo> > entry : channelPool.entrySet()) {
			for (ChannelInfo channelInfo : entry.getValue()) {
				if (channelInfo.isAvailable()) {
					channelInfo.getChannel()
						.writeAndFlush(new NettyMessage.CloseRequest())
						.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
				}
			}
		}

		for (Map.Entry<SocketAddress, Queue<ChannelInfo>> entry:
			channelPool.entrySet()) {
			entry.getValue().clear();
		}
		channelPool.clear();
	}

	public void addReference(ConnectionID connectionID) {
		Preconditions.checkNotNull(connectionID);

		synchronized (NettyChannelManager.class) {
			requestsRefCount.putIfAbsent(connectionID, new AtomicInteger());
			int refCount = requestsRefCount.get(connectionID).incrementAndGet();
			LOG.debug("[netty-reuse-debug] connection id: {} reference count: {}", connectionID, refCount);
		}
	}

	public void decreaseRequestCount(ConnectionID connectionID) {
		Preconditions.checkNotNull(connectionID);

		synchronized (NettyChannelManager.class) {
			AtomicInteger refCount = requestsRefCount.get(connectionID);
			refCount.decrementAndGet();
		}
	}

	private void destroyInactiveChannel() {
		synchronized (NettyChannelManager.class) {
			for (Map.Entry<SocketAddress, Queue<ChannelInfo>> entry : channelPool.entrySet()) {
				for (ChannelInfo channel : entry.getValue()) {
					if (channel.idleTooLong()) {
						LOG.debug("[netty-reuse-debug] destroy channel: {}", channel);
						channel.destroy();
						if (channel.channel.isActive()) {
							channel.channel.writeAndFlush(new NettyMessage.CloseRequest())
								.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
						} else {
							LOG.warn("[netty-reuse-debug] channel: {} already inactive", channel);
						}
					}
				}
			}

			for (Map.Entry<SocketAddress, Queue<ChannelInfo>> entry : channelPool.entrySet()) {
				entry.getValue().removeIf(ChannelInfo::canDestroy);
			}
		}
	}

	private Queue<ChannelInfo> getChannelPool(SocketAddress address) {
		return channelPool.getOrDefault(address, null);
	}

	@VisibleForTesting
	int getTotalPoolSize() {
		int totalIdleChannel = 0;
		for (Queue<ChannelInfo> channelInfoQueue: channelPool.values()) {
			totalIdleChannel += channelInfoQueue.size();
		}
		return totalIdleChannel;
	}

	@VisibleForTesting
	int getNumIdleChannels(ConnectionID connectionID) {
		Preconditions.checkArgument(channelPool.containsKey(Preconditions.checkNotNull(connectionID).getAddress()));
		return channelPool.get(connectionID.getAddress()).size();
	}

	@VisibleForTesting
	Object getChannelRequest(ConnectionID connectionID) {
		Preconditions.checkArgument(requests.containsKey(connectionID));
		return requests.get(connectionID);
	}

	@VisibleForTesting
	boolean containsRequest(ConnectionID connectionID) {
		return requests.containsKey(connectionID);
	}

	static final class ChannelRequest {
		private final Object requestLock = new Object();
		private final ConnectionID connectionID;
		private volatile ChannelInfo result;
		private final NettyChannelManager channelManager;

		public ChannelRequest(NettyChannelManager channelManager, ConnectionID connectionID) {
			this.connectionID = connectionID;
			this.channelManager = channelManager;
		}

		private ChannelInfo findIdleChannel() {
			synchronized (requestLock) {
				Queue<ChannelInfo> queue = channelManager.getChannelPool(connectionID.getAddress());
				if (queue != null) {
					synchronized (NettyChannelManager.class) {
						if (!queue.isEmpty()) {
							ChannelInfo channelInfo = queue.remove();
							if (channelInfo.isIdle() && !channelInfo.canDestroy()) {
								channelInfo.setConnectionID(connectionID);
								result = channelInfo;
							} else {
								LOG.warn("[netty-reuse-debug] connection id: {} get unsuitable channelInfo {}, isActive {}, ",
									connectionID,
									channelInfo,
									channelInfo.channel.isActive());
								result = new ChannelInfo();
							}
						} else {
							result = new ChannelInfo();
						}
					}
				} else {
					result = new ChannelInfo();
				}

				requestLock.notifyAll();
				return result;
			}
		}

		private ChannelInfo waitForChannel() throws InterruptedException {
			synchronized (requestLock) {
				while (result == null) {
					requestLock.wait(100);
				}
			}
			return result;
		}
	}

	static final class ChannelInfo {
		private ConnectionID connectionID;
		private final Channel channel;
		private final NetworkClientHandler clientHandler;
		private boolean canDestroy;
		private long lastUsedTimestamp;
		private final long channelIdleReleaseTimeMs;

		public ChannelInfo() {
			this(null, null, null, 0);
		}

		public ChannelInfo(Channel channel, NetworkClientHandler clientHandler, ConnectionID connectionID) {
			this(channel, clientHandler, connectionID, 0);
		}

		public ChannelInfo(
				Channel channel,
				NetworkClientHandler clientHandler,
				ConnectionID connectionID,
				long channelIdleReleaseTimeMs) {
			this.channel = channel;
			this.clientHandler = clientHandler;
			this.connectionID = connectionID;
			this.channelIdleReleaseTimeMs = channelIdleReleaseTimeMs;
			this.canDestroy = false;
			this.lastUsedTimestamp = System.currentTimeMillis();
		}

		private void setConnectionID(ConnectionID connectionID) {
			this.connectionID = connectionID;
			this.lastUsedTimestamp = System.currentTimeMillis();
			this.canDestroy = false;
		}

		public Channel getChannel() {
			return this.channel;
		}

		public NetworkClientHandler getClientHandler() {
			return this.clientHandler;
		}

		public ConnectionID getConnectionID() {
			return this.connectionID;
		}

		public boolean isAvailable() {
			return this.channel != null && this.clientHandler != null;
		}

		private void destroy() {
			this.canDestroy = true;
		}

		private boolean canDestroy() {
			return this.canDestroy || !channel.isActive();
		}

		private boolean idleTooLong() {
			long idleTime = System.currentTimeMillis() - this.lastUsedTimestamp;
			return this.connectionID == null && idleTime > channelIdleReleaseTimeMs;
		}

		private void setIdle() {
			this.connectionID = null;
		}

		private boolean isIdle() {
			return this.connectionID == null && channel.isActive();
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}

			if (!(o instanceof ChannelInfo)) {
				return false;
			}

			ChannelInfo c = (ChannelInfo) o;

			return Objects.equals(connectionID, c.connectionID)
				&& Objects.equals(channel, c.channel)
				&& Objects.equals(clientHandler, c.clientHandler)
				&& canDestroy == c.canDestroy;
		}

		@Override
		public String toString() {
			return "ChannelInfo{" +
				"connectionID=" + connectionID +
				", channel=" + channel +
				", clientHandler=" + clientHandler +
				", canDestroy=" + canDestroy +
				", lastUsedTimestamp=" + lastUsedTimestamp +
				", channelIdleReleaseTimeMs=" + channelIdleReleaseTimeMs +
				'}';
		}
	}
}
