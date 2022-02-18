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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.socket.handler.SocketJobSubmitHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.NetUtils;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrapConfig;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Socket endpoint for the {@link Dispatcher} component.
 */
public class DispatcherSocketEndpoint implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(DispatcherSocketEndpoint.class);

	private final GatewayRetriever<DispatcherGateway> leaderRetriever;
	private final Time timeout;
	private final Configuration configuration;
	private final String address;
	private final String socketPortRange;
	private final int connectBacklog;
	private int socketPort;

	private ServerBootstrap bootstrap;
	private Channel serverChannel;

	public DispatcherSocketEndpoint(
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			Configuration configuration) {
		this.leaderRetriever = leaderRetriever;
		this.configuration = configuration;
		this.address = configuration.getString(RestOptions.BIND_ADDRESS);
		this.socketPortRange = configuration.getString(RestOptions.BIND_SOCKET_PORT);
		this.connectBacklog = configuration.getInteger(RestOptions.DISPATCHER_CONNECT_BACKLOG);
		this.timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));
	}

	public final void start() throws Exception {
		NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new ExecutorThreadFactory("flink-socket-server-netty-boss"));
		NioEventLoopGroup workerGroup = new NioEventLoopGroup(0, new ExecutorThreadFactory("flink-socket-server-netty-worker"));

		bootstrap = new ServerBootstrap();
		bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class);
		if (connectBacklog > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, configuration.getInteger(RestOptions.DISPATCHER_CONNECT_BACKLOG));
		}

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new ObjectEncoder())
					.addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)))
					.addLast(new SocketJobSubmitHandler(leaderRetriever, timeout));
			}
		});

		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(socketPortRange);
		} catch (IllegalConfigurationException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + socketPortRange);
		}

		int chosenPort = 0;
		while (portsIterator.hasNext()) {
			try {
				chosenPort = portsIterator.next();
				final ChannelFuture channel;
				if (address == null) {
					channel = bootstrap.bind(chosenPort);
				} else {
					channel = bootstrap.bind(address, chosenPort);
				}
				serverChannel = channel.syncUninterruptibly().channel();
				break;
			} catch (final Exception e) {
				// continue if the exception is due to the port being in use, fail early otherwise
				if (!(e instanceof org.jboss.netty.channel.ChannelException)) {
					throw e;
				}
			}
		}
		if (serverChannel == null) {
			throw new BindException("Could not start socket endpoint on any port in port range " + socketPortRange);
		}

		LOG.debug("Binding socket endpoint to {}:{}.", address, chosenPort);

		final InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
		final String advertisedAddress;
		if (bindAddress.getAddress().isAnyLocalAddress()) {
			advertisedAddress = this.address;
		} else {
			advertisedAddress = bindAddress.getAddress().getHostAddress();
		}
		socketPort = bindAddress.getPort();

		LOG.info("Socket endpoint listening at {}:{}", advertisedAddress, socketPort);
	}

	public String getAddress() {
		return address;
	}

	public int getSocketPort() {
		return socketPort;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		CompletableFuture<?> channelFuture = new CompletableFuture<>();
		if (serverChannel != null) {
			serverChannel.close().addListener(finished -> {
				if (finished.isSuccess()) {
					channelFuture.complete(null);
				} else {
					channelFuture.completeExceptionally(finished.cause());
				}
			});
			serverChannel = null;
		}

		final CompletableFuture<Void> channelTerminationFuture = new CompletableFuture<>();

		channelFuture.thenRun(() -> {
			CompletableFuture<?> groupFuture = new CompletableFuture<>();
			CompletableFuture<?> childGroupFuture = new CompletableFuture<>();
			final Time gracePeriod = Time.seconds(10L);

			if (bootstrap != null) {
				final ServerBootstrapConfig config = bootstrap.config();
				final EventLoopGroup group = config.group();
				if (group != null) {
					group.shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
						.addListener(finished -> {
							if (finished.isSuccess()) {
								groupFuture.complete(null);
							} else {
								groupFuture.completeExceptionally(finished.cause());
							}
						});
				} else {
					groupFuture.complete(null);
				}

				final EventLoopGroup childGroup = config.childGroup();
				if (childGroup != null) {
					childGroup.shutdownGracefully(0L, gracePeriod.toMilliseconds(), TimeUnit.MILLISECONDS)
						.addListener(finished -> {
							if (finished.isSuccess()) {
								childGroupFuture.complete(null);
							} else {
								childGroupFuture.completeExceptionally(finished.cause());
							}
						});
				} else {
					childGroupFuture.complete(null);
				}

				bootstrap = null;
			} else {
				// complete the group futures since there is nothing to stop
				groupFuture.complete(null);
				childGroupFuture.complete(null);
			}

			CompletableFuture<Void> combinedFuture = FutureUtils.completeAll(Arrays.asList(groupFuture, childGroupFuture));

			combinedFuture.whenComplete(
				(Void ignored, Throwable throwable) -> {
					if (throwable != null) {
						channelTerminationFuture.completeExceptionally(throwable);
					} else {
						channelTerminationFuture.complete(null);
					}
				});
		});

		return channelTerminationFuture;
	}
}
