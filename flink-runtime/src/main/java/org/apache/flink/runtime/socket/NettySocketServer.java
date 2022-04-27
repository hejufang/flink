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

package org.apache.flink.runtime.socket;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Netty socket server will receive request from {@link NettySocketClient}.
 */
public class NettySocketServer implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(NettySocketServer.class);

	private final String tag;
	private final int connectBacklog;
	private final Consumer<ChannelPipeline> channelPipelineConsumer;
	private final int workerCount;
	private final String portRange;

	private String address;
	private int port;
	private ServerBootstrap bootstrap;
	private Channel serverChannel;

	public NettySocketServer(String tag, String address, String portRange) {
		this(tag, address, portRange, null);
	}

	public NettySocketServer(String tag, String address, String portRange, Consumer<ChannelPipeline> channelPipelineConsumer) {
		this(tag, address, portRange, channelPipelineConsumer, 0);
	}

	public NettySocketServer(String tag, String address, String portRange, Consumer<ChannelPipeline> channelPipelineConsumer, int connectBacklog) {
		this(tag, address, portRange, channelPipelineConsumer, connectBacklog, 0);
	}

	public NettySocketServer(
			String tag,
			String address,
			String portRange,
			Consumer<ChannelPipeline> channelPipelineConsumer,
			int connectBacklog,
			int workerCount) {
		this.tag = tag;
		this.address = address;
		this.portRange = portRange;
		this.channelPipelineConsumer = channelPipelineConsumer;
		this.connectBacklog = connectBacklog;
		this.workerCount = workerCount;
	}

	public String getAddress() {
		return address;
	}

	public int getPort() {
		return port;
	}

	public void start() throws Exception {
		NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, new ExecutorThreadFactory("flink-" + tag + "-server-netty-boss"));
		NioEventLoopGroup workerGroup = new NioEventLoopGroup(workerCount, new ExecutorThreadFactory("flink-" + tag + "-server-netty-worker"));

		bootstrap = new ServerBootstrap();
		bootstrap
			.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class);
		if (connectBacklog > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, connectBacklog);
		}

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) {
				if (channelPipelineConsumer != null) {
					channelPipelineConsumer.accept(ch.pipeline());
				}
			}
		});

		Iterator<Integer> portsIterator;
		try {
			portsIterator = NetUtils.getPortRangeFromString(portRange);
		} catch (IllegalConfigurationException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid port range definition: " + portRange);
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
			throw new BindException("Could not start socket endpoint on any port in port range " + portRange);
		}

		LOG.debug("Binding socket endpoint to {}:{}.", address, chosenPort);

		final InetSocketAddress bindAddress = (InetSocketAddress) serverChannel.localAddress();
		final String advertisedAddress;
		if (bindAddress.getAddress().isAnyLocalAddress()) {
			advertisedAddress = this.address;
		} else {
			advertisedAddress = bindAddress.getAddress().getHostAddress();
		}
		address = advertisedAddress;
		port = bindAddress.getPort();

		LOG.info("Socket endpoint listening at {}:{}", address, port);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final CompletableFuture<Void> channelTerminationFuture = new CompletableFuture<>();
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

		return channelTerminationFuture;
	}
}
