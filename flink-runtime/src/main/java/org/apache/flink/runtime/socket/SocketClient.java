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

import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.api.common.socket.SocketResultIterator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherSocketEndpoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.socket.handler.SocketJobResultHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This client is the counter-part to the {@link DispatcherSocketEndpoint}.
 * NOTICE: This socket client is not thread safe.
 */
@NotThreadSafe
public class SocketClient implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(SocketClient.class);

	private final String address;
	private final int port;
	private final int connectTimeoutMills;
	private final BlockingQueue<JobSocketResult> jobResultList;

	private Bootstrap bootstrap;
	private Channel channel;

	public SocketClient(String address, int port, int connectTimeoutMills) {
		this.address = address;
		this.port = port;
		this.connectTimeoutMills = connectTimeoutMills;
		this.jobResultList = new LinkedBlockingQueue<>();
	}

	public final void start() throws Exception {
		NioEventLoopGroup group = new NioEventLoopGroup(1, new ExecutorThreadFactory("flink-socket-client-netty"));
		bootstrap = new Bootstrap();
		bootstrap
			.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMills)
			.group(group)
			.channel(NioSocketChannel.class)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					ChannelPipeline p = socketChannel.pipeline();
					p.addLast(new ObjectEncoder());
					p.addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
					p.addLast(new SocketJobResultHandler(jobResultList));
				}
			});
		final ChannelFuture connectFuture = bootstrap.connect(address, port).sync();
		connectFuture.addListener((ChannelFutureListener) channelFuture -> {
			if (channelFuture.isSuccess()) {
				LOG.info("Connect to {}:{} successful", address, port);
			} else {
				LOG.error("Connect to {}:{} fail", address, port, channelFuture.cause());
				group.shutdownGracefully();
			}
		});
		channel = connectFuture.channel();
	}

	public <T> CloseableIterator<T> submitJob(JobGraph jobGraph) {
		channel.writeAndFlush(jobGraph).addListener((ChannelFutureListener) channelFuture -> {
			if (!channelFuture.isSuccess()) {
				LOG.error("Submit job {} to {}:{} fail", address, port, channelFuture.cause());
				jobResultList.add(
					new JobSocketResult.Builder()
						.setJobId(jobGraph.getJobID())
						.setResultStatus(ResultStatus.FAIL)
						.setSerializedThrowable(new SerializedThrowable(channelFuture.cause()))
						.build());
			}
		});
		return new SocketResultIterator<>(jobGraph.getJobID(), jobResultList);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final Time terminationTimeout = Time.seconds(10L);
		CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully(0L, terminationTimeout.toMilliseconds(), TimeUnit.MILLISECONDS)
					.addListener(finished -> {
						if (finished.isSuccess()) {
							terminationFuture.complete(null);
						} else {
							terminationFuture.completeExceptionally(finished.cause());
						}
					});
			}
		}
		return terminationFuture;
	}
}
