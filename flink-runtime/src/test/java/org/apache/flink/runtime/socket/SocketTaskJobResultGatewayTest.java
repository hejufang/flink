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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Test case for socket task job result gateway.
 */
public class SocketTaskJobResultGatewayTest {
	@Test
	public void testGatewayWriterWaterMark() throws Exception {
		final int totalCount = 200;
		AtomicInteger messageCount = new AtomicInteger(0);
		CompletableFuture<Void> finishFuture = new CompletableFuture<>();
		try (NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) {
						JobSocketResult jobSocketResult = (JobSocketResult) msg;
						if (jobSocketResult.isFinish()) {
							finishFuture.complete(null);
						}
						messageCount.incrementAndGet();
					}
				}
			))) {
			nettySocketServer.start();

			JobID jobId = new JobID();
			Configuration configuration = new Configuration();
			configuration.set(TaskManagerOptions.RESULT_PUSH_NETTY_LOW_WRITER_BUFFER_MARK, 10);
			configuration.set(TaskManagerOptions.RESULT_PUSH_NETTY_HIGH_WRITER_BUFFER_MARK, 100);
			SocketPushJobResultListener listener = new SocketPushJobResultListener();
			try (SocketTaskJobResultGateway socketTaskJobResultGateway =
					new SocketTaskJobResultGateway(1, 0, configuration)) {
				socketTaskJobResultGateway.connect(nettySocketServer.getAddress(), nettySocketServer.getPort());
				for (int i = 1; i <= totalCount - 1; i++) {
					String value = "Hello " + i + " World";
					socketTaskJobResultGateway.sendResult(jobId, value.getBytes(), ResultStatus.PARTIAL, listener);
				}
				String value = "Hello " + totalCount + " World";
				socketTaskJobResultGateway.sendResult(jobId, value.getBytes(), ResultStatus.COMPLETE, listener);
				finishFuture.get(10, TimeUnit.SECONDS);
				assertEquals(totalCount, messageCount.get());
			}
		}
	}

	@Test
	public void testGatewayServerClosed() throws Exception {
		NettySocketServer nettySocketServer = new NettySocketServer(
			"test",
			"localhost",
			"0",
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx, Object msg) {
					}
				}
			));
		nettySocketServer.start();

		JobID jobId = new JobID();
		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.RESULT_PUSH_NETTY_LOW_WRITER_BUFFER_MARK, 10);
		configuration.set(TaskManagerOptions.RESULT_PUSH_NETTY_HIGH_WRITER_BUFFER_MARK, 100);

		CompletableFuture<Void> closeFuture = new CompletableFuture<>();
		SocketPushJobResultListener listener = new SocketPushJobResultListener();
		try (SocketTaskJobResultGateway socketTaskJobResultGateway =
				new SocketTaskJobResultGateway(1, 0, configuration)) {
			socketTaskJobResultGateway.connect(nettySocketServer.getAddress(), nettySocketServer.getPort());

			NettySocketClient nettySocketClient = socketTaskJobResultGateway.getClientList().get(0);
			nettySocketClient.getChannel().closeFuture().addListener((ChannelFutureListener) channelFuture -> closeFuture.complete(null));

			for (int i = 1; i <= 10; i++) {
				String value = "Hello " + i + " World";
				socketTaskJobResultGateway.sendResult(jobId, value.getBytes(), ResultStatus.PARTIAL, listener);
			}
			nettySocketServer.closeAsync();
			closeFuture.get(10, TimeUnit.SECONDS);

			assertThrows("The channel is closed", RuntimeException.class, () -> {
				String value = "Hello  World";
				socketTaskJobResultGateway.sendResult(jobId, value.getBytes(), ResultStatus.PARTIAL, listener);
				return null;
			});
		}
	}
}
