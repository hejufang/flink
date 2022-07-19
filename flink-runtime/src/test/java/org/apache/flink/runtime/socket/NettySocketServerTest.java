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

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test case for netty socket server.
 */
public class NettySocketServerTest {
	@Test
	public void testServerReceiveString() throws Exception {
		CompletableFuture<String> receiveFuture = new CompletableFuture<>();
		String message = "Hello Netty";
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
						receiveFuture.complete((String) msg);
					}
				}
			))) {
			nettySocketServer.start();

			try (NettySocketClient nettySocketClient = new NettySocketClient(
					nettySocketServer.getAddress(),
					nettySocketServer.getPort(),
					10000,
					channelPipeline -> channelPipeline.addLast(
						new ObjectEncoder(),
						new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null))))) {
				nettySocketClient.start();
				nettySocketClient.getChannel().writeAndFlush(message);

				assertEquals(message, receiveFuture.get());
			}
		}
	}

	@Test
	public void testServerReceiveString2() throws Exception {
		CompletableFuture<String> receiveFuture = new CompletableFuture<>();
		CompletableFuture<Boolean> exceptionFuture = new CompletableFuture<>();
		String message = "Hello Netty";
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
						receiveFuture.complete((String) msg);
					}
				}
			))) {
			nettySocketServer.start();

			try (NettySocketClient nettySocketClient = new NettySocketClient(
				nettySocketServer.getAddress(),
				nettySocketServer.getPort(),
				100000,
				channelPipeline -> channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null))))) {
				nettySocketClient.start();
				Thread thread = new Thread(() -> {
					try {
						Channel channel = nettySocketClient.getChannel();
						while (channel.isWritable()) {
							channel.writeAndFlush(message);
							Thread.sleep(10L);
							if (!channel.isActive()) {
								exceptionFuture.complete(true);
								break;
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
				thread.start();
				assertEquals(message, receiveFuture.get());
				nettySocketServer.close();
				exceptionFuture.get(1000L, TimeUnit.MILLISECONDS);
			}
		}
	}

}