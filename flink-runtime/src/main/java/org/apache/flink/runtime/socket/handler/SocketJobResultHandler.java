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

package org.apache.flink.runtime.socket.handler;

import org.apache.flink.api.common.socket.JobSocketResult;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Socket job result handler.
 */
public class SocketJobResultHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(SocketJobSubmitHandler.class);

	private final BlockingQueue<JobSocketResult> resultList;

	public SocketJobResultHandler(BlockingQueue<JobSocketResult> resultList) {
		this.resultList = resultList;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		boolean blocked = true;
		Channel channel = ctx.channel();
		while (blocked) {
			blocked = !resultList.offer((JobSocketResult) msg, 10L, TimeUnit.MILLISECONDS);
			if (blocked) {
				LOG.warn("Channel id {}, local address {}, remote address {}, is blocked.", channel.id().toString(), channel.localAddress().toString(), channel.remoteAddress().toString());
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.error("SocketJobResultHandler occur error: ", cause);
		ctx.close();
		throw new RuntimeException(cause);
	}

	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();
		String channelId = channel.id().toString();
		LOG.warn("Channel id {}, local address {}, remote address {}, active status {}.",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isActive());
		ctx.fireChannelInactive();
	}

	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		long outBoundBufSize = ((NioSocketChannel) ctx.channel()).unsafe().outboundBuffer().totalPendingWriteBytes();
		Channel channel = ctx.channel();
		String channelId = channel.id().toString();
		LOG.warn("Channel id {}, local address {}, remote address {}, writable status {}, outbound buffer size {}",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isWritable(), outBoundBufSize);
		ctx.fireChannelWritabilityChanged();
	}
}
