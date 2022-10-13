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
import org.apache.flink.runtime.socket.result.JobResultClientManager;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receive job result from task and transfer it to the socket client of the given job.
 */
public class PushTaskResultHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(PushTaskResultHandler.class);

	private final JobResultClientManager jobResultClientManager;

	public PushTaskResultHandler(JobResultClientManager jobResultClientManager) {
		this.jobResultClientManager = jobResultClientManager;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof JobSocketResult) {
			JobSocketResult socketResult = (JobSocketResult) msg;
			jobResultClientManager.writeJobResult(socketResult);
		} else {
			ctx.fireChannelRead(msg);
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.error("PushTaskResultHandler occur error: ", cause);
		ctx.close();
		throw new IllegalStateException(cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();
		String channelId = channel.id().toString();
		LOG.warn("Channel id {}, local address {}, remote address {}, active status {}.",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isActive());
		ctx.fireChannelInactive();
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();

		long outBoundBufSize = ((NioSocketChannel) channel).unsafe().outboundBuffer().totalPendingWriteBytes();
		String channelId = channel.id().toString();
		LOG.debug("Channel id {}, local address {}, remote address {}, writable status {}, outbound buffer size {}",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isWritable(), outBoundBufSize);
		ctx.fireChannelWritabilityChanged();
	}
}
