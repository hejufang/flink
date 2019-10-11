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

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatHandler extends ChannelInboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(HeartBeatHandler.class);

	private volatile int readIdleCount = 0;
	private final int maxIdleCount;

	public HeartBeatHandler(int maxIdleCount) {
		this.maxIdleCount = maxIdleCount;
		LOGGER.info("maxIdleCount = {}", this.maxIdleCount);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof NettyMessage.HeartBeat) {
			NettyMessage.HeartBeat heartBeat = (NettyMessage.HeartBeat) msg;
			LOGGER.debug("heartbeat = {}", heartBeat);
			readIdleCount = 0;
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
			if (idleStateEvent.state() == IdleState.READER_IDLE) {
				if (readIdleCount++ > maxIdleCount) {
					String errMsg = String.format("client %s lost connection to %s",
						ctx.channel().localAddress(), ctx.channel().remoteAddress());
					LOGGER.error(errMsg);
					ctx.fireExceptionCaught(new Exception(errMsg));
				} else {
					LOGGER.debug("It is {} idleStateEvent", readIdleCount);
					sendHeartBeat(ctx);
				}
			}
		} else {
			ctx.fireUserEventTriggered(evt);
		}
	}

	private void sendHeartBeat(ChannelHandlerContext ctx) {
		ctx.writeAndFlush(new NettyMessage.HeartBeat());
	}
}
