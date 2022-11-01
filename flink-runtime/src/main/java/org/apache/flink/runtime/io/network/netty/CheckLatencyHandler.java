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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler to read the messages of time of send, to statistics network delay.
 */
public class CheckLatencyHandler extends ChannelInboundHandlerAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(CheckLatencyHandler.class);

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof NettyMessage.LatencyCheck) {
			NettyMessage.LatencyCheck latencyCheck = (NettyMessage.LatencyCheck) msg;
			LOGGER.debug("network latencyCheck = {}", latencyCheck);
			if (latencyCheck.sendTime != 0) {
				latencyCheck.receiveTime = System.currentTimeMillis();
				ctx.writeAndFlush(latencyCheck);
			}
		} else {
			ctx.fireChannelRead(msg);
		}
	}
}
