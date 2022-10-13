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
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherSocketRestEndpoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.socket.result.JobChannelManager;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OptionalConsumer;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This handler can be used to submit jobs to a Flink cluster by {@link DispatcherSocketRestEndpoint}.
 */
public class SocketJobSubmitHandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(SocketJobSubmitHandler.class);

	private final GatewayRetriever<DispatcherGateway> leaderRetriever;
	private final JobResultClientManager jobResultClientManager;
	private final Time timeout;
	private final long blockTimeout;

	public SocketJobSubmitHandler(
		GatewayRetriever<DispatcherGateway> leaderRetriever,
		JobResultClientManager jobResultClientManager,
		Time timeout,
		long blockTimeout) {
		this.leaderRetriever = leaderRetriever;
		this.jobResultClientManager = jobResultClientManager;
		this.timeout = timeout;
		this.blockTimeout = blockTimeout;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof JobGraph) {
			JobGraph jobGraph = (JobGraph) msg;
			jobResultClientManager.addJobChannelManager(
				jobGraph.getJobID(),
				new JobChannelManager(jobGraph.getJobID(), ctx, computeFinishTaskCount(jobGraph), jobResultClientManager, blockTimeout));

			OptionalConsumer<DispatcherGateway> optLeaderConsumer = OptionalConsumer.of(leaderRetriever.getNow());
			optLeaderConsumer.ifPresent(
				gateway -> gateway.submitJob(jobGraph, ctx, timeout)
			).ifNotPresent(
				() -> {
					JobSocketResult jobSocketResult = new JobSocketResult.Builder()
						.setJobId(jobGraph.getJobID())
						.setResultStatus(ResultStatus.FAIL)
						.setSerializedThrowable(new SerializedThrowable(new Exception("Get dispatcher gateway failed.")))
						.build();
					ctx.writeAndFlush(jobSocketResult);
				});
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	public static int computeFinishTaskCount(JobGraph jobGraph) {
		List<JobVertex> vertexList = jobGraph.getVerticesSortedTopologicallyFromSources();
		checkArgument(!vertexList.isEmpty(), "There are no vertices in the job");
		return vertexList.get(vertexList.size() - 1).getParallelism();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		LOG.error("SocketJobSubmitHandler occur error: ", cause);
		ctx.close();
		throw new IllegalStateException(cause);
	}

	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();
		String channelId = channel.id().toString();
		LOG.warn("Channel id {}, local address {}, remote address {}, active status {}.",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isActive());
		ctx.fireChannelInactive();
	}

	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		Channel channel = ctx.channel();

		long outBoundBufSize = ((NioSocketChannel) channel).unsafe().outboundBuffer().totalPendingWriteBytes();
		String channelId = channel.id().toString();
		LOG.debug("Channel id {}, local address {}, remote address {}, writable status {}, outbound buffer size {}",
			channelId, channel.localAddress().toString(), channel.remoteAddress().toString(), channel.isWritable(), outBoundBufSize);
		ctx.fireChannelWritabilityChanged();
	}
}
