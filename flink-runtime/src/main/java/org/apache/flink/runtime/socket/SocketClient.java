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
import org.apache.flink.runtime.dispatcher.DispatcherSocketRestEndpoint;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.socket.handler.SocketJobResultHandler;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This client is the counter-part to the {@link DispatcherSocketRestEndpoint}.
 * NOTICE: This socket client is not thread safe.
 */
@NotThreadSafe
public class SocketClient implements AutoCloseableAsync {
	private static final Logger LOG = LoggerFactory.getLogger(SocketClient.class);

	private final NettySocketClient nettySocketClient;
	private final BlockingQueue<JobSocketResult> jobResultList;

	public SocketClient(String address, int port, int connectTimeoutMills) {
		this.jobResultList = new LinkedBlockingQueue<>();
		this.nettySocketClient = new NettySocketClient(
			address,
			port,
			connectTimeoutMills,
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new SocketJobResultHandler(jobResultList)));
	}

	public final void start() throws Exception {
		this.nettySocketClient.start();
	}

	public <T> CloseableIterator<T> submitJob(JobGraph jobGraph) {
		this.nettySocketClient.getChannel().writeAndFlush(jobGraph).addListener((ChannelFutureListener) channelFuture -> {
			if (!channelFuture.isSuccess()) {
				LOG.error("Submit job {} to {}:{} fail",
					this.nettySocketClient.getAddress(),
					this.nettySocketClient.getPort(), channelFuture.cause());
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
		return this.nettySocketClient.closeAsync();
	}
}
