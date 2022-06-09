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

package org.apache.flink.runtime.taskexecutor.netty;

import org.apache.flink.runtime.deployment.JobDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.TaskExecutorSocketAddress;
import org.apache.flink.runtime.socket.NettySocketClient;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Netty client used by job master to send request to {@link TaskExecutorNettyServer} in task executor.
 */
public class TaskExecutorNettyClient implements Closeable {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorNettyClient.class);

	private final TaskExecutorSocketAddress socketAddress;
	private final int channelCount;
	private final int connectTimeoutMills;
	private final int lowWaterMark;
	private final int highWaterMark;
	private final List<NettySocketClient> clientList;
	private final boolean jobDeploymentEnabled;

	public TaskExecutorNettyClient(
			TaskExecutorSocketAddress socketAddress,
			int channelCount,
			int connectTimeoutMills,
			int lowWaterMark,
			int highWaterMark) {
		this(socketAddress, channelCount, connectTimeoutMills, lowWaterMark, highWaterMark, false);
	}

	public TaskExecutorNettyClient(
			TaskExecutorSocketAddress socketAddress,
			int channelCount,
			int connectTimeoutMills,
			int lowWaterMark,
			int highWaterMark,
			boolean jobDeploymentEnabled) {
		this.socketAddress = socketAddress;
		this.channelCount = channelCount;
		this.connectTimeoutMills = connectTimeoutMills;
		this.lowWaterMark = lowWaterMark;
		this.highWaterMark = highWaterMark;
		this.clientList = new ArrayList<>(channelCount);
		this.jobDeploymentEnabled = jobDeploymentEnabled;
	}

	public void start() throws Exception {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (jobDeploymentEnabled) {
			channelPipelineConsumer = channelPipeline -> channelPipeline
				.addLast(new JobDeploymentEncoder())
				.addLast(new JobDeploymentDecoder());
		} else {
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
		}

		for (int i = 0; i < channelCount; i++) {
			NettySocketClient nettySocketClient = new NettySocketClient(
				socketAddress.getAddress(),
				socketAddress.getPort(),
				connectTimeoutMills,
				lowWaterMark,
				highWaterMark,
				channelPipelineConsumer);
			nettySocketClient.start();
			clientList.add(nettySocketClient);
		}
	}

	public CompletableFuture<Acknowledge> submitTaskList(
			String jobMasterAddress,
			List<TaskDeploymentDescriptor> deploymentDescriptorList,
			JobMasterId jobMasterId) {
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				channel.writeAndFlush(new JobTaskListDeployment(jobMasterAddress, deploymentDescriptorList, jobMasterId))
					.addListener((ChannelFutureListener) channelFuture -> {
						if (channelFuture.isSuccess()) {
							submitFuture.complete(Acknowledge.get());
						} else {
							submitFuture.completeExceptionally(channelFuture.cause());
						}
					});
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		return submitFuture;
	}

	public CompletableFuture<Acknowledge> submitTaskList(
			String jobMasterAddress,
			JobDeploymentDescriptor jdd,
			JobMasterId jobMasterId) {
		CompletableFuture<Acknowledge> submitFuture = new CompletableFuture<>();
		Channel channel = clientList.get(RandomUtils.nextInt(0, clientList.size())).getChannel();
		while (true) {
			if (channel.isWritable()) {
				channel.writeAndFlush(new JobDeployment(jobMasterAddress, jdd, jobMasterId))
					.addListener((ChannelFutureListener) channelFuture -> {
						if (channelFuture.isSuccess()) {
							submitFuture.complete(Acknowledge.get());
						} else {
							submitFuture.completeExceptionally(channelFuture.cause());
						}
					});
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException ignored) { }
		}
		return submitFuture;
	}

	public void close() {
		for (NettySocketClient nettySocketClient : clientList) {
			try {
				nettySocketClient.closeAsync().get();
			} catch (Exception e) {
				LOG.error("Close the connection to {}:{} failed", socketAddress.getAddress(), socketAddress.getPort(), e);
			}
		}
	}
}
