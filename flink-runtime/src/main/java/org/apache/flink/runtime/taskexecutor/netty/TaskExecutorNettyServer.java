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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.socket.NettySocketServer;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * TM supports to create a netty server to which JM can send job task deployment information.
 */
public class TaskExecutorNettyServer implements Closeable {
	private final NettySocketServer nettySocketServer;

	public TaskExecutorNettyServer(
		Supplier<TaskExecutorGateway> gatewaySupplier,
		String address,
		Configuration configuration) {
		this(gatewaySupplier, address, configuration, false);
	}

	public TaskExecutorNettyServer(
			Supplier<TaskExecutorGateway> gatewaySupplier,
			String address,
			Configuration configuration,
			boolean jobDeploymentEnabled) {
		Consumer<ChannelPipeline> channelPipelineConsumer;
		if (jobDeploymentEnabled) {
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new JobDeploymentEncoder(),
				new JobDeploymentDecoder(),
				new TaskExecutorServerHandler(gatewaySupplier.get()));
		} else {
			channelPipelineConsumer = channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new TaskExecutorServerHandler(gatewaySupplier.get()));
		}

		this.nettySocketServer = new NettySocketServer(
			"taskexecutor",
			address,
			"0",
			channelPipelineConsumer,
			configuration.get(TaskManagerOptions.NETTY_SERVER_CONNECT_BACKLOG),
			configuration.get(TaskManagerOptions.NETTY_SERVER_WORKER_THREAD_COUNT));
	}

	public void start() throws Exception {
		nettySocketServer.start();
	}

	public String getAddress() {
		return nettySocketServer.getAddress();
	}

	public int getPort() {
		return nettySocketServer.getPort();
	}

	@Override
	public void close() throws IOException {
		try {
			nettySocketServer.closeAsync().get();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
