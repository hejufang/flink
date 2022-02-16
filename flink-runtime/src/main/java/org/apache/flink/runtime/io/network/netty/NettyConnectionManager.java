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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Connection manager manages netty channels.
 */
public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;

	private final NettyBufferPool bufferPool;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	private final NettyProtocol nettyProtocol;

	private final boolean channelReuseEnable;

	public NettyConnectionManager(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		NettyConfig nettyConfig) {
		this(partitionProvider, taskEventPublisher, nettyConfig, false, 0L);
	}

	public NettyConnectionManager(
			ResultPartitionProvider partitionProvider,
			TaskEventPublisher taskEventPublisher,
			NettyConfig nettyConfig,
			boolean channelReuseEnable,
			long channelIdleReleaseTimeMs) {

		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(
			client,
			channelReuseEnable,
			channelIdleReleaseTimeMs);

		this.nettyProtocol = new NettyProtocol(checkNotNull(partitionProvider), checkNotNull(taskEventPublisher), channelReuseEnable, nettyConfig.isNotifyPartitionRequestEnable());
		this.channelReuseEnable = channelReuseEnable;
	}

	public NettyConnectionManager(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		NettyConfig nettyConfig,
		MetricGroup metricGroup) {
		this(partitionProvider, taskEventPublisher, nettyConfig, metricGroup, false, 0L);
	}

	public NettyConnectionManager(
			ResultPartitionProvider partitionProvider,
			TaskEventPublisher taskEventPublisher,
			NettyConfig nettyConfig,
			MetricGroup metricGroup,
			boolean channelReuseEnable,
			long channelIdleReleaseTimeMs) {

		this(partitionProvider, taskEventPublisher, nettyConfig, channelReuseEnable, channelIdleReleaseTimeMs);
		this.client.registerConnectRetryTimesMetrics(metricGroup);
	}

	@Override
	public int start() throws IOException {
		client.init(nettyProtocol, bufferPool);

		return server.init(nettyProtocol, bufferPool);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public void shutdown() {
		if (channelReuseEnable) {
			partitionRequestClientFactory.shutdownChannelManager();
		}
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
