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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.socket.NettySocketServer;
import org.apache.flink.runtime.socket.SocketRestLeaderAddress;
import org.apache.flink.runtime.socket.handler.PushTaskResultHandler;
import org.apache.flink.runtime.socket.handler.SocketJobSubmitHandler;
import org.apache.flink.runtime.socket.result.JobResultClientManager;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Socket endpoint for the {@link Dispatcher} component.
 */
public class DispatcherSocketRestEndpoint extends DispatcherRestEndpoint {
	private final JobResultClientManager jobResultClientManager;
	private final NettySocketServer nettySocketServer;

	public DispatcherSocketRestEndpoint(
			RestServerEndpointConfiguration endpointConfiguration,
			GatewayRetriever<DispatcherGateway> leaderRetriever,
			JobResultClientManager jobResultClientManager,
			Configuration clusterConfiguration,
			RestHandlerConfiguration restConfiguration,
			GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
			TransientBlobService transientBlobService,
			ScheduledExecutorService executor,
			MetricFetcher metricFetcher,
			LeaderElectionService leaderElectionService,
			ExecutionGraphCache executionGraphCache,
			FatalErrorHandler fatalErrorHandler) throws IOException {
		super(
			endpointConfiguration,
			leaderRetriever,
			clusterConfiguration,
			restConfiguration,
			resourceManagerRetriever,
			transientBlobService,
			executor,
			metricFetcher,
			leaderElectionService,
			executionGraphCache,
			fatalErrorHandler);
		this.jobResultClientManager = jobResultClientManager;
		this.nettySocketServer = new NettySocketServer(
			"dispatcher",
			clusterConfiguration.getString(RestOptions.SOCKET_ADDRESS),
			clusterConfiguration.getString(RestOptions.BIND_SOCKET_PORT),
			channelPipeline -> channelPipeline.addLast(
				new ObjectEncoder(),
				new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)),
				new SocketJobSubmitHandler(
					leaderRetriever,
					jobResultClientManager,
					Time.milliseconds(clusterConfiguration.getLong(WebOptions.TIMEOUT))),
				new PushTaskResultHandler(jobResultClientManager)
			),
			clusterConfiguration.getInteger(RestOptions.DISPATCHER_CONNECT_BACKLOG)
		);
	}

	@Override
	public void startInternal() throws Exception {
		nettySocketServer.start();
		log.info("Socket endpoint listening at {}:{}", nettySocketServer.getAddress(), nettySocketServer.getPort());

		super.startInternal();
	}

	@Override
	public String getLeaderAddress() {
		SocketRestLeaderAddress socketRestLeaderAddress = new SocketRestLeaderAddress(super.getLeaderAddress(), getAddress(), getSocketPort());
		try {
			return socketRestLeaderAddress.toJson();
		} catch (IOException e) {
			log.error("Generate leader address failed", e);
			throw new RuntimeException(e);
		}
	}

	public String getAddress() {
		return nettySocketServer.getAddress();
	}

	public int getSocketPort() {
		return nettySocketServer.getPort();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (jobResultClientManager != null) {
			jobResultClientManager.close();
		}
		CompletableFuture<Void> closeFuture = super.closeAsync();
		return FutureUtils.completeAll(Arrays.asList(closeFuture, nettySocketServer.closeAsync()));
	}
}
