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

package org.apache.flink.client.program.socket;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.client.program.rest.retry.ExponentialWaitStrategy;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.highavailability.ClientHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.socket.SocketClient;
import org.apache.flink.runtime.socket.SocketRestLeaderAddress;
import org.apache.flink.util.CloseableIterator;

/**
 * A {@link ClusterClient} implementation that support to submit job via socket and communicates via HTTP REST requests.
 */
public class SocketRestClusterClient<T> extends RestClusterClient<T> {
	private final long connectTimeoutMills;
	private final int queueSize;
	private SocketClient socketClient;

	public SocketRestClusterClient(Configuration config, T clusterId) throws Exception {
		this(
			config,
			clusterId,
			HighAvailabilityServicesUtils.createClientHAService(config));
	}

	public SocketRestClusterClient(
			Configuration config,
			T clusterId,
			ClientHighAvailabilityServices clientHAServices) throws Exception {
		super(
			config,
			null,
			clusterId,
			new ExponentialWaitStrategy(config.getLong(RestOptions.CLUSTER_CLIENT_RETRY_INIT_DELAY),
				config.getLong(RestOptions.CLUSTER_CLIENT_RETRY_MAX_DELAY)),
			clientHAServices);

		this.connectTimeoutMills = config.getLong(RestOptions.CONNECTION_TIMEOUT);
		this.queueSize = config.getInteger(ClusterOptions.CLUSTER_CLIENT_RESULT_QUEUE_MAX_SIZE);
		refreshSocketClient();
	}

	@Override
	protected String parseRestUrl(String leaderAddress) throws Exception {
		SocketRestLeaderAddress socketRestLeaderAddress = SocketRestLeaderAddress.fromJson(leaderAddress);
		return socketRestLeaderAddress.getBaseRestUrl();
	}

	@Override
	public <R> CloseableIterator<R> submitJobSync(JobGraph jobGraph) {
		if (socketClient == null) {
			refreshSocketClient();
		}
		return socketClient.submitJob(jobGraph);
	}

	private void refreshSocketClient() {
		if (socketClient != null) {
			socketClient.closeAsync();
			socketClient = null;
		}
		try {
			SocketRestLeaderAddress socketRestLeaderAddress = SocketRestLeaderAddress.fromJson(super.retrieveLeaderAddress().get().f0);
			this.socketClient = new SocketClient(
				socketRestLeaderAddress.getSocketAddress(),
				socketRestLeaderAddress.getSocketPort(),
				Math.toIntExact(connectTimeoutMills),
				queueSize);
			this.socketClient.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		super.close();
		if (socketClient != null) {
			socketClient.closeAsync();
			socketClient = null;
		}
	}
}
