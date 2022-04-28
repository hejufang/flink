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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.JobSocketResult;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ClassResolvers;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.serialization.ObjectEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Task writes job result to netty server in cluster entry process.
 */
public class SocketTaskJobResultGateway implements TaskJobResultGateway {
	private static final Logger LOG = LoggerFactory.getLogger(SocketTaskJobResultGateway.class);

	private final int clientCount;
	private final int connectTimeoutMills;
	private final int lowWriteMark;
	private final int highWriteMark;
	private List<NettySocketClient> clientList;

	public SocketTaskJobResultGateway(int clientCount, int connectTimeoutMills) {
		this(clientCount, connectTimeoutMills, new Configuration());
	}

	public SocketTaskJobResultGateway(int clientCount, int connectTimeoutMills, Configuration configuration) {
		checkArgument(clientCount > 0);
		this.clientCount = clientCount;
		this.connectTimeoutMills = connectTimeoutMills;
		this.lowWriteMark = configuration.getInteger(TaskManagerOptions.RESULT_PUSH_NETTY_LOW_WRITER_BUFFER_MARK);
		this.highWriteMark = configuration.getInteger(TaskManagerOptions.RESULT_PUSH_NETTY_HIGH_WRITER_BUFFER_MARK);
		this.clientList = new ArrayList<>(clientCount);
	}

	@Override
	public void connect(String address, int port) throws Exception {
		List<NettySocketClient> newConnectionList = new ArrayList<>(clientCount);
		for (int i = 0; i < clientCount; i++) {
			NettySocketClient nettySocketClient = new NettySocketClient(
				address,
				port,
				connectTimeoutMills,
				lowWriteMark,
				highWriteMark,
				channelPipeline -> channelPipeline.addLast(
					new ObjectEncoder(),
					new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null))));
			nettySocketClient.start();
			newConnectionList.add(nettySocketClient);
		}
		this.close();
		this.clientList = newConnectionList;
	}

	@Override
	public void sendResult(JobID jobId, @Nullable byte[] data, ResultStatus resultStatus) {
		JobSocketResult jobSocketResult = new JobSocketResult.Builder()
			.setJobId(jobId)
			.setResult(data)
			.setResultStatus(resultStatus)
			.build();
		NettySocketClient nettySocketClient = clientList.get(getJobNettySocketClient(jobId));
		while (true) {
			if (nettySocketClient.getChannel().isWritable()) {
				nettySocketClient.getChannel().write(jobSocketResult);
				break;
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) { }
		}
		if (resultStatus == ResultStatus.COMPLETE) {
			LOG.info("Send complete result for job {}", jobId);
			nettySocketClient.getChannel().flush();
		}
	}

	private int getJobNettySocketClient(JobID jobId) {
		checkArgument(!clientList.isEmpty(),
			"Dispatcher has not registered netty server to task executor");
		return Math.abs(jobId.hashCode()) % clientList.size();
	}

	@Override
	public void close() {
		for (NettySocketClient socketClient : clientList) {
			socketClient.closeAsync();
		}
	}
}
