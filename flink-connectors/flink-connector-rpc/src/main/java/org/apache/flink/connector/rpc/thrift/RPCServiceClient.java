/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rpc.thrift;

import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;

import com.bytedance.arch.transport.ClientOptions;
import com.bytedance.arch.transport.RpcFunction;
import com.bytedance.arch.transport.ServiceClient;
import com.bytedance.arch.transport.ServiceMeta;
import com.bytedance.arch.transport.SocketPoolOptions;
import com.bytedance.arch.transport.loadbalance.LoadBalancerType;
import org.apache.thrift.TServiceClient;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/**
 * A client connect with RPC service.
 */
public class RPCServiceClient implements Serializable {
	private static final long serialVersionUID = 1L;
	private final Class<? extends TServiceClient> clientClass;
	private final Class<?> requestClass;
	private final RPCOptions options;
	private transient ClientOptions clientOptions;
	private transient ServiceClient<?> socketPool;

	public RPCServiceClient(
			RPCOptions options,
			Class<? extends TServiceClient> clientClass,
			Class<?> requestClass) {
		this.options = options;
		this.clientClass = clientClass;
		this.requestClass = requestClass;
	}

	public void open() {
		ServiceMeta serviceMeta = new ServiceMeta(
			options.getConsul(),
			options.getCluster(),
			options.getConsulUpdateIntervalMs(),
			options.getConsulUpdateIntervalMs());
		if (options.getConnectionPoolSize() > 0) {
			serviceMeta.setCorePoolSize(options.getConnectionPoolSize());
		}
		this.clientOptions = new ClientOptions();
		if (options.getConnectTimeoutMs() > 0) {
			clientOptions.setConnectTimeout(options.getConnectTimeoutMs());
		}
		if (options.getSocketTimeoutMs() > 0) {
			clientOptions.setSocketTimeout(options.getSocketTimeoutMs());
		}
		this.socketPool = new ServiceClient<>(
			serviceMeta,
			options.getPsm(),
			options.getTransportType(),
			clientClass,
			LoadBalancerType.ConsistentHashV3,
			new SocketPoolOptions());
	}

	@SuppressWarnings("unchecked")
	public Object sendRequest(Object request) {
		return socketPool.call(
			request,
			(RpcFunction) (client, request1) -> {
				try {
					return clientClass.getMethod(options.getThriftMethod(), requestClass).invoke(client, request1);
				} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException("Cannot send a request to the service.", e);
				}},
			clientOptions);
	}

	public void close() {
		if (socketPool != null) {
			socketPool.close();
		}
	}

}
