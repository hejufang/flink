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

package org.apache.flink.connectors.rpc.thrift;

import org.apache.flink.connectors.rpc.RPCOptions;
import org.apache.flink.connectors.rpc.discovery.RPCDiscovery;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Keep all RPC Client logic in {@link ThriftRPCClient}, which can be easily changed.
 */
public class ThriftRPCClient implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ThriftRPCClient.class);

	private final String consul;
	private final String cluster;
	private final String testHostPort;
	private final long consulRefreshIntervalMs;
	private final int connectTimeoutMs;
	private final int connectionPoolSize;
	private long lastConsulRefreshTimestamp = 0;
	private Class<?> thriftClientClass;
	private final String thriftMethod;
	private final TransportType transportType;
	/* request body is as below:

	 namespace java org.apache.flink.connectors.rpc.thriftexample
		struct GetDimInfosBatchRequest{
		  1: list<GetDimInfosRequest> requests
		}
		service BatchDimMiddlewareService {
		  GetDimInfosBatchResponse GetDimInfosBatch(1: GetDimInfosBatchRequest request);
		}

		requestClass is org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchRequest
		responseClass is org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchResponse
		requestBatchListFieldName is requests
	*/
	private Class<?> requestClass;
	private String requestBatchListFieldName;
	private Class<?> responseClass;

	private final String batchConstantValue;

	private List<RPCDiscovery.HostPort> hostPorts;
	private int currentHostPortIndex = 0;

	private GenericKeyedObjectPool<RPCDiscovery.HostPort, ThriftClientFactory.ThriftClient> clientPool;

	public ThriftRPCClient(RPCOptions options, String requestBatchListFieldName) {
		this.consul = options.getConsul();
		this.cluster = options.getCluster();
		this.testHostPort = options.getTestHostPort();
		this.consulRefreshIntervalMs = options.getConsulIntervalSeconds() * 1000;
		this.connectTimeoutMs = options.getConnectTimeoutMs();
		this.connectionPoolSize = options.getConnectionPoolSize();
		this.thriftClientClass = ThriftUtil.getThriftClientClass(options.getThriftServiceClass());
		this.thriftMethod = options.getThriftMethod();
		this.transportType = options.getTransportType();
		this.batchConstantValue = options.getBatchConstantValue();
		this.requestClass = ThriftUtil.getParameterClassOfMethod(
			this.thriftClientClass.getName(), options.getThriftMethod());
		this.responseClass = ThriftUtil.getReturnClassOfMethod(
			this.thriftClientClass.getName(), options.getThriftMethod());
		this.requestBatchListFieldName = requestBatchListFieldName;
	}

	public void open() {
		if (this.testHostPort != null) {
			String host = this.testHostPort.split(":")[0];
			int port = Integer.parseInt(this.testHostPort.split(":")[1]);
			this.hostPorts = Collections.singletonList(new RPCDiscovery.HostPort(host, port));
		} else {
			discoverHostPort();
		}
		ThriftClientFactory clientFactory = new ThriftClientFactory(
			connectTimeoutMs, this.thriftClientClass.getName(), transportType);
		GenericKeyedObjectPoolConfig<ThriftClientFactory.ThriftClient> config = new GenericKeyedObjectPoolConfig<>();
		config.setMaxTotalPerKey(1);
		config.setMaxTotal(connectionPoolSize);
		clientPool = new GenericKeyedObjectPool<>(clientFactory, config);
	}

	public void discoverHostPort() {
		hostPorts = RPCDiscovery.getHostPorts(consul, cluster, connectionPoolSize);
		Preconditions.checkArgument(hostPorts.size() > 0,
			"consul cannot get any host:port for " + consul);
	}

	public void close() {
		if (clientPool != null) {
			clientPool.clear();
		}
	}

	public Object constructBatchLookupThriftRequestObj(List<Object> requestObjects) throws Exception {
		return ThriftUtil.constructInstanceWithInnerList(requestClass, requestBatchListFieldName, requestObjects);
	}

	/**
	 *
	 * @return Return the list of the response object. The value is extracted from the inner list of thrift response object.
	 *         Each response object's class name is connector.response-batch-class
	 */
	public Object sendLookupBatchRequest(Object requestObject) {
		if (this.responseClass == null) {
			throw new IllegalStateException("Lookup batch request is disabled because current " +
				"thrift client does not support it");
		}
		final RPCDiscovery.HostPort hostPort = refreshAndGetOneServer();
		ThriftClientFactory.ThriftClient thriftClient = null;
		try {
			thriftClient = clientPool.borrowObject(hostPort);
			final TServiceClient serviceClient = thriftClient.getClient();
			return thriftClientClass.getMethod(thriftMethod, requestClass).invoke(serviceClient, requestObject);
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		} finally {
			if (thriftClient != null) {
				clientPool.returnObject(hostPort, thriftClient);
			}
		}
	}

	public Object sendRequest(List<Object> request) {
		final RPCDiscovery.HostPort hostPort = refreshAndGetOneServer();
		ThriftClientFactory.ThriftClient thriftClient = null;
		try {
			thriftClient = clientPool.borrowObject(hostPort);
			final TServiceClient serviceClient = thriftClient.getClient();
			if (request.size() > 1) {
				return sendBatchRequest(request, serviceClient);
			} else {
				return sendSingleRequest(request, serviceClient);
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException(e);
		} finally {
			if (thriftClient != null) {
				clientPool.returnObject(hostPort, thriftClient);
			}
		}
	}

	private Object sendSingleRequest(List<Object> request, TServiceClient serviceClient) {
		try {
			return thriftClientClass.getMethod(thriftMethod, requestClass).invoke(serviceClient, request.get(0));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to send a RPC Request.", e);
		}
	}

	private Object sendBatchRequest(List<Object> requestListValue, TServiceClient serviceClient) {
		try {
			Object requestValue = ThriftUtil.constructInstanceWithString(requestClass, batchConstantValue,
				requestBatchListFieldName, requestListValue);
			return thriftClientClass.getMethod(thriftMethod, requestClass).invoke(serviceClient, requestValue);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to send a batch RPC Request.", e);
		}
	}

	private RPCDiscovery.HostPort refreshAndGetOneServer() {
		if (this.testHostPort == null) {
			doRefreshServerList();
		}
		final RPCDiscovery.HostPort hostPort = hostPorts.get(currentHostPortIndex);
		currentHostPortIndex = currentHostPortIndex == hostPorts.size() - 1 ? 0 : currentHostPortIndex + 1;
		return hostPort;
	}

	private void doRefreshServerList() {
		long currentTimestamp = System.currentTimeMillis();
		if (currentTimestamp - lastConsulRefreshTimestamp < consulRefreshIntervalMs) {
			return;
		}
		lastConsulRefreshTimestamp = currentTimestamp;
		List<RPCDiscovery.HostPort> discovered = RPCDiscovery.getHostPorts(consul, cluster, connectionPoolSize);
		if (discovered.size() == 0) {
			LOG.warn("Discovery for ThriftRPC host:port for {} returns nothing.", consul);
			return;
		}

		hostPorts = discovered;
		currentHostPortIndex = 0;
	}
}
