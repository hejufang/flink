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
import org.apache.flink.connectors.rpc.util.JsonUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/**
 * Keep all RPC Client logic in {@link ThriftRPCClient}, which can be easily changed.
 */
public class ThriftRPCClient implements Serializable {
	public static final String CLIENT_CLASS_SUFFIX = "$Client";

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ThriftRPCClient.class);

	private final String consul;
	private final String cluster;
	private final long consulRefreshIntervalMs;
	private final int connectTimeoutMs;
	private final int connectionPoolSize;
	private long lastConsulRefreshTimestamp = 0;
	private Class<?> serviceClass;
	private final Class<?> requestClass;
	private final String serviceClassName;
	private final String thriftMethod;
	private final TransportType transportType;
	private final String batchClassName;
	private String batchListFieldName;
	private final String batchConstantValue;
	private final String responseValue;

	private List<RPCDiscovery.HostPort> hostPorts;
	private int currentHostPortIndex = 0;

	private final ObjectMapper objectMapper = new ObjectMapper().disable(JsonParser.Feature.ALLOW_MISSING_VALUES);
	private GenericKeyedObjectPool<RPCDiscovery.HostPort, ThriftClientFactory.ThriftClient> clientPool;

	public ThriftRPCClient(RPCOptions options, Class<?> requestClass) {
		this.consul = options.getConsul();
		this.cluster = options.getCluster();
		this.consulRefreshIntervalMs = options.getConsulIntervalSeconds() * 1000;
		this.connectTimeoutMs = options.getConnectTimeoutMs();
		this.connectionPoolSize = options.getConnectionPoolSize();
		this.serviceClassName = options.getThriftServiceClass();
		this.requestClass = requestClass;
		this.thriftMethod = options.getThriftMethod();
		this.transportType = options.getTransportType();
		this.batchClassName = options.getBatchClass();
		this.batchConstantValue = options.getBatchConstantValue();
		this.responseValue = options.getResponseValue();
	}

	public void open() {
		ThriftClientFactory clientFactory = new ThriftClientFactory(connectTimeoutMs, serviceClassName, transportType);
		GenericKeyedObjectPoolConfig<ThriftClientFactory.ThriftClient> config = new GenericKeyedObjectPoolConfig<>();
		config.setMaxTotalPerKey(1);
		config.setMaxTotal(connectionPoolSize);
		clientPool = new GenericKeyedObjectPool<>(clientFactory, config);
		hostPorts = RPCDiscovery.getHostPorts(consul, cluster, connectionPoolSize);
		Preconditions.checkArgument(hostPorts.size() > 0,
			"consul cannot get any host:port for " + consul);
		try {
			serviceClass = Class.forName(serviceClassName + CLIENT_CLASS_SUFFIX);
			batchListFieldName = batchClassName == null ? null :
				ThriftUtil.getFieldNameOfRequestList(requestClass, Class.forName(batchClassName));
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	public void close() {
		if (clientPool != null) {
			clientPool.clear();
		}
	}

	public Object sendRequest(List<Object> request) {
		final RPCDiscovery.HostPort hostPort = refreshAndGetOneServer();
		ThriftClientFactory.ThriftClient thriftClient = null;
		try {
			thriftClient = clientPool.borrowObject(hostPort);
			final TServiceClient serviceClient = thriftClient.getClient();
			if (request.size() > 1) {
				Preconditions.checkArgument(batchClassName.length() > 0,
					"In batch scenario, connector.batch-class must set.");
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
			return serviceClass.getMethod(thriftMethod, requestClass).invoke(serviceClient, request.get(0));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to send a RPC Request.", e);
		}
	}

	private Object sendBatchRequest(List<Object> requestListValue, TServiceClient serviceClient) {
		try {
			Object requestValue = ThriftUtil.constructInstanceWithString(requestClass, batchConstantValue,
				batchListFieldName, requestListValue);
			return serviceClass.getMethod(thriftMethod, requestClass).invoke(serviceClient, requestValue);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to send a batch RPC Request.", e);
		}
	}

	/**
	 * This method is used for judge whether the rpc sink is success.
	 * It will map the response object to json string. If user provide expected response value
	 * which is also json format, it will judge whether method response contains all user provide
	 * json field value. If user doesn't provide response value, we suppose sink always success.
	 * @param response the rpc method response object.
	 */
	public void checkResponse(Object response) {
		String responseJson;
		try {
			responseJson = objectMapper.writeValueAsString(response);
		} catch (JsonProcessingException e) {
			throw new FlinkRuntimeException(String.format("Mapping response object : %s to json string failed.",
				response), e);
		}
		if (responseValue != null && !JsonUtil.isSecondJsonCoversFirstJson(responseValue, responseJson)) {
			throw new FlinkRuntimeException(
				String.format("Send request failed. The response value is : %s.", responseJson));
		}
	}

	private RPCDiscovery.HostPort refreshAndGetOneServer() {
		doRefreshServerList();
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
