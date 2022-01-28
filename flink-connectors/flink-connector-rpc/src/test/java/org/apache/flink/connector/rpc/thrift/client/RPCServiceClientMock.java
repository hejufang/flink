/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.rpc.thrift.client;

import org.apache.flink.connector.rpc.table.descriptors.RPCOptions;

import com.bytedance.arch.transport.ServiceClient;
import org.apache.thrift.TServiceClient;
import org.mockito.ArgumentCaptor;

import java.io.Serializable;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Mock of a service client.
 * As it's hard to generate different responses for different tests.
 * The mock will return the incoming request as the corresponding response, therefore request and response of the
 * method defined in the service should be of the same struct.
 * For example:
 * service TestService {
 *     TestStruct testFunc(1: TestStruct req)
 * }
 */
public class RPCServiceClientMock implements Serializable, RPCServiceClientBase {
	private static final long serialVersionUID = 1L;
	private static RPCServiceClientMock instance;
	private final ServiceClient<?> socketPool;
	private final Class<?> requestClass;

	private RPCServiceClientMock(Class<?> requestClass) {
		this.requestClass = requestClass;
		this.socketPool = mock(ServiceClient.class);
	}

	public static synchronized RPCServiceClientMock getInstance(
			RPCOptions options,
			Class<? extends TServiceClient> clientClass,
			Class<?> requestClass) {
		if (instance == null) {
			instance = new RPCServiceClientMock(requestClass);
		}
		return instance;
	}

	private synchronized void refresh() {
		instance = new RPCServiceClientMock(requestClass);
	}

	@Override
	public void open() {
	}

	@Override
	public Object sendRequest(Object request) throws Exception {
		socketPool.call(request, null);
		return request;
	}

	@Override
	public void close() {
	}

	public List<?> getRequests(int mockTimes) throws Exception {
		refresh();
		ArgumentCaptor<?> argument = ArgumentCaptor.forClass(requestClass);
		verify(socketPool, times(mockTimes)).call(argument.capture(), any());
		return argument.getAllValues();
	}
}
