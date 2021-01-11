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

import org.apache.flink.connectors.rpc.discovery.RPCDiscovery;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;

/**
 * A thrift ClientFactory for managing ThriftClient's life cycle.
 */
public class ThriftClientFactory
		implements KeyedPooledObjectFactory<RPCDiscovery.HostPort, ThriftClientFactory.ThriftClient> {

	private final int timeout;
	private final String thriftClientClass;
	private final TransportType transportType;

	public ThriftClientFactory(
			int timeout,
			String thriftClientClass,
			TransportType transportType) {
		this.timeout = timeout;
		this.thriftClientClass = thriftClientClass;
		this.transportType = transportType;
	}

	@Override
	public PooledObject<ThriftClient> makeObject(RPCDiscovery.HostPort key) throws Exception {
		TSocket socket = new TSocket(key.getHost(), key.getPort(), timeout);
		TTransport transport;
		switch (transportType) {
			case Buffered:
				transport = socket;
				break;
			case Framed:
				transport = new TFramedTransport(socket);
				break;
			default:
				throw new IllegalArgumentException("Not support transport type " + transportType);
		}
		TProtocol protocol = new TBinaryProtocol(transport);
		Class<?> c = Class.forName(thriftClientClass);
		TServiceClient serviceClient;
		try {
			Constructor<?> constructor = c.getDeclaredConstructor(TProtocol.class);
			constructor.setAccessible(true);
			serviceClient = (TServiceClient) constructor.newInstance(protocol);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Init the rpc client failed.", e);
		}
		return new DefaultPooledObject<>(new ThriftClient(transport, serviceClient));
	}

	@Override
	public void destroyObject(RPCDiscovery.HostPort key, PooledObject<ThriftClient> p) {
		TTransport transport = p.getObject().getTransport();
		if (transport.isOpen()) {
			transport.close();
		}
	}

	@Override
	public boolean validateObject(RPCDiscovery.HostPort key, PooledObject<ThriftClient> p) {
		TTransport transport = p.getObject().getTransport();
		return transport.isOpen();
	}

	@Override
	public void activateObject(RPCDiscovery.HostPort key, PooledObject<ThriftClient> p) throws Exception {
		TTransport transport = p.getObject().getTransport();
		if (!transport.isOpen()) {
			transport.open();
		}
	}

	@Override
	public void passivateObject(RPCDiscovery.HostPort key, PooledObject<ThriftClient> p) {
		TTransport transport = p.getObject().getTransport();
		if (transport.isOpen()) {
			transport.close();
		}
	}

	/**
	 * The ThriftClient wraps the TTransport and RPC Client.
	 */
	public static class ThriftClient {

		private final TTransport transport;
		private final TServiceClient client;

		public ThriftClient(TTransport transport, TServiceClient client) {
			this.transport = transport;
			this.client = client;
		}

		public TTransport getTransport() {
			return transport;
		}

		public TServiceClient getClient() {
			return client;
		}
	}
}
