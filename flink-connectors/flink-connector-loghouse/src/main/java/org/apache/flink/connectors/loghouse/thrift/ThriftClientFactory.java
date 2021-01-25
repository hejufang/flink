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

package org.apache.flink.connectors.loghouse.thrift;

import org.apache.flink.connectors.loghouse.discovery.LogHouseDiscovery;
import org.apache.flink.connectors.loghouse.service.LogHouse;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * A thrift ClientFactory for managing ThriftClient's life cycle.
 */
public class ThriftClientFactory
		implements KeyedPooledObjectFactory<LogHouseDiscovery.HostPort, ThriftClientFactory.ThriftClient> {

	private final int timeout;

	public ThriftClientFactory(int timeout) {
		this.timeout = timeout;
	}

	@Override
	public PooledObject<ThriftClient> makeObject(LogHouseDiscovery.HostPort key) throws Exception {
		TSocket socket = new TSocket(key.getHost(), key.getPort(), timeout);
		TTransport transport = new TFramedTransport(socket);
		TProtocol protocol = new TBinaryProtocol(transport);
		LogHouse.Client client = new LogHouse.Client(protocol);
		return new DefaultPooledObject<>(new ThriftClient(transport, client));
	}

	@Override
	public void destroyObject(LogHouseDiscovery.HostPort key, PooledObject<ThriftClient> p) throws Exception {
		TTransport transport = p.getObject().getTransport();
		if (transport.isOpen()) {
			transport.close();
		}
	}

	@Override
	public boolean validateObject(LogHouseDiscovery.HostPort key, PooledObject<ThriftClient> p) {
		TTransport transport = p.getObject().getTransport();
		return transport.isOpen();
	}

	@Override
	public void activateObject(LogHouseDiscovery.HostPort key, PooledObject<ThriftClient> p) throws Exception {
		TTransport transport = p.getObject().getTransport();
		if (!transport.isOpen()) {
			transport.open();
		}
	}

	@Override
	public void passivateObject(LogHouseDiscovery.HostPort key, PooledObject<ThriftClient> p) throws Exception {
		TTransport transport = p.getObject().getTransport();
		if (transport.isOpen()) {
			transport.close();
		}
	}

	/**
	 * The ThriftClient wraps the TTransport and LogHouse.Client.
	 */
	public static class ThriftClient {

		private final TTransport transport;
		private final LogHouse.Client client;

		public ThriftClient(TTransport transport, LogHouse.Client client) {
			this.transport = transport;
			this.client = client;
		}

		public TTransport getTransport() {
			return transport;
		}

		public LogHouse.Client getClient() {
			return client;
		}
	}
}
