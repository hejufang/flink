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

import org.apache.flink.connectors.loghouse.LogHouseOptions;
import org.apache.flink.connectors.loghouse.discovery.LogHouseDiscovery;
import org.apache.flink.connectors.loghouse.service.LogHouse;
import org.apache.flink.connectors.loghouse.service.PutRequest;
import org.apache.flink.connectors.loghouse.service.PutResponse;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Keep all LogHouse Client logic in {@link LogHouseClient}, which can be easily changed.
 */
public class LogHouseClient {

	private static final Logger LOG = LoggerFactory.getLogger(LogHouseClient.class);

	private final LogHouseOptions options;
	private LogHouseDiscovery discovery;

	private final long consulIntervalMs;
	private long lastConsulTimestamp = 0;

	private List<LogHouseDiscovery.HostPort> hostPorts;
	private int currentIndex = 0;

	private GenericKeyedObjectPool<LogHouseDiscovery.HostPort, ThriftClientFactory.ThriftClient> clientPool;

	public LogHouseClient(LogHouseOptions options) {
		this.options = options;
		this.consulIntervalMs = options.getConsulIntervalSeconds() * 1000;
	}

	public void open() {
		ThriftClientFactory clientFactory = new ThriftClientFactory(options.getConnectTimeoutMs());
		GenericKeyedObjectPoolConfig<ThriftClientFactory.ThriftClient> config = new GenericKeyedObjectPoolConfig<>();
		config.setMaxTotalPerKey(1);
		config.setMaxTotal(options.getConnectionPoolSize());
		clientPool = new GenericKeyedObjectPool<>(clientFactory, config);
		discovery = new LogHouseDiscovery();
		hostPorts = discovery.getHostPorts(options.getConsul(), options.getConnectionPoolSize());
		Preconditions.checkArgument(hostPorts.size() > 0,
			"consul cannot get any host:port for " + options.getConsul());
	}

	public void close() {
		clientPool.clear();
	}

	public void sendPut(PutRequest request) {
		discoverIfNecessary();

		String lastErrorMessage = "";
		for (int i = 1; i <= options.getFlushMaxRetries(); ++i) {
			final LogHouseDiscovery.HostPort hostPort = hostPorts.get(currentIndex);
			currentIndex = currentIndex == hostPorts.size() - 1 ? 0 : currentIndex + 1;
			ThriftClientFactory.ThriftClient thriftClient = null;
			try {
				thriftClient = clientPool.borrowObject(hostPort);
				final LogHouse.Client client = thriftClient.getClient();
				final PutResponse response = client.Put(request); // sync send.

				if (response.getStatus().getErrorCode() == 0) {
					return; // success
				}

				LOG.warn("Sending PutRequest to {} failed with error code: {}, and error message {}. will retry after {} seconds.",
					hostPort,
					response.getStatus().getErrorCode(),
					response.getStatus().getErrorMessage(),
					i);
				lastErrorMessage = response.getStatus().getErrorMessage();
			} catch (Exception e) {
				LOG.warn("Failed to send a PutRequest, will retry after {} seconds.", i, e);
				lastErrorMessage = e.getMessage();
			} finally {
				if (thriftClient != null) {
					clientPool.returnObject(hostPort, thriftClient);
				}
			}

			try {
				Thread.sleep(i * 1000);
			} catch (InterruptedException e) {
				throw new FlinkRuntimeException("Interrupted while sleeping.", e);
			}

			if (thriftClient != null) {
				try {
					clientPool.invalidateObject(hostPort, thriftClient); // close current connection.
				} catch (Throwable t) {
					LOG.warn("invalidate {} error.", hostPort, t);
				}
			}
			discoverNow(); // discover after failure.
		}

		// failed finally.
		throw new FlinkRuntimeException("Sending PutRequest failed " + options.getFlushMaxRetries() + " times. " +
			"Last error messsage is: " + lastErrorMessage);
	}

	private void discoverIfNecessary() {
		long currentTimestamp = System.currentTimeMillis();
		if (currentTimestamp - lastConsulTimestamp < consulIntervalMs) {
			return;
		}
		lastConsulTimestamp = currentTimestamp;

		discoverNow();
	}

	private void discoverNow() {
		List<LogHouseDiscovery.HostPort> discovered = discovery.getHostPorts(options.getConsul(), options.getConnectionPoolSize());
		if (discovered.size() == 0) {
			LOG.warn("Discovery for LogHouse host:port for {} returns nothing.", options.getConsul());
			return;
		}

		hostPorts = discovered;
		currentIndex = ThreadLocalRandom.current().nextInt(0, hostPorts.size());
	}
}
