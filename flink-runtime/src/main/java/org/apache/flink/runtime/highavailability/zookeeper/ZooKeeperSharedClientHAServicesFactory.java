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

package org.apache.flink.runtime.highavailability.zookeeper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.SharedClientHAServices;
import org.apache.flink.runtime.highavailability.SharedClientHAServicesFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Factory for creating shared client high availability services.
 */
public class ZooKeeperSharedClientHAServicesFactory implements SharedClientHAServicesFactory {

	private static final Map<String, SharedClientHAServices> sharedClientHAServicesMap = new ConcurrentHashMap<>();

	public SharedClientHAServices createSharedClientHAServices(Configuration configuration) throws Exception {

		String clusterId = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		SharedClientHAServices services;
		synchronized (ZooKeeperSharedClientHAServicesFactory.class) {
			if (sharedClientHAServicesMap.containsKey(clusterId)) {
				return sharedClientHAServicesMap.get(clusterId);
			}

			CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
			services = new ZooKeeperSharedClientHAServices(client, configuration);
			sharedClientHAServicesMap.put(clusterId, services);
		}

		return services;
	}

	@Override
	public HighAvailabilityServices createHAServices(Configuration configuration, Executor executor) throws Exception {
		throw new UnsupportedOperationException();
	}
}
