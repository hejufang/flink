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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.NetworkAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Testing BlacklistActions.
 */
public class TestingBlacklistActions implements BlacklistActions{

	private Map<NetworkAddress, ResourceID> historyNetworkAddressToTaskManagerID;
	private Set<ResourceID> aliveTaskManagers;

	Runnable notifyBlacklistUpdatedConsumer;
	Supplier<Integer> registeredWorkerNumberSupplier;

	public TestingBlacklistActions(Runnable notifyBlacklistUpdatedConsumer, Supplier<Integer> registeredWorkerNumberSupplier) {
		this.notifyBlacklistUpdatedConsumer = notifyBlacklistUpdatedConsumer;
		this.registeredWorkerNumberSupplier = registeredWorkerNumberSupplier;
		historyNetworkAddressToTaskManagerID = new HashMap<>();
		aliveTaskManagers = new HashSet<>();
	}

	@Override
	public void notifyBlacklistUpdated() {
		this.notifyBlacklistUpdatedConsumer.run();
	}

	@Override
	public int getRegisteredWorkerNumber() {
		return registeredWorkerNumberSupplier.get();
	}

	@Override
	public int queryNumberOfHosts() {
		return 0;
	}

	@Override
	public ResourceID queryTaskManagerID(NetworkAddress networkAddress) {
		return historyNetworkAddressToTaskManagerID.get(networkAddress);
	}

	@Override
	public String queryNodeName(NetworkAddress networkAddress) {
		return networkAddress.getHostName();
	}

	@Override
	public boolean isTaskManagerOffline(ResourceID taskManagerID) {
		return !aliveTaskManagers.contains(taskManagerID);
	}

	public void addNewTaskManagerWith(ResourceID taskManagerID, String hostName, int dataPort) {
		this.aliveTaskManagers.add(taskManagerID);
		recordHistoryTaskManagerWith(taskManagerID, hostName, dataPort);
	}

	public void recordHistoryTaskManagerWith(ResourceID taskManagerID, String hostName, int dataPort) {
		this.historyNetworkAddressToTaskManagerID.put(new NetworkAddress(hostName, dataPort), taskManagerID);
	}

	/**
	 * Builder for {@link TestingBlacklistActions}.
	 */
	public static class TestingBlacklistActionsBuilder {
		Runnable notifyBlacklistUpdatedConsumer = () -> {};
		Supplier<Integer> registeredWorkerNumberSupplier = () -> 0;

		public TestingBlacklistActionsBuilder setNotifyBlacklistUpdatedConsumer(Runnable notifyBlacklistUpdatedConsumer) {
			this.notifyBlacklistUpdatedConsumer = notifyBlacklistUpdatedConsumer;
			return this;
		}

		public TestingBlacklistActionsBuilder setRegisteredWorkerNumberSupplier(Supplier<Integer> registeredWorkerNumberSupplier) {
			this.registeredWorkerNumberSupplier = registeredWorkerNumberSupplier;
			return this;
		}

		public TestingBlacklistActions build() {
			return new TestingBlacklistActions(notifyBlacklistUpdatedConsumer, registeredWorkerNumberSupplier);
		}
	}
}
