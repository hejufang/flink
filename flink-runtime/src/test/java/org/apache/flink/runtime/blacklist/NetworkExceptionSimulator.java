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

import org.apache.flink.runtime.blacklist.tracker.BlacklistTracker;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A simulator to simulate network exception happened from some task managers to some hosts.
 */
public class NetworkExceptionSimulator {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkExceptionSimulator.class);

	private static final int DEFAULT_NUM_HOST = 10;

	private final Map<String, TaskManagers> hostToTaskManagersMap;
	private final Random random;

	private final BlacklistTracker blacklistTracker;
	private final Clock clock;

	NetworkExceptionSimulator(BlacklistTracker blacklistTracker, Clock clock) {
		this.hostToTaskManagersMap = new HashMap<>();
		this.blacklistTracker = blacklistTracker;
		this.clock = clock;
		this.random = new Random(clock.absoluteTimeMillis());
	}

	public void start(final int numHosts) {
		int tmIndex = 1;
		hostToTaskManagersMap.clear();
		for (int i = 0; i < numHosts; i++) {
			String key = "host" + i;
			hostToTaskManagersMap.computeIfAbsent(key, TaskManagers::new);
			hostToTaskManagersMap.get(key).addTaskManager("resource" + tmIndex);
			tmIndex++;
		}
	}

	public void start() {
		start(DEFAULT_NUM_HOST);
	}

	public String triggerNetworkExceptionInOneHost() {
		String chosenHost = chosenHosts(1)[0];
		for (TaskManagers taskManagers : hostToTaskManagersMap.values()) {
			Throwable exception = new LocalTransportException("connection corrupt",
					new InetSocketAddress(taskManagers.host, 10000),
					new InetSocketAddress(chosenHost, 10000),
					new IOException("connection corrupt"));
			for (String taskManagerID : taskManagers.taskManagerList) {
				LOG.error("exception happened: {}: {}", exception.getClass(), exception.getMessage());
				blacklistTracker.onFailure(BlacklistUtil.FailureType.NETWORK, taskManagers.host, new ResourceID(taskManagerID), exception, clock.absoluteTimeMillis());
			}
		}
		return chosenHost;
	}

	public String[] triggerNetworkExceptionInMultipleHost(int numHosts) {
		String[] chosenHosts = chosenHosts(numHosts);
		triggerNetworkExceptionInHost(chosenHosts);
		return chosenHosts;
	}

	public void triggerNetworkExceptionInHost(String... chosenHosts) {
		for (TaskManagers taskManagers : hostToTaskManagersMap.values()) {
			for (String taskManagerID : taskManagers.taskManagerList) {
				String randomProposedBadHost = chosenHosts[random.nextInt(chosenHosts.length)];
				Throwable exception = new LocalTransportException("connection corrupt",
						new InetSocketAddress(taskManagers.host, 10000),
						new InetSocketAddress(randomProposedBadHost, 10000),
						new IOException("connection corrupt"));
				LOG.error("exception happened: {}: {}", exception.getClass(), exception.getMessage());
				blacklistTracker.onFailure(BlacklistUtil.FailureType.NETWORK, taskManagers.host, new ResourceID(taskManagerID), exception, clock.absoluteTimeMillis());
			}
		}
	}

	private String[] chosenHosts(int n) {
		List<String> hosts = new ArrayList<>(hostToTaskManagersMap.keySet());
		Collections.shuffle(hosts, random);
		String[] choseHosts = hosts.subList(0, n).toArray(new String[0]);
		LOG.info("Chosen hosts: {}", Arrays.toString(choseHosts));
		return choseHosts;
	}

	private static class TaskManagers {
		String host;
		List<String> taskManagerList = new LinkedList<>();

		TaskManagers(String host) {
			this.host = host;
		}

		void addTaskManager(String resourceId) {
			this.taskManagerList.add(resourceId);
		}
	}
}
