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

package org.apache.flink.runtime.blacklisttracker;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceActions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Blacklist tracker for session.
 * Always run in mainThread.
 */
public class SessionBlacklistTracker implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(SessionBlacklistTracker.class);
	/**
	 * Host already added to blacklist. key: host, value: latest failure timestamp.
	 */
	private Map<String, Long> blackedHosts;
	/**
	 * All host with failed TaskManagers.
	 */
	private Map<String, LinkedList<TaskManagerFailure>> hostFailures;

	private ComponentMainThreadExecutor mainThreadExecutor;
	private ResourceActions resourceActions;
	private final BlacklistConfiguration blacklistConfiguration;

	public SessionBlacklistTracker(BlacklistConfiguration blacklistConfiguration) {
		this.blackedHosts = new HashMap<>();
		this.hostFailures = new HashMap<>();
		this.blacklistConfiguration = blacklistConfiguration;
	}

	public void start(ComponentMainThreadExecutor mainThreadExecutor, ResourceActions newResourceActions) {
		this.resourceActions = newResourceActions;
		this.mainThreadExecutor = mainThreadExecutor;
		this.mainThreadExecutor.schedule(
				this::checkFailureExceedTimeout,
				blacklistConfiguration.getCheckInterval().toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing the SessionBlacklistTracker.");
		clearAll();
	}

	public Map<String, LinkedList<TaskManagerFailure>> getHostFailures() {
		return hostFailures;
	}

	public Set<String> getBlackedHosts() {
		return blackedHosts.keySet();
	}

	public void clearAll() {
		this.hostFailures.clear();
		this.blackedHosts.clear();
		resourceActions.notifyBlacklistUpdated();
	}

	private void removeEarliestHost() {
		if (blackedHosts.size() > 0) {
			String earliestHost = Collections.min(blackedHosts.entrySet(), Map.Entry.comparingByValue()).getKey();
			hostFailures.remove(earliestHost);
			blackedHosts.remove(earliestHost);
			LOG.info("Remove the earliest host {}", earliestHost);
		}
	}

	public void taskManagerFailure(TaskManagerLocation taskManagerLocation, String cause, Long timestamp) {
		ResourceID resourceID = taskManagerLocation.getResourceID();
		String hostname = taskManagerLocation.getFQDNHostname();
		if (hostFailures.containsKey(hostname)) {
			LinkedList<TaskManagerFailure> taskManagerFailures = hostFailures.get(hostname);
			// TODO: filter Exception
			taskManagerFailures.add(new TaskManagerFailure(taskManagerLocation, cause, timestamp));
			LOG.info("Add taskManagerFailures, TaskManagerLocation: {}, cause: {}, timestamp: {}.",
					taskManagerLocation, cause, timestamp);

			boolean updated = false;
			if (taskManagerFailures.size() > blacklistConfiguration.getSessionMaxTaskmanagerFailurePerHost()) {
				if (!blackedHosts.containsKey(hostname)) {
					LOG.info("number of failureTaskManager ({}) on {} exceeds the maximum {}, add the host to blacklist.",
							taskManagerFailures.size(), hostname, blacklistConfiguration.getSessionMaxTaskmanagerFailurePerHost());
					updated = true;
				}
				blackedHosts.put(hostname, timestamp);

				// remove earliest blacked host.
				if (blackedHosts.size() > blacklistConfiguration.getSessionBlacklistLength()) {
					LOG.info("Number of host in sessionBlacklist exceeds the maximum {}, remove earliest host.",
							blacklistConfiguration.getSessionBlacklistLength());
					removeEarliestHost();
					updated = true;
				}

				// remove earliest exception.
				if (taskManagerFailures.size() > blacklistConfiguration.getSessionMaxTaskmanagerFailurePerHost() + 1) {
					TaskManagerFailure taskManagerFailure = taskManagerFailures.removeFirst();
					if (taskManagerFailure != null) {
						LOG.info("Blacklist Exceptions for host {} too long, remove {}",
								hostname, taskManagerFailure);
					}
				}
				if (updated) {
					resourceActions.notifyBlacklistUpdated();
				}
			}
		} else {
			LinkedList<TaskManagerFailure> taskManagerFailures = new LinkedList<>();
			taskManagerFailures.add(new TaskManagerFailure(taskManagerLocation, cause, timestamp));
			hostFailures.put(hostname, taskManagerFailures);
			LOG.info("Add host {} TaskManager {} to hostToFailureTaskManager.", hostname, resourceID);
		}
	}

	public void checkFailureExceedTimeout() {
		long currentTimestampMs = System.currentTimeMillis();
		LOG.debug("Start to check failures exceeds timeout, current timestamp: {}.", currentTimestampMs);
		AtomicBoolean updated = new AtomicBoolean(false);

		// remove host which failures is empty.
		hostFailures.entrySet().removeIf(stringListEntry -> {
			String host = stringListEntry.getKey();
			List<TaskManagerFailure> taskManagerFailures = stringListEntry.getValue();
			// remove failures which out of date.
			taskManagerFailures.removeIf(
					value -> (currentTimestampMs - value.getTimestamp()) > blacklistConfiguration.getFailureTimeout().toMilliseconds());

			if (blackedHosts.containsKey(host) && taskManagerFailures.size() <= blacklistConfiguration.getSessionMaxTaskmanagerFailurePerHost()) {
				LOG.info("Number of failure TaskManager on host {} small than threshold {}, remove from blacklist.",
						host, blacklistConfiguration.getSessionMaxTaskmanagerFailurePerHost());
				blackedHosts.remove(host);
				updated.set(true);
			}
			return taskManagerFailures.isEmpty();
		});

		if (updated.get()) {
			resourceActions.notifyBlacklistUpdated();
		}

		this.mainThreadExecutor.schedule(
				this::checkFailureExceedTimeout,
				blacklistConfiguration.getCheckInterval().toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}

	public static SessionBlacklistTracker fromConfiguration(BlacklistConfiguration blacklistConfiguration) {
		if (!blacklistConfiguration.isBlacklistEnabled()) {
			LOG.info("Blacklist not enabled.");
			return null;
		} else {
			LOG.info("create new SessionBlacklist with config: {}", blacklistConfiguration);
			return new SessionBlacklistTracker(blacklistConfiguration);
		}
	}
}
