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

package org.apache.flink.runtime.blacklist.tracker;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStoreImpl;
import org.apache.flink.runtime.blacklist.BlacklistActions;
import org.apache.flink.runtime.blacklist.BlacklistConfiguration;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.WarehouseBlacklistFailureMessage;
import org.apache.flink.runtime.blacklist.WarehouseBlacklistRecordMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.util.LoggerHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blacklist tracker for session.
 * Always run in mainThread.
 */
public class BlacklistTrackerImpl implements BlacklistTracker {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistTrackerImpl.class);

	private static final String BLACKLIST_METRIC_NAME = "blackedHost";
	private static final String WAREHOUSE_BLACKLIST_FAILURES = "warehouseBlacklistFailures";
	private static final String WAREHOUSE_BLACKLIST_RECORDS = "warehouseBlacklistRecords";

	private final TagGauge blacklistGauge = new TagGauge.TagGaugeBuilder().build();
	private final MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet =
			new MessageSet<>(MessageType.BLACKLIST);
	private final MessageSet<WarehouseBlacklistRecordMessage> blacklistRecordMessageSet =
			new MessageSet<>(MessageType.BLACKLIST);

	private final ResourceManagerMetricGroup jobManagerMetricGroup;
	/**
	 * Host already added to blacklist. key: host, value: latest failure timestamp.
	 */
	private Map<String, HostFailure> blackedHosts;
	/**
	 * All host with failed TaskManagers.
	 */
	private final Map<BlacklistUtil.FailureType, HostFailures> allFailures;

	private ComponentMainThreadExecutor mainThreadExecutor;
	private BlacklistActions blacklistActions;
	private final Time checkInterval;

	private final Set<Class<? extends Throwable>> ignoreExceptionClasses;

	public BlacklistTrackerImpl(
			BlacklistConfiguration blacklistConfiguration,
			ResourceManagerMetricGroup jobManagerMetricGroup) {
		this(
				blacklistConfiguration.getMaxTaskFailureNumPerHost(),
				blacklistConfiguration.getMaxTaskManagerFailureNumPerHost(),
				blacklistConfiguration.getTaskBlacklistMaxLength(),
				blacklistConfiguration.getTaskManagerBlacklistMaxLength(),
				blacklistConfiguration.getFailureTimeout(),
				blacklistConfiguration.getCheckInterval(),
				jobManagerMetricGroup);
	}

	public BlacklistTrackerImpl(
			int maxTaskFailureNumPerHost,
			int maxTaskManagerFailureNumPerHost,
			int taskBlacklistMaxLength,
			int taskManagerBlacklistMaxLength,
			Time failureTimeout,
			Time checkInterval,
			ResourceManagerMetricGroup jobManagerMetricGroup) {

		this.blackedHosts = new HashMap<>();
		this.checkInterval = checkInterval;
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);

		this.allFailures = new HashMap<>();
		this.allFailures.put(BlacklistUtil.FailureType.TASK_MANAGER,
				new HostFailures(maxTaskManagerFailureNumPerHost, failureTimeout, taskManagerBlacklistMaxLength, blacklistFailureMessageSet));
		this.allFailures.put(BlacklistUtil.FailureType.TASK,
				new HostFailures(maxTaskFailureNumPerHost, failureTimeout, taskBlacklistMaxLength, blacklistFailureMessageSet));

		this.ignoreExceptionClasses = new HashSet<>();

		registerMetrics();
	}

	@Override
	public void start(ComponentMainThreadExecutor mainThreadExecutor, BlacklistActions blacklistActions) {
		this.blacklistActions = blacklistActions;
		this.mainThreadExecutor = mainThreadExecutor;
		this.mainThreadExecutor.schedule(
				this::checkFailureExceedTimeout,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
		tryUpdateBlacklist();
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing the SessionBlacklistTracker.");
		clearAll();
	}

	@Override
	public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) {
		this.ignoreExceptionClasses.add(exceptionClass);
		LOG.info("Add ignore Exception class {}.", exceptionClass.getName());
	}

	@Override
	public Map<String, HostFailure> getBlackedHosts() {
		return blackedHosts;
	}

	@Override
	public Set<ResourceID> getBlackedResources(BlacklistUtil.FailureType failureType, String hostname) {
		HostFailures hostFailures = allFailures.get(failureType);
		if (hostFailures != null) {
			return hostFailures.getResources(hostname);
		} else {
			return Collections.emptySet();
		}
	}

	@Override
	public void clearAll() {
		this.blackedHosts.clear();
		for (HostFailures hostFailures : allFailures.values()) {
			hostFailures.clear();
		}
		if (blacklistActions != null) {
			blacklistActions.notifyBlacklistUpdated();
		}
		LOG.info("All blacklist cleared");
	}

	@Override
	public void onFailure(BlacklistUtil.FailureType failureType, String hostname, ResourceID resourceID, Throwable cause, long timestamp) {
		if (!allFailures.containsKey(failureType)) {
			LOG.warn("UnSupport failure type {}.", failureType);
			return;
		}

		if (this.ignoreExceptionClasses.contains(cause.getClass())) {
			return;
		}

		HostFailure hostFailure = new HostFailure(failureType, hostname, resourceID, cause, timestamp);
		if (allFailures.get(failureType).addFailure(hostFailure)) {
			tryUpdateBlacklist();
		}
	}

	private void registerMetrics() {
		jobManagerMetricGroup.gauge(BLACKLIST_METRIC_NAME, blacklistGauge);
		jobManagerMetricGroup.gauge(WAREHOUSE_BLACKLIST_FAILURES, blacklistFailureMessageSet);
		jobManagerMetricGroup.gauge(WAREHOUSE_BLACKLIST_RECORDS, blacklistRecordMessageSet);
	}

	public void tryUpdateBlacklist() {
		if (blacklistActions == null) {
			LOG.info("Blacklist Tracker is not started, ignore update blacklist.");
			return;
		}
		Map<String, HostFailure> tempBlackedHost = new HashMap<>();
		for (Map.Entry<BlacklistUtil.FailureType, HostFailures> allFailuresEntry : allFailures.entrySet()) {
			for (Map.Entry<String, HostFailure> blackedHost : allFailuresEntry.getValue().getBlackedHosts().entrySet()) {
				if (!tempBlackedHost.containsKey(blackedHost.getKey())) {
					tempBlackedHost.put(blackedHost.getKey(), blackedHost.getValue());
				} else {
					if (blackedHost.getValue().getTimestamp() > tempBlackedHost.get(blackedHost.getKey()).getTimestamp()) {
						tempBlackedHost.put(blackedHost.getKey(), blackedHost.getValue());
					}
				}
			}
		}
		if (!blackedHosts.equals(tempBlackedHost)) {
			blackedHosts = tempBlackedHost;

			blacklistGauge.reset();
			long ts = System.currentTimeMillis();
			for (Map.Entry<String, HostFailure> entry : blackedHosts.entrySet()) {
				String host = entry.getKey();
				Throwable exception = entry.getValue().getException();
				String reason = exception.getMessage();
				BlacklistUtil.FailureType failureType = entry.getValue().getFailureType();
				if (reason != null) {
					reason = reason.replaceAll(" ", "_").substring(0, Math.min(30, reason.length())).split("\n")[0];
				}
				blacklistGauge.addMetric(
						1,
						new TagGaugeStoreImpl.TagValuesBuilder()
							.addTagValue("blackedHost", host)
							.addTagValue("exception", exception.getClass().getName())
							.addTagValue("reason", reason)
							.addTagValue("type", failureType.name())
							.build()
				);

				blacklistRecordMessageSet.addMessage(
						new Message<>(new WarehouseBlacklistRecordMessage(
								entry.getValue().getHostname(),
								entry.getValue().getFailureType(),
								ts)));
			}
			blacklistActions.notifyBlacklistUpdated();
		}
	}

	public void checkFailureExceedTimeout() {
		try {
			for (HostFailures hostFailures : allFailures.values()) {
				hostFailures.checkOutdatedFailure();
			}
			tryUpdateBlacklist();
		} catch (Exception e) {
			LOG.error("checkFailureExceedTimeout error", e);
		}

		this.mainThreadExecutor.schedule(
				this::checkFailureExceedTimeout,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}

	static class HostFailures {
		private final Map<String, LinkedList<HostFailure>> hostFailures;
		private final int maxFailureNumPerHost;
		private final Time failureTimeout;
		private final int blacklistMaxLength;
		private final MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet;

		public HostFailures(
				int maxFailureNumPerHost,
				Time failureTimeout,
				int blacklistMaxLength,
				MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet) {
			this.hostFailures = new HashMap<>();
			this.maxFailureNumPerHost = maxFailureNumPerHost;
			this.failureTimeout = failureTimeout;
			this.blacklistMaxLength = blacklistMaxLength;
			this.blacklistFailureMessageSet = blacklistFailureMessageSet;
		}

		public boolean addFailure(HostFailure hostFailure) {
			Class<? extends Throwable> t = hostFailure.getException().getClass();
			int numberOfExceptionHost = calculateHosts(t);
			if (numberOfExceptionHost > blacklistMaxLength) {
				LOG.info("{} occur on too many hosts {}, ignore this failure.", LoggerHelper.secMark("errMsg", t), LoggerHelper.secMark("numberOfExceptionHost", numberOfExceptionHost));
				return false;
			}

			hostFailures.putIfAbsent(hostFailure.getHostname(), new LinkedList<>());
			hostFailures.get(hostFailure.getHostname()).add(hostFailure);
			checkOutdatedFailure();

			blacklistFailureMessageSet.addMessage(
					new Message<>(WarehouseBlacklistFailureMessage.fromHostFailure(hostFailure)));
			return true;
		}

		public int calculateHosts(Class<? extends Throwable> t) {
			return hostFailures
					.values()
					.stream()
					.mapToInt(value -> {
						for (HostFailure hostFailure : value) {
							if (hostFailure.getException().getClass() == t) {
								return 1;
							}
						}
						return 0;
					})
					.sum();
		}

		public void checkOutdatedFailure() {
			Iterator<Map.Entry<String, LinkedList<HostFailure>>> hostFailuresIterator = hostFailures.entrySet().iterator();
			while (hostFailuresIterator.hasNext()) {
				Map.Entry<String, LinkedList<HostFailure>> entry = hostFailuresIterator.next();
				LinkedList<HostFailure> failures = entry.getValue();

				// remove excess failures.
				while (failures.size() > maxFailureNumPerHost) {
					failures.removeFirst();
				}

				// remove outdated failures.
				Iterator<HostFailure> iterator = failures.iterator();
				while (iterator.hasNext()) {
					if ((System.currentTimeMillis() - iterator.next().getTimestamp())
							> failureTimeout.toMilliseconds()) {
						iterator.remove();
					} else {
						break;
					}
				}

				if (failures.isEmpty()) {
					hostFailuresIterator.remove();
				}
			}
		}

		public Set<ResourceID> getResources(String hostname) {
			LinkedList<HostFailure> failures = hostFailures.get(hostname);
			if (failures != null) {
				return failures
						.stream()
						.map(HostFailure::getResourceID)
						.collect(Collectors.toSet());
			} else {
				return Collections.emptySet();
			}

		}

		public Map<String, HostFailure> getBlackedHosts() {
			Map<String, HostFailure> blackedHosts = new TreeMap<>();
			for (Map.Entry<String, LinkedList<HostFailure>> hostFailuresEntry : hostFailures.entrySet()) {
				String host = hostFailuresEntry.getKey();
				LinkedList<HostFailure> failures = hostFailuresEntry.getValue();
				if (failures.size() >= maxFailureNumPerHost) {
					blackedHosts.put(host, failures.getLast());
				}
			}
			if (blackedHosts.size() > blacklistMaxLength) {
				List<String> keyList = new ArrayList<>(blackedHosts.keySet());
				keyList.sort(Comparator.comparingLong(o -> blackedHosts.get(o).getTimestamp()));
				for (String host : keyList) {
					if (blackedHosts.size() > blacklistMaxLength) {
						blackedHosts.remove(host);
					} else {
						break;
					}
				}
			}

			return blackedHosts;
		}

		public void clear() {
			this.hostFailures.clear();
		}
	}
}
