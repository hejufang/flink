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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blacklist tracker for session.
 * Always run in mainThread.
 */
public class BlacklistTrackerImpl implements BlacklistTracker {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistTrackerImpl.class);

	private static final String BLACKLIST_METRIC_NAME = "blackedHost";
	private static final String BLACKLIST_EXCEPTION_ACCURACY_METRIC_NAME = "blackedExceptionAccuracy";
	private static final String HAS_WRONG_BLACKED_EXCEPTION_METRIC_NAME = "hasWrongBlackedException";
	private static final String WAREHOUSE_BLACKLIST_FAILURES = "warehouseBlacklistFailures";
	private static final String WAREHOUSE_BLACKLIST_RECORDS = "warehouseBlacklistRecords";

	private final TagGauge blacklistGauge = new TagGauge.TagGaugeBuilder().build();
	private final TagGauge blackedExceptionAccuracyGauge = new TagGauge.TagGaugeBuilder().build();
	private final TagGauge hasWrongBlackedExceptionGauge = new TagGauge.TagGaugeBuilder().build();
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
	private final boolean isCriticalErrorEnable;
	private final Clock clock;

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
				blacklistConfiguration.isBlacklistCriticalEnable(),
				blacklistConfiguration.getMaxFailureNum(),
				blacklistConfiguration.getMaxHostPerExceptionMinNumber(),
				blacklistConfiguration.getMaxHostPerExceptionRatio(),
				jobManagerMetricGroup,
				SystemClock.getInstance());
	}

	public BlacklistTrackerImpl(
			int maxTaskFailureNumPerHost,
			int maxTaskManagerFailureNumPerHost,
			int taskBlacklistMaxLength,
			int taskManagerBlacklistMaxLength,
			Time failureTimeout,
			Time checkInterval,
			boolean isCriticalErrorEnable,
			int maxFailureNum,
			int maxHostPerExceptionMinNumber,
			double maxHostPerExceptionRatio,
			ResourceManagerMetricGroup jobManagerMetricGroup,
			Clock clock) {

		this.blackedHosts = new HashMap<>();
		this.checkInterval = checkInterval;
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);
		this.clock = clock;
		this.allFailures = new HashMap<>();
		this.allFailures.put(BlacklistUtil.FailureType.TASK_MANAGER,
				new HostFailures(
						maxTaskManagerFailureNumPerHost,
						failureTimeout,
						taskManagerBlacklistMaxLength,
						maxFailureNum,
						maxHostPerExceptionRatio,
						maxHostPerExceptionMinNumber,
						clock,
						blacklistFailureMessageSet));
		this.allFailures.put(BlacklistUtil.FailureType.TASK,
				new HostFailures(
						maxTaskFailureNumPerHost,
						failureTimeout,
						taskBlacklistMaxLength,
						maxFailureNum,
						maxHostPerExceptionRatio,
						maxHostPerExceptionMinNumber,
						clock,
						blacklistFailureMessageSet));
		this.isCriticalErrorEnable = isCriticalErrorEnable;

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
		this.mainThreadExecutor.schedule(
				this::reportBlackedRecordAccuracy,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
		this.mainThreadExecutor.schedule(
				this::tryUpdateMaxHostPerExceptionThreshold,
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
	public Map<String, HostFailure> getBlackedCriticalErrorHosts() {
		Map<String, HostFailure> criticalErrorHosts = new HashMap<>();
		if (!isCriticalErrorEnable) {
			return criticalErrorHosts;
		}
		for (Map.Entry<String, HostFailure> entry : blackedHosts.entrySet()) {
			if (entry.getValue().isCrtialError()) {
				criticalErrorHosts.put(entry.getKey(), entry.getValue());
			}
		}
		return criticalErrorHosts;
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

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, Integer> getMaxHostPerExceptionNumber() {
		Map<BlacklistUtil.FailureType, Integer> result = new HashMap<>();
		allFailures.forEach((key, value) -> result.put(key, value.getMaxHostPerExceptionNumber()));
		return result;
	}

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, BlackedExceptionAccuracy> getBlackedExceptionAccuracies() {
		Map<BlacklistUtil.FailureType, BlackedExceptionAccuracy> result = new HashMap<>();
		allFailures.forEach((key, value) -> result.put(key, value.getBlackedRecordAccuracy()));
		return result;
	}

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, Integer> getFilteredExceptionNumber() {
		Map<BlacklistUtil.FailureType, Integer> result = new HashMap<>();
		allFailures.forEach((key, value) -> result.put(key, value.getFilteredExceptionNumber()));
		return result;
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
		jobManagerMetricGroup.gauge(BLACKLIST_EXCEPTION_ACCURACY_METRIC_NAME, blackedExceptionAccuracyGauge);
		jobManagerMetricGroup.gauge(HAS_WRONG_BLACKED_EXCEPTION_METRIC_NAME, hasWrongBlackedExceptionGauge);
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
			long ts = clock.absoluteTimeMillis();
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
							.addTagValue("isCritical", String.valueOf(entry.getValue().isCrtialError() ? 1 : 0))
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
			LOG.debug("Start to checkFailureExceedTimeout");
			tryUpdateBlacklist();
		} catch (Exception e) {
			LOG.error("checkFailureExceedTimeout error", e);
		}

		this.mainThreadExecutor.schedule(
				this::checkFailureExceedTimeout,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}

	public void reportBlackedRecordAccuracy() {
		try {
			LOG.debug("Start to reportBlackedRecordAccuracy.");
			hasWrongBlackedExceptionGauge.reset();
			blackedExceptionAccuracyGauge.reset();

			for (Map.Entry<BlacklistUtil.FailureType, HostFailures> allFailuresEntry : allFailures.entrySet()) {
				BlackedExceptionAccuracy blackedExceptionAccuracy = allFailuresEntry.getValue().getBlackedRecordAccuracy();
				hasWrongBlackedExceptionGauge.addMetric(
						blackedExceptionAccuracy.hasWrongBlackedException() ? 1 : 0,
						new TagGaugeStoreImpl.TagValuesBuilder()
								.addTagValue("type", allFailuresEntry.getKey().name())
								.build());

				for (Class<? extends Throwable> e : blackedExceptionAccuracy.getUnknownBlackedException()) {
					blackedExceptionAccuracyGauge.addMetric(
							1,
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", allFailuresEntry.getKey().name())
									.addTagValue("exception", e.getName())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.UNKNOWN.name())
									.build());
				}
				for (Class<? extends Throwable> e : blackedExceptionAccuracy.getWrongBlackedException()) {
					blackedExceptionAccuracyGauge.addMetric(
							1,
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", allFailuresEntry.getKey().name())
									.addTagValue("exception", e.getName())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.WRONG.name())
									.build());
				}
				for (Class<? extends Throwable> e : blackedExceptionAccuracy.getRightBlackedException()) {
					blackedExceptionAccuracyGauge.addMetric(
							1,
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", allFailuresEntry.getKey().name())
									.addTagValue("exception", e.getName())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.RIGHT.name())
									.build());
				}
			}
		} catch (Exception e) {
			LOG.error("reportBlackedRecordAccuracy error", e);
		}

		this.mainThreadExecutor.schedule(
				this::reportBlackedRecordAccuracy,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}

	public void tryUpdateMaxHostPerExceptionThreshold() {
		try {
			LOG.debug("Start to updateMaxHostPerExceptionThreshold.");
			int totalWorkerNumber = blacklistActions.getRegisteredWorkerNumber();
			if (totalWorkerNumber > 0) {
				for (HostFailures hostFailures : allFailures.values()) {
					hostFailures.tryUpdateMaxHostPerExceptionThreshold(totalWorkerNumber);
				}
			}
		} catch (Exception e) {
			LOG.error("updateMaxHostPerExceptionThreshold error", e);
		}

		this.mainThreadExecutor.schedule(
				this::tryUpdateMaxHostPerExceptionThreshold,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS);
	}
}
