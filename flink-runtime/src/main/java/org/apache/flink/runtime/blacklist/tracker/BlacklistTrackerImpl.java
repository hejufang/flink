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
import org.apache.flink.runtime.blacklist.BlacklistRecord;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.WarehouseBlacklistFailureMessage;
import org.apache.flink.runtime.blacklist.WarehouseBlacklistRecordMessage;
import org.apache.flink.runtime.blacklist.tracker.handler.CountBasedFailureHandler;
import org.apache.flink.runtime.blacklist.tracker.handler.FailureHandler;
import org.apache.flink.runtime.blacklist.tracker.handler.NetworkFailureHandler;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blacklist tracker for session.
 * Always run in mainThread.
 */
public class BlacklistTrackerImpl implements BlacklistTracker {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistTrackerImpl.class);

	private static final String BLACKLIST_METRIC_NAME = "blackedHost";
	private static final String BLACKLIST_EXCEPTION_ACCURACY_METRIC_NAME = "blackedExceptionAccuracy";
	private static final String BLACKED_EXCEPTION_STATUS_METRIC_NAME = "blackedExceptionStatus";
	private static final String WAREHOUSE_BLACKLIST_FAILURES = "warehouseBlacklistFailures";
	private static final String WAREHOUSE_BLACKLIST_RECORDS = "warehouseBlacklistRecords";

	private final TagGauge blacklistGauge = new TagGauge.TagGaugeBuilder().build();
	private final TagGauge blackedExceptionAccuracyGauge = new TagGauge.TagGaugeBuilder().build();
	private final TagGauge blackedExceptionStatusGauge = new TagGauge.TagGaugeBuilder().build();
	private final MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet =
			new MessageSet<>(MessageType.BLACKLIST);
	private final MessageSet<WarehouseBlacklistRecordMessage> blacklistRecordMessageSet =
			new MessageSet<>(MessageType.BLACKLIST);
	private final ResourceManagerMetricGroup jobManagerMetricGroup;
	private final Set<BlacklistRecord> blackedRecords;
	private final Set<FailureHandler> failureHandlers;
	private final Time checkInterval;

	private final Set<String> ignoreExceptionClassNames;
	private final Clock clock;

	private final FailureHandlerRouter router;
	private final AtomicBoolean scheduleToUpdateBlacklist = new AtomicBoolean(false);
	private final Time scheduleToUpdateBlacklistTime;
	private final List<ScheduledFuture<?>> periodicScheduledFutures = new ArrayList<>();

	private ScheduledExecutor scheduledExecutor;
	private ComponentMainThreadExecutor mainThreadExecutor;
	private BlacklistActions blacklistActions;

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
				blacklistConfiguration.getIgnoredExceptionClassNames(),
				blacklistConfiguration.getFailureEffectiveTime(),
				blacklistConfiguration.getNetworkFailureExpireTime(),
				blacklistConfiguration.getTimestampRecordExpireTime(),
				blacklistConfiguration.getMeanSdRatioAllBlockedThreshold(),
				blacklistConfiguration.getMeanSdRatioSomeBlockedThreshold(),
				blacklistConfiguration.getExpectedMinHost(),
				blacklistConfiguration.getExpectedBlockedHostRatio(),
				jobManagerMetricGroup,
				SystemClock.getInstance());
	}

	@VisibleForTesting
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
			List<String> ignoredExceptions,
			ResourceManagerMetricGroup jobManagerMetricGroup,
			Clock clock) {
		this(
				maxTaskFailureNumPerHost,
				maxTaskManagerFailureNumPerHost,
				taskBlacklistMaxLength,
				taskManagerBlacklistMaxLength,
				failureTimeout,
				checkInterval,
				isCriticalErrorEnable,
				maxFailureNum,
				maxHostPerExceptionMinNumber,
				maxHostPerExceptionRatio,
				ignoredExceptions,
				Time.seconds(30),
				Time.minutes(30),
				Time.seconds(60),
				0.5f,
				0.5f,
				1,
				0.5f,
				jobManagerMetricGroup,
				clock);
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
			List<String> ignoredExceptions,
			Time failureEffectiveTime,
			Time networkFailureExpireTime,
			Time timestampRecordExpireTime,
			float meanSdRatioAllBlockedThreshold,
			float meanSdRatioThresholdSomeBlocked,
			int expectedMinHost,
			float expectedBlockedHostRatio,
			ResourceManagerMetricGroup jobManagerMetricGroup,
			Clock clock) {

		this.blackedRecords = new HashSet<>();
		this.failureHandlers = new HashSet<>();
		this.checkInterval = checkInterval;
		this.jobManagerMetricGroup = checkNotNull(jobManagerMetricGroup);
		this.clock = clock;
		this.ignoreExceptionClassNames = new HashSet<>(ignoredExceptions);
		// we want to accumulate a range of exception and then update blacklist, so the schedule time to update blacklist
		// should be longer than failureEffectiveTime. Here set it as twice.
		this.scheduleToUpdateBlacklistTime = Time.milliseconds(2 * failureEffectiveTime.toMilliseconds());

		FailureHandler taskFailoverHandler = new CountBasedFailureHandler(
				maxTaskFailureNumPerHost,
				failureTimeout,
				taskBlacklistMaxLength,
				maxFailureNum,
				maxHostPerExceptionRatio,
				maxHostPerExceptionMinNumber,
				clock,
				BlacklistUtil.FailureActionType.RELEASE_BLACKED_RESOURCE,
				BlacklistUtil.FailureType.TASK,
				false);
		failureHandlers.add(taskFailoverHandler);

		// mark normal task failover handler as default.
		this.router = new FailureHandlerRouter(taskFailoverHandler);

		// task manager handler
		FailureHandler taskManagerFailoverHandler = new CountBasedFailureHandler(
				maxTaskManagerFailureNumPerHost,
				failureTimeout,
				taskManagerBlacklistMaxLength,
				maxFailureNum,
				maxHostPerExceptionRatio,
				maxHostPerExceptionMinNumber,
				clock,
				BlacklistUtil.FailureActionType.NO_SCHEDULE,
				BlacklistUtil.FailureType.TASK_MANAGER,
				false);
		failureHandlers.add(taskManagerFailoverHandler);
		router.registerFailureHandler(BlacklistUtil.FailureType.TASK_MANAGER, taskManagerFailoverHandler);

		if (isCriticalErrorEnable) {
			// critical task handler
			FailureHandler criticalFailoverHandler = CountBasedFailureHandler.createAlwaysBlackedHandler(
					failureTimeout,
					taskBlacklistMaxLength,
					maxFailureNum,
					clock,
					BlacklistUtil.FailureActionType.RELEASE_BLACKED_HOST,
					BlacklistUtil.FailureType.CRITICAL_EXCEPTION);
			failureHandlers.add(criticalFailoverHandler);
			router.registerFailureHandler(ThrowableType.CriticalError, criticalFailoverHandler);
		}

		// network failure handler
		FailureHandler networkFailureHandler = new NetworkFailureHandler(
				failureEffectiveTime,
				networkFailureExpireTime,
				timestampRecordExpireTime,
				meanSdRatioAllBlockedThreshold,
				meanSdRatioThresholdSomeBlocked,
				expectedMinHost,
				expectedBlockedHostRatio,
				maxTaskFailureNumPerHost,
				clock);
		failureHandlers.add(networkFailureHandler);
		router.registerFailureHandler(BlacklistUtil.FailureType.NETWORK, networkFailureHandler);

		FailureHandler startSlowTaskManagerHandler = CountBasedFailureHandler.createAlwaysBlackedHandler(
				failureTimeout,
				taskBlacklistMaxLength,
				maxFailureNum,
				clock,
				BlacklistUtil.FailureActionType.NO_SCHEDULE,
				BlacklistUtil.FailureType.START_SLOW_TASK_MANAGER);
		failureHandlers.add(startSlowTaskManagerHandler);
		router.registerFailureHandler(BlacklistUtil.FailureType.START_SLOW_TASK_MANAGER, startSlowTaskManagerHandler);

		registerMetrics();
	}

	@Override
	public void start(ScheduledExecutor scheduledExecutor, ComponentMainThreadExecutor mainThreadExecutor, BlacklistActions blacklistActions) {
		this.scheduledExecutor = scheduledExecutor;
		this.mainThreadExecutor = mainThreadExecutor;
		this.blacklistActions = blacklistActions;
		this.failureHandlers.forEach(h -> h.start(mainThreadExecutor, blacklistActions));
		// mainThreadExecutor in RpcEndpoint doesn't implement scheduleAtFixedRate(), so here we use another scheduledExecutor
		// to call execute() of mainThreadExecutor in the future.
		ScheduledFuture<?> checkFailureExceedTimeoutFuture = scheduledExecutor.scheduleAtFixedRate(
				() -> mainThreadExecutor.execute(this::checkFailureExceedTimeout),
				0,
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS
		);
		ScheduledFuture<?> reportBlackedRecordAccuracyFuture = scheduledExecutor.scheduleAtFixedRate(
				() -> mainThreadExecutor.execute(this::reportBlackedRecordAccuracy),
				checkInterval.toMilliseconds(),
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS
		);
		ScheduledFuture<?> updateMaxHostPerExceptionThresholdFuture = scheduledExecutor.scheduleAtFixedRate(
				() -> mainThreadExecutor.execute(this::tryUpdateMaxHostPerExceptionThreshold),
				checkInterval.toMilliseconds(),
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS
		);
		ScheduledFuture<?> updateTotalNumberOfHostsFuture = scheduledExecutor.scheduleAtFixedRate(
				() -> mainThreadExecutor.execute(this::tryUpdateTotalNumberOfHosts),
				checkInterval.toMilliseconds(),
				checkInterval.toMilliseconds(),
				TimeUnit.MILLISECONDS
		);
		periodicScheduledFutures.add(checkFailureExceedTimeoutFuture);
		periodicScheduledFutures.add(reportBlackedRecordAccuracyFuture);
		periodicScheduledFutures.add(updateMaxHostPerExceptionThresholdFuture);
		periodicScheduledFutures.add(updateTotalNumberOfHostsFuture);
	}

	@Override
	public void close() {
		LOG.info("Closing the SessionBlacklistTracker.");
		clearAll();
		for (ScheduledFuture<?> scheduledFuture : periodicScheduledFutures) {
			scheduledFuture.cancel(true);
		}
		periodicScheduledFutures.clear();
	}

	@Override
	public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) {
		this.ignoreExceptionClassNames.add(exceptionClass.getName());
		LOG.info("Add ignore Exception class {}.", exceptionClass.getName());
	}

	public Set<BlacklistRecord> getBlackedRecords() {
		return blackedRecords;
	}

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, Integer> getMaxHostPerExceptionNumber() {
		Map<BlacklistUtil.FailureType, Integer> result = new HashMap<>();
		failureHandlers.forEach(h -> result.put(h.getFailureType(), h.getMaxHostPerException()));
		return result;
	}

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, BlackedExceptionAccuracy> getBlackedExceptionAccuracies() {
		Map<BlacklistUtil.FailureType, BlackedExceptionAccuracy> result = new HashMap<>();
		failureHandlers.forEach(h -> result.put(h.getFailureType(), h.getBlackedRecordAccuracy()));
		return result;
	}

	@VisibleForTesting
	public Map<BlacklistUtil.FailureType, Integer> getFilteredExceptionNumber() {
		Map<BlacklistUtil.FailureType, Integer> result = new HashMap<>();
		failureHandlers.forEach(h -> result.put(h.getFailureType(), h.getFilteredExceptionNumber()));
		return result;
	}

	@Override
	public void clearAll() {
		this.blackedRecords.clear();
		scheduleToUpdateBlacklist.set(false);
		for (FailureHandler failureHandler : failureHandlers) {
			failureHandler.clear();
		}
		if (blacklistActions != null) {
			blacklistActions.notifyBlacklistUpdated();
		}
		LOG.info("All blacklist cleared");
	}

	@Override
	public void onFailure(BlacklistUtil.FailureType failureType, String hostname, ResourceID resourceID, Throwable cause, long timestamp) {
		if (this.ignoreExceptionClassNames.contains(cause.getClass().getName())) {
			return;
		}

		HostFailure hostFailure = new HostFailure(failureType, hostname, resourceID, cause, timestamp);
		FailureHandler failureHandler = router.getFailureHandler(hostFailure);
		failureHandler.addFailure(hostFailure);

		blacklistFailureMessageSet.addMessage(
				new Message<>(WarehouseBlacklistFailureMessage.fromHostFailure(hostFailure)));

		if (failureHandler.updateBlacklistImmediately()) {
			tryUpdateBlacklist();
		} else {
			if (scheduleToUpdateBlacklist.compareAndSet(false, true)) {
				this.mainThreadExecutor.schedule(
						() -> {
							tryUpdateBlacklist();
							scheduleToUpdateBlacklist.set(false);
						},
						scheduleToUpdateBlacklistTime.toMilliseconds(),
						TimeUnit.MILLISECONDS);
			}
		}
	}

	private void registerMetrics() {
		jobManagerMetricGroup.gauge(BLACKLIST_METRIC_NAME, blacklistGauge);
		jobManagerMetricGroup.gauge(BLACKLIST_EXCEPTION_ACCURACY_METRIC_NAME, blackedExceptionAccuracyGauge);
		jobManagerMetricGroup.gauge(BLACKED_EXCEPTION_STATUS_METRIC_NAME, blackedExceptionStatusGauge);
		jobManagerMetricGroup.gauge(WAREHOUSE_BLACKLIST_FAILURES, blacklistFailureMessageSet);
		jobManagerMetricGroup.gauge(WAREHOUSE_BLACKLIST_RECORDS, blacklistRecordMessageSet);
	}

	private Set<BlacklistRecord> updateAndGetBlackedRecords() {
		failureHandlers.forEach(FailureHandler::updateBlackedHostInternal);
		return failureHandlers.stream().map(FailureHandler::getBlackedRecord).collect(Collectors.toSet());
	}

	public void tryUpdateBlacklist() {
		if (blacklistActions == null) {
			LOG.info("Blacklist Tracker is not started, ignore update blacklist.");
			return;
		}
		long ts = clock.absoluteTimeMillis();
		Set<BlacklistRecord> newBlackedRecords = updateAndGetBlackedRecords();
		if (!blackedRecords.equals(newBlackedRecords)) {
			blackedRecords.clear();
			blackedRecords.addAll(newBlackedRecords);

			// update metrics
			blacklistGauge.reset();
			for (BlacklistRecord blacklistRecord : blackedRecords) {
				BlacklistUtil.FailureType failureType = blacklistRecord.getFailureType();
				for (String blackedHostName : blacklistRecord.getBlackedHosts()) {
					HostFailure hostFailure = blacklistRecord.getLatestBlackedFailureInHost(blackedHostName);
					Throwable exception = hostFailure.getException();
					String reason = exception.getMessage();
					if (reason != null) {
						reason = reason.replaceAll(" ", "_").substring(0, Math.min(30, reason.length())).split("\n")[0];
					}
					blacklistGauge.addMetric(
							1,
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("blackedHost", blackedHostName)
									.addTagValue("exception", exception.getClass().getName())
									.addTagValue("reason", reason)
									.addTagValue("type", failureType.name())
									.build()
					);

					blacklistRecordMessageSet.addMessage(
							new Message<>(new WarehouseBlacklistRecordMessage(
									blackedHostName,
									failureType,
									ts)));
				}
			}

			// notify action.
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
	}

	public void reportBlackedRecordAccuracy() {
		try {
			LOG.debug("Start to reportBlackedRecordAccuracy.");
			blackedExceptionStatusGauge.reset();
			blackedExceptionAccuracyGauge.reset();

			for (FailureHandler failureHandler: failureHandlers) {
				BlackedExceptionAccuracy blackedExceptionAccuracy = failureHandler.getBlackedRecordAccuracy();
				for (Map.Entry<String, Integer> entry : blackedExceptionAccuracy.getNumOfHostsForUnknownBlackedException().entrySet()) {
					blackedExceptionStatusGauge.addMetric(
							entry.getValue(),
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", failureHandler.getFailureType().name())
									.addTagValue("exception", entry.getKey())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.UNKNOWN.name())
									.build());
				}
				for (Map.Entry<String, Integer> entry : blackedExceptionAccuracy.getNumOfHostsForWrongBlackedException().entrySet()) {
					blackedExceptionStatusGauge.addMetric(
							entry.getValue(),
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", failureHandler.getFailureType().name())
									.addTagValue("exception", entry.getKey())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.WRONG.name())
									.build());
				}
				for (Map.Entry<String, Integer> entry : blackedExceptionAccuracy.getNumOfHostsForRightBlackedException().entrySet()) {
					blackedExceptionStatusGauge.addMetric(
							entry.getValue(),
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", failureHandler.getFailureType().name())
									.addTagValue("exception", entry.getKey())
									.addTagValue("blacked_exception_state", BlacklistUtil.BlackedExceptionState.RIGHT.name())
									.build());
				}
				for (Map.Entry<String, Double> confidenceEntry : blackedExceptionAccuracy.getConfidenceMapAfterBlackedHosts().entrySet()) {
					blackedExceptionAccuracyGauge.addMetric(
							confidenceEntry.getValue(),
							new TagGaugeStoreImpl.TagValuesBuilder()
									.addTagValue("type", failureHandler.getFailureType().name())
									.addTagValue("exception", confidenceEntry.getKey())
									.build());
				}
			}
		} catch (Exception e) {
			LOG.error("reportBlackedRecordAccuracy error", e);
		}
	}

	public void tryUpdateMaxHostPerExceptionThreshold() {
		try {
			LOG.debug("Start to updateMaxHostPerExceptionThreshold.");
			int totalWorkerNumber = blacklistActions.getRegisteredWorkerNumber();
			if (totalWorkerNumber > 0) {
				for (FailureHandler failureHandler : failureHandlers) {
					failureHandler.tryUpdateMaxHostPerExceptionThreshold(totalWorkerNumber);
				}
			}
		} catch (Exception e) {
			LOG.error("updateMaxHostPerExceptionThreshold error", e);
		}
	}

	public void tryUpdateTotalNumberOfHosts(){
		try {
			LOG.debug("Start to updateTotalNumberOfHosts.");
			int totalHostNumber = blacklistActions.queryNumberOfHosts();
			if (totalHostNumber > 0) {
				for (FailureHandler failureHandler : failureHandlers) {
					failureHandler.updateTotalNumberOfHosts(totalHostNumber);
				}
			}
		} catch (Exception e) {
			LOG.error("updateTotalNumberOfHosts error", e);
		}
	}
}
