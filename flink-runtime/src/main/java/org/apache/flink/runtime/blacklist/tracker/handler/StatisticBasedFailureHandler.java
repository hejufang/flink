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

package org.apache.flink.runtime.blacklist.tracker.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blacklist.BlacklistActions;
import org.apache.flink.runtime.blacklist.BlacklistRecord;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Statistic based failure handler.
 */
public abstract class StatisticBasedFailureHandler extends AbstractFailureHandler {
	private static final Logger LOG = LoggerFactory.getLogger(StatisticBasedFailureHandler.class);

	// statistic related
	private final Time failureExpireTime;
	private final Time failureEffectiveTime;
	private final float meanSdRatioAllBlockedThreshold;
	private final float meanSdRatioSomeBlockedThreshold;
	private final int expectedMinHost;
	private final float expectedBlockedHostRatio;
	private final int maxTaskFailureNumPerHost;

	protected final Map<String, LinkedList<HostFailure>> blackedHosts = new HashMap<>();
	protected final Map<String, LinkedList<HostFailure>> failureHosts = new HashMap<>();
	protected final LinkedList<HostFailure> hostFailuresQueue = new LinkedList<>();

	protected final Clock clock;
	protected final int blacklistMaxLength;
	protected int currentTotalNumHosts;

	protected BlacklistActions blacklistActions;

	public StatisticBasedFailureHandler(
			Time failureEffectiveTime,
			Time failureExpireTime,
			int blacklistMaxLength,
			float meanSdRatioAllBlockedThreshold,
			float meanSdRatioSomeBlockedThreshold,
			int expectedMinHost,
			float expectedBlockedHostRatio,
			int maxTaskFailureNumPerHost,
			Clock clock) {
		this.failureEffectiveTime = failureEffectiveTime;
		this.failureExpireTime = failureExpireTime;
		this.blacklistMaxLength = blacklistMaxLength;
		this.meanSdRatioAllBlockedThreshold = meanSdRatioAllBlockedThreshold;
		this.meanSdRatioSomeBlockedThreshold = meanSdRatioSomeBlockedThreshold;
		this.expectedMinHost = expectedMinHost;
		this.expectedBlockedHostRatio = expectedBlockedHostRatio;
		this.maxTaskFailureNumPerHost = maxTaskFailureNumPerHost;
		this.clock = clock;
	}

	@Override
	public void start(@Nullable ComponentMainThreadExecutor mainThreadExecutor, BlacklistActions blacklistActions) {
		this.blacklistActions = blacklistActions;
	}

	@Override
	public boolean updateBlacklistImmediately() {
		return false;
	}

	@Override
	public int getFilteredExceptionNumber() {
		// not needed in this handler
		return 0;
	}

	@Override
	public void updateTotalNumberOfHosts(int currentTotalNumHosts){
		this.currentTotalNumHosts = currentTotalNumHosts;
	}

	@Override
	public BlacklistRecord getBlackedRecord() {
		return new BlacklistRecord(getFailureActionType(), getFailureType(), blackedHosts);
	}

	public void updateBlackedHostInternal() {
		Map<String, LinkedList<HostFailure>> failureHostsCopy = removeExpiredFailuresAndReturnEffectiveFailureMap();
		double mean = calculateMean(failureHostsCopy);
		double sd = calculateStandardDeviation(failureHostsCopy, mean);
		// analyzing ...
		Map<String, LinkedList<HostFailure>> blackedHostTempMap = new HashMap<>();
		// If sd is relative small comparing to mean, then all relative hosts may exist some environment problems.
		if (sd < meanSdRatioAllBlockedThreshold * mean) {
			if (mean < maxTaskFailureNumPerHost) {
				// too few exceptions
				return;
			}
			if (failureHostsCopy.size() < expectedBlockedHostRatio * currentTotalNumHosts
					|| currentTotalNumHosts > 0 && currentTotalNumHosts < expectedMinHost) {
				// 1. the number of failure hosts is small comparing total number of hosts or
				// 2. the current total number of hosts is very small and less than the expected minimum number of hosts.
				failureHostsCopy.forEach((s, hostFailures) -> markExceptionInHost(blackedHostTempMap, s, hostFailures));
				LOG.warn("Add all hosts having possible issues to blacklist.");
			} else {
				LOG.warn("There may be a common problem happening in current cluster, issueHosts: {}", failureHostsCopy.keySet());
			}
		} else {
			// filter hosts having number of exceptions greater than the numExceptionsThreshold.
			final double numExceptionsThreshold = mean + meanSdRatioSomeBlockedThreshold * sd;
			for (String host : failureHostsCopy.keySet()) {
				LinkedList<HostFailure> failures = failureHostsCopy.get(host);
				if (failures.size() > numExceptionsThreshold) {
					markExceptionInHost(blackedHostTempMap, host, failures);
				}
			}
		}
		if (!blackedHostTempMap.isEmpty()) {
			blackedHosts.putAll(blackedHostTempMap);
			LOG.warn("add new host to blacklist due to possible host problem: {}, current blacked hosts: {}",
					blackedHostTempMap.keySet(), blackedHosts.keySet());
			LOG.debug(generateIssueHostSnapshot(mean, sd));
			// remove the host failure statistics information that have been in blocked hosts
			removeFailuresFromBlackedHosts();
			removeEarliestBlacklist(this.blackedHosts);
		}
	}

	protected int getBlacklistMaxLength(){
		return blacklistMaxLength;
	}

	@Override
	public void clear() {
		blackedHosts.clear();
		failureHosts.clear();
		currentTotalNumHosts = 0;
	}

	/**
	 * Implement to check if the related task managers of a failure are offline. If any of the task managers is offline,
	 * this method return true, otherwise return false.
	 * @param hostFailure the failure
	 * @return true if any of its related task manager is offline, otherwise false
	 */
	protected abstract boolean isFailureRelatedTaskManagersOffline(HostFailure hostFailure);

	/**
	 * Implement to remove the recorded failures whose relative hosts has been blacklisted.
	 */
	protected abstract void removeFailuresFromBlackedHosts();

	/**
	 * Check whether the host is in blacklist.
	 *
	 * @param hostName the name of the host
	 * @return true if this host is in the blacklist.
	 */
	protected boolean checkHostInBlacklist(String hostName) {
		return blackedHosts.containsKey(hostName);
	}

	protected boolean addFailureIfNotInBlacklist(HostFailure hostFailure, String hostName) {
		if (blackedHosts.containsKey(hostName)) {
			// we should ignore those failures from host that was already in blacklist.
			return false;
		}
		this.failureHosts.computeIfAbsent(hostName, v -> new LinkedList<>());
		this.failureHosts.get(hostName).addLast(hostFailure);
		return true;
	}

	protected long getCurrentTimestamp(){
		return clock.absoluteTimeMillis();
	}

	@Override
	protected LinkedList<HostFailure> getHostFailureQueue() {
		long currentTime = clock.absoluteTimeMillis();
		// remove expired exceptions, exceptions will be expired after several minutes
		this.hostFailuresQueue.removeIf(failure -> currentTime - failure.getTimestamp() > failureExpireTime.toMilliseconds());
		return hostFailuresQueue;
	}

	// Internal functions.
	private Map<String, LinkedList<HostFailure>> removeExpiredFailuresAndReturnEffectiveFailureMap() {
		long currentTime = clock.absoluteTimeMillis();
		Map<String, LinkedList<HostFailure>> failureHostsCopy = new HashMap<>();
		for (Map.Entry<String, LinkedList<HostFailure>> item : failureHosts.entrySet()) {
			// remove expired exceptions, exceptions will be expired after several minutes
			item.getValue().removeIf(failure -> currentTime - failure.getTimestamp() > failureExpireTime.toMilliseconds());
			// if the src or dest task manager is offline, we ignore this network failure.
			item.getValue().removeIf(this::isFailureRelatedTaskManagersOffline);

			LinkedList<HostFailure> hostFailuresCopy = new LinkedList<>(item.getValue());
			// only analyze the failures happened after failureEffectiveTime
			hostFailuresCopy.removeIf(failure -> currentTime - failure.getTimestamp() < failureEffectiveTime.toMilliseconds());
			if (!hostFailuresCopy.isEmpty()) {
				failureHostsCopy.put(item.getKey(), hostFailuresCopy);
			}
		}
		failureHosts.entrySet().removeIf(e -> e.getValue().isEmpty());
		removeExpiredAndOfflineHostsRelatedFailuresInQueue();
		return failureHostsCopy;
	}

	private void removeExpiredAndOfflineHostsRelatedFailuresInQueue(){
		long currentTime = clock.absoluteTimeMillis();
		// remove expired exceptions, exceptions will be expired after several minutes
		this.hostFailuresQueue.removeIf(failure -> currentTime - failure.getTimestamp() > failureExpireTime.toMilliseconds());
		// if the src or dest task manager is offline, we ignore this network failure.
		this.hostFailuresQueue.removeIf(this::isFailureRelatedTaskManagersOffline);
	}

	private void markExceptionInHost(Map<String, LinkedList<HostFailure>> blackedHost, String host, LinkedList<HostFailure> failures) {
		LinkedList<HostFailure> listOnlyContainsLastFailure = new LinkedList<>();
		listOnlyContainsLastFailure.add(failures.getLast());
		blackedHost.put(host, listOnlyContainsLastFailure);
	}

	private String generateIssueHostSnapshot(double mean, double sd) {
		StringBuilder sb = new StringBuilder()
				.append("---------print Issues Hosts Info------\n")
				.append("mean of #exceptions in all hosts:")
				.append(mean)
				.append('\n')
				.append("stand deviation of #exceptions in all hosts:")
				.append(sd)
				.append('\n');
		for (String host : failureHosts.keySet()) {
			sb.append(String.format("%s -> %d\n", host, failureHosts.get(host).size()));
		}
		return sb.toString();
	}

	private static double calculateMean(Map<String, LinkedList<HostFailure>> issueHosts) {
		AtomicInteger sum = new AtomicInteger();
		issueHosts.forEach((s, hostFailures) -> sum.addAndGet(hostFailures.size()));
		return ((double) sum.get()) / issueHosts.size();
	}

	private static double calculateStandardDeviation(Map<String, LinkedList<HostFailure>> issueHosts, final double mean) {
		double var = 0;
		for (List<HostFailure> failures : issueHosts.values()) {
			var += (mean - failures.size()) * (mean - failures.size());
		}
		return Math.sqrt(var / issueHosts.size());
	}
}
