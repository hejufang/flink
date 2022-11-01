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
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.WarehouseBlacklistFailureMessage;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * maintain all failures and blacked hosts for one kind of {@link org.apache.flink.runtime.blacklist.BlacklistUtil.FailureType}.
 */
public class HostFailures {
	private static final Logger LOG = LoggerFactory.getLogger(HostFailures.class);

	// Exceptions within this time period will be ignored after the blacked record. These exceptions may cause by old failover.
	private final Long failureTolerateTime = 60_000L;
	// Blacked record will be marked right if there is no exceptions appear after this time period.
	private final Long accuracyCheckingTime = 360_000L;
	private final LinkedList<HostFailure> hostFailureQueue;
	private final Map<Class<? extends Throwable>, Long> filteredExceptions;
	private final Map<String, HostFailure> blackedHosts;
	private final Map<String, HostFailure> criticalBlackedHosts;
	private final int maxFailureNum;
	private final double maxHostPerExceptionRatio;
	private final int maxHostPerExceptionMinNumber;
	private int maxHostPerException;
	private final int maxFailureNumPerHost;
	private final Time failureTimeout;
	private final int blacklistMaxLength;
	private final MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet;
	private final Clock clock;

	public HostFailures(
			int maxFailureNumPerHost,
			Time failureTimeout,
			int blacklistMaxLength,
			int maxFailureNum,
			double maxHostPerExceptionRatio,
			int maxHostPerExceptionMinNumber,
			Clock clock,
			MessageSet<WarehouseBlacklistFailureMessage> blacklistFailureMessageSet) {
		this.hostFailureQueue = new LinkedList<>();
		this.filteredExceptions = new HashMap<>();
		this.blackedHosts = new HashMap<>();
		this.criticalBlackedHosts = new HashMap<>();
		this.maxFailureNumPerHost = maxFailureNumPerHost;
		this.failureTimeout = failureTimeout;
		this.blacklistMaxLength = blacklistMaxLength;
		this.blacklistFailureMessageSet = blacklistFailureMessageSet;
		this.maxFailureNum = maxFailureNum;
		this.maxHostPerExceptionRatio = maxHostPerExceptionRatio;
		this.maxHostPerExceptionMinNumber = maxHostPerExceptionMinNumber;
		this.maxHostPerException = maxHostPerExceptionMinNumber;
		this.clock = clock;
	}

	@VisibleForTesting
	public int getMaxHostPerExceptionNumber() {
		return maxHostPerException;
	}

	@VisibleForTesting
	public int getFilteredExceptionNumber() {
		return filteredExceptions.size();
	}

	public boolean addFailure(HostFailure hostFailure) {
		blacklistFailureMessageSet.addMessage(
				new Message<>(WarehouseBlacklistFailureMessage.fromHostFailure(hostFailure)));
		Class<? extends Throwable> t = hostFailure.getException().getClass();
		if (filteredExceptions.containsKey(t) &&
				clock.absoluteTimeMillis() - filteredExceptions.get(t) < failureTimeout.toMilliseconds()) {
			// update filtered exception timestamp
			LOG.debug("update filtered exception {} ts to {}", t, hostFailure.getTimestamp());
			filteredExceptions.put(t, hostFailure.getTimestamp());
			return false;
		} else {
			LOG.debug("add exception {} to queue", t);
			hostFailureQueue.add(hostFailure);
			if (hostFailureQueue.size() > maxFailureNum) {
				tryRemoveOutdatedFailure();
			}
			return true;
		}
	}

	public void tryRemoveOutdatedFailure() {
		// remove excess failures.
		while (hostFailureQueue.size() > maxFailureNum) {
			LOG.debug("Failure queue too long {}, remove the first one", hostFailureQueue.size());
			hostFailureQueue.removeFirst();
		}

		long currentTs = clock.absoluteTimeMillis();
		// remove outdated failures.
		hostFailureQueue.removeIf(hostFailure -> (currentTs - hostFailure.getTimestamp()) > failureTimeout.toMilliseconds());
		// remove outdated filtered exceptions.
		filteredExceptions.entrySet().removeIf(filterException -> {
			boolean r = currentTs - filterException.getValue() > failureTimeout.toMilliseconds();
			if (r) {
				LOG.info("filtered exception {} at {} removed", filterException.getKey(), filterException.getValue());
			}
			return r;
		});
	}

	public Set<ResourceID> getResources(String hostname) {
		return hostFailureQueue.stream()
				.filter(hostFailure -> hostFailure.getHostname().equals(hostname))
				.map(HostFailure::getResourceID)
				.collect(Collectors.toSet());
	}

	public Map<String, HostFailure> getBlackedHosts() {
		tryRemoveOutdatedFailure();

		// add new filtered exceptions.
		Map<Class<? extends Throwable>, Set<String>> exceptions = new HashMap<>();
		Map<Class<? extends Throwable>, Long> exceptionLastTimestamp = new HashMap<>();
		for (HostFailure hostFailure : hostFailureQueue) {
			if (hostFailure.isCrtialError()) {
				continue;
			}
			Class<? extends Throwable> exceptionClass = hostFailure.getException().getClass();
			exceptions.computeIfAbsent(exceptionClass, ignore -> new HashSet<>()).add(hostFailure.getHostname());
			exceptionLastTimestamp.put(exceptionClass, hostFailure.getTimestamp());
		}

		for (Class<? extends Throwable> t : exceptions.keySet()) {
			if (exceptions.get(t).size() >= maxHostPerException) {
				if (!filteredExceptions.containsKey(t)) {
					LOG.info("Add new filtered exception {} ts {}", t, exceptionLastTimestamp.get(t));
					filteredExceptions.put(t, exceptionLastTimestamp.get(t));
				} else {
					if (exceptionLastTimestamp.get(t) > filteredExceptions.get(t)) {
						LOG.debug("Update filtered exception {} ts to {}", t, exceptionLastTimestamp.get(t));
						filteredExceptions.put(t, exceptionLastTimestamp.get(t));
					}
				}
			}
		}

		// check blacked host.
		Map<String, LinkedList<HostFailure>> tmpHostFailures = new HashMap<>();
		blackedHosts.clear();
		criticalBlackedHosts.clear();
		for (HostFailure hostFailure : hostFailureQueue) {
			if (hostFailure.isCrtialError()) {
				LOG.debug("Add Critical blacked host {}", hostFailure.getHostname());
				criticalBlackedHosts.put(hostFailure.getHostname(), hostFailure);
			}
			if (!filteredExceptions.containsKey(hostFailure.getException().getClass())) {
				tmpHostFailures.computeIfAbsent(hostFailure.getHostname(), ignore -> new LinkedList<>()).add(hostFailure);
			}
		}

		if (criticalBlackedHosts.size() > blacklistMaxLength) {
			// too many blacked host, shortcut to return.
			return criticalBlackedHosts;
		}

		for (Map.Entry<String, LinkedList<HostFailure>> hostFailuresEntry : tmpHostFailures.entrySet()) {
			String host = hostFailuresEntry.getKey();
			LinkedList<HostFailure> failures = hostFailuresEntry.getValue();
			if (criticalBlackedHosts.containsKey(host)) {
				// critical failure already marked.
				continue;
			}
			if (failures.size() >= maxFailureNumPerHost) {
				LOG.debug("Add normal blacked host {}", host);
				blackedHosts.put(host, failures.getLast());
			}
		}

		int remainingBlacklistLength = blacklistMaxLength - criticalBlackedHosts.size();
		if (blackedHosts.size() > remainingBlacklistLength) {
			List<String> keyList = new ArrayList<>(blackedHosts.keySet());
			keyList.sort(Comparator.comparingLong(o -> blackedHosts.get(o).getTimestamp()));
			for (String host : keyList) {
				if (blackedHosts.size() > remainingBlacklistLength) {
					LOG.debug("Remove normal blacked host {} because exceed blacklist max length", host);
					blackedHosts.remove(host);
				} else {
					break;
				}
			}
		}

		Map<String, HostFailure> allBlackedHosts = new HashMap<>();
		allBlackedHosts.putAll(criticalBlackedHosts);
		allBlackedHosts.putAll(blackedHosts);
		return allBlackedHosts;
	}

	public BlackedExceptionAccuracy getBlackedRecordAccuracy() {
		Set<Class<? extends Throwable>> unknownBlackedException = new HashSet<>();
		Set<Class<? extends Throwable>> wrongBlackedException = new HashSet<>();
		Set<Class<? extends Throwable>> rightBlackedException = new HashSet<>();

		long currentTs = clock.absoluteTimeMillis();
		for (HostFailure blackedRecord : blackedHosts.values()) {
			Class<? extends Throwable> exception = blackedRecord.getException().getClass();
			if (blackedRecord.getTimestamp() > currentTs - accuracyCheckingTime) {
				// not exceed accuracyMarkRightTimeout, this record is unknown or wrong.
				unknownBlackedException.add(exception);
			} else {
				rightBlackedException.add(exception);
			}

			for (HostFailure hostFailure : hostFailureQueue) {
				if (exception.equals(hostFailure.getException().getClass()) &&
						hostFailure.getTimestamp() > blackedRecord.getTimestamp() + failureTolerateTime && hostFailure.getTimestamp() < blackedRecord.getTimestamp() + accuracyCheckingTime) {
					wrongBlackedException.add(exception);
				}
			}
		}

		unknownBlackedException.removeAll(wrongBlackedException);
		rightBlackedException.removeAll(unknownBlackedException);
		rightBlackedException.removeAll(wrongBlackedException);
		LOG.debug("rightBlackedException: {}, wrongBlackedException: {}, unknownBlackedException: {}", rightBlackedException, wrongBlackedException, unknownBlackedException);
		return new BlackedExceptionAccuracy(unknownBlackedException, wrongBlackedException, rightBlackedException);
	}

	public void tryUpdateMaxHostPerExceptionThreshold(int totalWorkerNumber) {
		int newMaxHostPerException = Math.max(maxHostPerExceptionMinNumber, (int) Math.ceil(totalWorkerNumber * maxHostPerExceptionRatio));
		if (newMaxHostPerException != maxHostPerException) {
			LOG.info("Update maxHostPerException from {} to {}.", maxHostPerException, newMaxHostPerException);
			maxHostPerException = newMaxHostPerException;
		}
	}

	public void clear() {
		this.hostFailureQueue.clear();
		this.filteredExceptions.clear();
		this.blackedHosts.clear();
		this.criticalBlackedHosts.clear();
	}
}
