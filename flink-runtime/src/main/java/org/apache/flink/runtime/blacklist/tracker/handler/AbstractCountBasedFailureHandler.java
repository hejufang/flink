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
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * the abstract implement of {@link FailureHandler}.
 * maintain all failures and filtered exceptions.
 */
public abstract class AbstractCountBasedFailureHandler extends AbstractFailureHandler {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractCountBasedFailureHandler.class);

	private final LinkedList<HostFailure> hostFailureQueue;
	private final Map<Class<? extends Throwable>, Long> filteredExceptions;
	private final int maxFailureNum;
	private final double maxHostPerExceptionRatio;
	private final int maxHostPerExceptionMinNumber;
	private int maxHostPerException;
	private final int maxFailureNumPerHost;
	private final Time failureTimeout;
	private final int blacklistMaxLength;
	private final Clock clock;
	private final BlacklistUtil.FailureActionType failureActionType;
	private final BlacklistUtil.FailureType failureType;

	private final boolean shouldUpdateBlacklistImmediately;

	public AbstractCountBasedFailureHandler(
			int maxFailureNumPerHost,
			Time failureTimeout,
			int blacklistMaxLength,
			int maxFailureNum,
			double maxHostPerExceptionRatio,
			int maxHostPerExceptionMinNumber,
			Clock clock,
			BlacklistUtil.FailureActionType failureActionType,
			BlacklistUtil.FailureType failureType,
			boolean shouldUpdateBlacklistImmediately) {
		this.hostFailureQueue = new LinkedList<>();
		this.filteredExceptions = new HashMap<>();
		this.maxFailureNumPerHost = maxFailureNumPerHost;
		this.failureTimeout = failureTimeout;
		this.blacklistMaxLength = blacklistMaxLength;
		this.maxFailureNum = maxFailureNum;
		this.maxHostPerExceptionRatio = maxHostPerExceptionRatio;
		this.maxHostPerExceptionMinNumber = maxHostPerExceptionMinNumber;
		this.maxHostPerException = maxHostPerExceptionMinNumber;
		this.clock = clock;
		this.failureActionType = failureActionType;
		this.failureType = failureType;
		this.shouldUpdateBlacklistImmediately = shouldUpdateBlacklistImmediately;
	}

	/*
	 * Getters.
	 */

	public LinkedList<HostFailure> getHostFailureQueue() {
		return hostFailureQueue;
	}

	public int getMaxFailureNumPerHost() {
		return maxFailureNumPerHost;
	}

	public int getBlacklistMaxLength() {
		return blacklistMaxLength;
	}

	/*
	 * Overrides.
	 */

	@Override
	public int getMaxHostPerException() {
		return maxHostPerException;
	}

	@Override
	public int getFilteredExceptionNumber() {
		return filteredExceptions.size();
	}

	@Override
	public BlacklistUtil.FailureActionType getFailureActionType() {
		return failureActionType;
	}

	@Override
	public BlacklistUtil.FailureType getFailureType() {
		return failureType;
	}

	@Override
	public boolean addFailure(HostFailure hostFailure) {
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

	@Override
	public void tryUpdateMaxHostPerExceptionThreshold(int totalWorkerNumber) {
		int newMaxHostPerException = Math.max(maxHostPerExceptionMinNumber, (int) Math.ceil(totalWorkerNumber * maxHostPerExceptionRatio));
		if (newMaxHostPerException != maxHostPerException) {
			LOG.info("Update maxHostPerException from {} to {}.", maxHostPerException, newMaxHostPerException);
			maxHostPerException = newMaxHostPerException;
		}
	}

	@Override
	public boolean updateBlacklistImmediately() {
		return shouldUpdateBlacklistImmediately;
	}

	@Override
	public void clear() {
		this.hostFailureQueue.clear();
		this.filteredExceptions.clear();
	}

	/*
	 * Internal methods
	 */

	public boolean isExceptionFiltered(Class<? extends Throwable> exceptionClass) {
		return filteredExceptions.containsKey(exceptionClass);
	}

	public void tryUpdateFilteredException() {
		if (maxHostPerException <= 0) {
			return;
		}
		// add new filtered exceptions.
		Map<Class<? extends Throwable>, Set<String>> exceptions = new HashMap<>();
		Map<Class<? extends Throwable>, Long> exceptionLastTimestamp = new HashMap<>();
		for (HostFailure hostFailure : getHostFailureQueue()) {
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
	}

	public void tryRemoveOutdatedFailure() {
		// remove excess failures.
		while (hostFailureQueue.size() > maxFailureNum) {
			LOG.info("Failure queue too long {}, remove the first one", hostFailureQueue.size());
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

	protected long getCurrentTimestamp(){
		return clock.absoluteTimeMillis();
	}
}