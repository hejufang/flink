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

import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.tracker.BlackedExceptionAccuracy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Abstract failure handler which implements to calculate blacklist accuracy.
 */
public abstract class AbstractFailureHandler implements FailureHandler{

	private static final Logger LOG = LoggerFactory.getLogger(AbstractFailureHandler.class);

	// Exceptions within this time period will be ignored after the blacked record. These exceptions may cause by old failover.
	private final Long failureTolerateTime = 60_000L;
	// The time of window to record recent number of exceptions within this time window for accuracy calculation.
	private final Long accuracyCheckingWindow = 300_000L;

	@Override
	public BlackedExceptionAccuracy getBlackedRecordAccuracy() {
		long currentTs = getCurrentTimestamp();
		// record the number of blacked host for each exception.
		Map<String, Integer> numOfHostsForBlackedException = new HashMap<>();
		// record the earliest time of the latest exception in each blacked host for each exception. The key is the
		// exception name, the value is the earliest time of the latest exception in all blacked host due to this exception.
		// This variable is used to identify the unknown blockage for one exception.
		Map<String, Long> earliestTimeOfLatestExceptionsMap = new HashMap<>();
		// use to calculate confidence of each blacked exceptions, the key is the exception class name, the value is the number
		// of this exception in the recent time of accuracyCheckingTime.
		Map<Class<? extends Throwable>, Integer> numExceptionsMapAfterBlackedHosts = new HashMap<>();
		for (HostFailure latestBlackedFailure : getBlackedRecord().getLatestBlackedFailure()) {
			Class<? extends Throwable> exception = latestBlackedFailure.getException().getClass();
			if (earliestTimeOfLatestExceptionsMap.containsKey(exception.getName())) {
				long earliestTimeOfLatestException = earliestTimeOfLatestExceptionsMap.get(exception.getName());
				earliestTimeOfLatestException = Math.min(earliestTimeOfLatestException, latestBlackedFailure.getTimestamp());
				earliestTimeOfLatestExceptionsMap.put(exception.getName(), earliestTimeOfLatestException);
			} else {
				earliestTimeOfLatestExceptionsMap.put(exception.getName(), latestBlackedFailure.getTimestamp());
			}
			int numOfBlackedHost = numOfHostsForBlackedException.getOrDefault(exception.getName(), 0);
			numOfHostsForBlackedException.put(exception.getName(), numOfBlackedHost + 1);
			numExceptionsMapAfterBlackedHosts.put(exception, 0);
		}
		Set<String> unknownExceptions = new HashSet<>();
		for (Map.Entry<String, Long> entry : earliestTimeOfLatestExceptionsMap.entrySet()) {
			// not exceed failureTolerateTime, this blacklist record status is unknown.
			if (currentTs <= entry.getValue() + failureTolerateTime) {
				unknownExceptions.add(entry.getKey());
			}
		}
		LinkedList<HostFailure> hostFailureQueue = getHostFailureQueue();
		for (HostFailure hostFailure : hostFailureQueue) {
			Class<? extends Throwable> exception = hostFailure.getException().getClass();
			if (numExceptionsMapAfterBlackedHosts.containsKey(exception) && hostFailure.getTimestamp() > currentTs - accuracyCheckingWindow
					&& hostFailure.getTimestamp() > earliestTimeOfLatestExceptionsMap.get(exception.getName()) + failureTolerateTime) {
				int numOfExceptionsAfterBlacked = numExceptionsMapAfterBlackedHosts.get(exception);
				numExceptionsMapAfterBlackedHosts.put(exception, numOfExceptionsAfterBlacked + 1);
			}
		}
		Map<String, Double> confidenceMapAfterBlackedHosts = new HashMap<>();
		for (Map.Entry<Class<? extends Throwable>, Integer> numExceptionsEntry : numExceptionsMapAfterBlackedHosts.entrySet()) {
			double confidence = 1;
			if (!hostFailureQueue.isEmpty()) {
				confidence = 1 - (double) (numExceptionsEntry.getValue()) / hostFailureQueue.size();
			}
			confidenceMapAfterBlackedHosts.put(numExceptionsEntry.getKey().getName(), confidence);
		}
		LOG.debug("confidenceMap for each exceptions: {}, unknownExceptions: {}", confidenceMapAfterBlackedHosts, unknownExceptions);
		return new BlackedExceptionAccuracy(numOfHostsForBlackedException, unknownExceptions, confidenceMapAfterBlackedHosts);
	}

	protected abstract LinkedList<HostFailure> getHostFailureQueue();

	protected abstract long getCurrentTimestamp();

}
