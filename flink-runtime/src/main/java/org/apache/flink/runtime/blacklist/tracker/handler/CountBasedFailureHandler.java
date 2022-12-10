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
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Count based Failure handler, check failure on same host whether exceed the threshold.
 */
public class CountBasedFailureHandler extends AbstractCountBasedFailureHandler {
	private static final Logger LOG = LoggerFactory.getLogger(CountBasedFailureHandler.class);
	private final Map<String, LinkedList<HostFailure>> blackedHosts;

	public CountBasedFailureHandler(
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
		super(maxFailureNumPerHost, failureTimeout, blacklistMaxLength, maxFailureNum, maxHostPerExceptionRatio, maxHostPerExceptionMinNumber, clock, failureActionType, failureType, shouldUpdateBlacklistImmediately);
		blackedHosts = new HashMap<>();
	}

	@Override
	public BlacklistRecord getBlackedRecord() {
		return new BlacklistRecord(getFailureActionType(), getFailureType(), blackedHosts);
	}

	public void updateBlackedHostInternal() {
		tryRemoveOutdatedFailure();
		tryUpdateFilteredException();

		Map<String, LinkedList<HostFailure>> tmpBlackedHosts = new HashMap<>();

		// filter exception and key by host.
		for (HostFailure hostFailure : getHostFailureQueue()) {
			if (!isExceptionFiltered(hostFailure.getException().getClass())) {
				tmpBlackedHosts.computeIfAbsent(hostFailure.getHostname(), ignore -> new LinkedList<>()).add(hostFailure);
			}
		}

		tmpBlackedHosts.entrySet().removeIf(e -> e.getValue().size() < getMaxFailureNumPerHost());

		removeEarliestBlacklist(tmpBlackedHosts);

		LOG.debug("newly blacked hosts are {}.", tmpBlackedHosts);
		this.blackedHosts.clear();
		this.blackedHosts.putAll(tmpBlackedHosts);
	}

	@Override
	public void clear() {
		this.blackedHosts.clear();
		super.clear();
	}

	@Override
	public void start(ComponentMainThreadExecutor mainThreadExecutor, BlacklistActions blacklistActions) {
		// do nothing
	}

	@Override
	public void updateTotalNumberOfHosts(int currentTotalNumHosts) {
		// not need
	}

	public static CountBasedFailureHandler createAlwaysBlackedHandler(
			Time failureTimeout,
			int blacklistMaxLength,
			int maxFailureNum,
			Clock clock,
			BlacklistUtil.FailureActionType failureActionType,
			BlacklistUtil.FailureType failureType) {
		return new CountBasedFailureHandler(1, failureTimeout, blacklistMaxLength, maxFailureNum, 0, 0, clock, failureActionType, failureType, true);
	}

}
