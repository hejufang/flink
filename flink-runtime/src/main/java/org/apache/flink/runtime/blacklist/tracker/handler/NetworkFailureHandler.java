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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.NetworkAddress;
import org.apache.flink.runtime.io.network.NetworkTraceable;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * The handler used to process network failure, specially, for those exceptions that inherits {@code NetworkTraceable}.
 */
public class NetworkFailureHandler extends StatisticBasedFailureHandler {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkFailureHandler.class);

	// This map saves the resource id to the last failure timestamp of connections from the same remote address. The key
	// is the resource id, the value is another map where the key is the remote address, the value is the timestamp of the
	// last network connection failure.
	private final Map<String, Map<String, Long>> resourceIdToNetworkFailuresTimestamp = new HashMap<>();
	private final Time timestampRecordExpireTime;

	public NetworkFailureHandler(
			Time failureEffectiveTime,
			Time networkFailureExpireTime,
			Time timestampRecordExpireTime,
			float meanSdRatioAllBlockedThreshold,
			float meanSdRatioSomeBlockedThreshold,
			int expectedMinHost,
			float expectedBlockedHostRatio,
			int maxTaskFailureNumPerHost,
			Clock clock) {
		super(
				failureEffectiveTime,
				networkFailureExpireTime,
				meanSdRatioAllBlockedThreshold,
				meanSdRatioSomeBlockedThreshold,
				expectedMinHost,
				expectedBlockedHostRatio,
				maxTaskFailureNumPerHost,
				clock);
		this.timestampRecordExpireTime = timestampRecordExpireTime;
	}

	@Override
	public BlacklistUtil.FailureType getFailureType() {
		return BlacklistUtil.FailureType.NETWORK;
	}

	@Override
	public BlacklistUtil.FailureActionType getFailureActionType() {
		return BlacklistUtil.FailureActionType.RELEASE_BLACKED_HOST;
	}

	@Override
	public int getMaxHostPerException() {
		// not used now
		return 0;
	}

	@Override
	public void tryUpdateMaxHostPerExceptionThreshold(int totalWorkerNumber) {
		// not used now
	}

	@Override
	public boolean addFailure(HostFailure hostFailure) {
		NetworkTraceable networkTraceable = (NetworkTraceable) hostFailure.getException();
		NetworkAddress remoteAddress = networkTraceable.getRemoteAddress();
		if (remoteAddress == null) {
			LOG.warn("ignore host network failure without destination {}", hostFailure);
			return false;
		}
		if (isFailureRelatedTaskManagersOffline(hostFailure)) {
			// if the src or dest task manager is offline, we ignore this network failure.
			return false;
		}
		// Because there may be multiple network exceptions from the same src to the same dest simultaneously. So the
		// network failure handler needs to check the timestamp for last failures to know whether we need to skip this failure.
		String resourceId = hostFailure.getResourceID().getResourceIdString();
		if (resourceIdToNetworkFailuresTimestamp.containsKey(resourceId)) {
			Map<String, Long> remoteAddressToLastTimestamp = resourceIdToNetworkFailuresTimestamp.get(resourceId);
			if (remoteAddressToLastTimestamp.containsKey(remoteAddress.toString())
					&& hostFailure.getTimestamp() - remoteAddressToLastTimestamp.get(remoteAddress.toString()) < timestampRecordExpireTime.toMilliseconds()) {
				// network failure with same src and dest happened again in a very short time, skip processing this failure
				LOG.debug("network failure with same src and dest happened again in {}, skip processing this failure: {}", timestampRecordExpireTime, hostFailure);
				return false;
			}
			// update the last timestamp for this address
			remoteAddressToLastTimestamp.put(remoteAddress.toString(), hostFailure.getTimestamp());
		} else {
			// update the last timestamp for this address
			Map<String, Long> remoteAddressToLastTimestamp = new HashMap<>();
			remoteAddressToLastTimestamp.put(remoteAddress.toString(), hostFailure.getTimestamp());
			resourceIdToNetworkFailuresTimestamp.put(resourceId, remoteAddressToLastTimestamp);
		}
		clearExpiredTimestampRecord(2 * timestampRecordExpireTime.toMilliseconds());
		// should not add host if either the src and dest host has been in the blacklist.
		String remoteNodeName = blacklistActions.queryNodeName(remoteAddress);
		if (StringUtils.isNullOrWhitespaceOnly(remoteNodeName)) {
			LOG.debug("dest host is unknown: {}", hostFailure);
			return false;
		}
		if (checkHostInBlacklist(hostFailure.getHostname()) || checkHostInBlacklist(remoteNodeName)) {
			LOG.debug("src and dest host is already in blacklist: {}", hostFailure);
			return false;
		}
		boolean addSrc = addFailureIfNotInBlacklist(hostFailure, hostFailure.getHostname());
		boolean addDest = addFailureIfNotInBlacklist(hostFailure, remoteNodeName);
		return addSrc && addDest;
	}

	protected boolean isFailureRelatedTaskManagersOffline(HostFailure hostFailure){
		NetworkTraceable networkTraceable = (NetworkTraceable) hostFailure.getException();
		NetworkAddress remoteAddress = networkTraceable.getRemoteAddress();
		// if the src task manager is offline, we may ignore this network failure.
		if (blacklistActions.isTaskManagerOffline(hostFailure.getResourceID())) {
			LOG.debug("src task manager is offline: {}, may skip this network failure.", hostFailure.getResourceID());
			return true;
		}
		// query the resource id of remote task manager. Returning null means this connection may not connect to
		// the data port of the remote task manager.
		ResourceID remoteTaskManagerID = blacklistActions.queryTaskManagerID(remoteAddress);
		// if the task manager with the remote address is offline, we may ignore this network failure.
		if (remoteTaskManagerID != null && blacklistActions.isTaskManagerOffline(remoteTaskManagerID)) {
			LOG.debug("dest task manager is offline: {}, may skip this network failure", remoteTaskManagerID);
			return true;
		}
		// if the task manager id is null, we can't know whether the task manager is offline, so we just mark this task manager as alive.
		return false;
	}

	protected void removeFailuresFromBlackedHosts() {
		// remove record whose src host is in blacklist
		Set<HostFailure> removedFailures = new HashSet<>();
		this.failureHosts.entrySet().removeIf(e -> {
			if (blackedHosts.containsKey(e.getKey())) {
				removedFailures.addAll(e.getValue());
				return true;
			}
			return false;
		});
		// remove record whose dest host is in blacklist
		for (Map.Entry<String, LinkedList<HostFailure>> hostFailuresEntry : failureHosts.entrySet()) {
			hostFailuresEntry.getValue().removeIf(removedFailures::contains);
		}
		this.failureHosts.entrySet().removeIf(e -> e.getValue().isEmpty());
	}

	private void clearExpiredTimestampRecord(long recordClearTime) {
		for (Map<String, Long> remoteAddressToLastTimestamp : resourceIdToNetworkFailuresTimestamp.values()) {
			remoteAddressToLastTimestamp.entrySet().removeIf(e -> clock.absoluteTimeMillis() - e.getValue() > recordClearTime);
		}
		resourceIdToNetworkFailuresTimestamp.entrySet().removeIf(e -> e.getValue().isEmpty());
	}

}
