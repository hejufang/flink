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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wrapper of Blacked hosts and resources.
 */
public class BlacklistRecord {
	BlacklistUtil.FailureActionType actionType;
	BlacklistUtil.FailureType failureType;
	Map<String, LinkedList<HostFailure>> blackedHosts;

	public BlacklistRecord(BlacklistUtil.FailureActionType actionType, BlacklistUtil.FailureType failureType, Map<String, LinkedList<HostFailure>> blackedHosts) {
		this.actionType = actionType;
		this.failureType = failureType;
		this.blackedHosts = blackedHosts;
	}

	public BlacklistUtil.FailureActionType getActionType() {
		return actionType;
	}

	public BlacklistUtil.FailureType getFailureType() {
		return failureType;
	}

	public Set<String> getBlackedHosts() {
		return blackedHosts.keySet();
	}

	public Set<ResourceID> getBlackedResources() {
		return blackedHosts.values().stream()
				.flatMap(l -> l.stream().map(HostFailure::getResourceID))
				.collect(Collectors.toSet());
	}

	public List<HostFailure> getLatestBlackedFailure() {
		return blackedHosts.values().stream().map(LinkedList::getLast).collect(Collectors.toList());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BlacklistRecord that = (BlacklistRecord) o;
		return actionType == that.actionType && failureType == that.failureType && Objects.equals(blackedHosts, that.blackedHosts);
	}

	@Override
	public int hashCode() {
		return Objects.hash(actionType, failureType, blackedHosts);
	}

	@Override
	public String toString() {
		return "BlacklistRecord{" +
				"actionType=" + actionType +
				", failureType=" + failureType +
				", blackedHosts=" + blackedHosts.keySet() +
				'}';
	}
}
