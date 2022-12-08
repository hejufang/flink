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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper of unknown/wrong/right blacked exceptions.
 */
public class BlackedExceptionAccuracy {

	/**
	 * These maps store the number of blacked hosts for each exception with different blockage status (UNKNOWN, RIGHT, WRONG).
	 * The key is the exception class name, the value is the number of blacked hosts for this exception.
	 */
	private final Map<String, Integer> numOfHostsForUnknownBlackedException;
	private final Map<String, Integer> numOfHostsForWrongBlackedException;
	private final Map<String, Integer> numOfHostsForRightBlackedException;
	private final Map<String, Double> confidenceMapAfterBlackedHosts;

	public BlackedExceptionAccuracy(
			Map<String, Integer> numOfHostsForBlackedException,
			Set<String> unknownBlackedExceptions,
			Map<String, Double> confidenceMapAfterBlackedHosts) {
		this.numOfHostsForUnknownBlackedException = new HashMap<>();
		this.numOfHostsForWrongBlackedException = new HashMap<>();
		this.numOfHostsForRightBlackedException = new HashMap<>();
		this.confidenceMapAfterBlackedHosts = confidenceMapAfterBlackedHosts;
		for (Map.Entry<String, Double> entry : confidenceMapAfterBlackedHosts.entrySet()) {
			Integer numOfBlackedHostForThisException = numOfHostsForBlackedException.get(entry.getKey());
			if (unknownBlackedExceptions.contains(entry.getKey())) {
				numOfHostsForUnknownBlackedException.put(entry.getKey(), numOfBlackedHostForThisException);
				numOfHostsForRightBlackedException.put(entry.getKey(), 0);
				numOfHostsForWrongBlackedException.put(entry.getKey(), 0);
				continue;
			}
			if (entry.getValue() > 0.8) {
				numOfHostsForUnknownBlackedException.put(entry.getKey(), 0);
				numOfHostsForRightBlackedException.put(entry.getKey(), numOfBlackedHostForThisException);
				numOfHostsForWrongBlackedException.put(entry.getKey(), 0);
			} else {
				numOfHostsForUnknownBlackedException.put(entry.getKey(), 0);
				numOfHostsForRightBlackedException.put(entry.getKey(), 0);
				numOfHostsForWrongBlackedException.put(entry.getKey(), numOfBlackedHostForThisException);
			}
		}
	}

	public Map<String, Integer> getNumOfHostsForUnknownBlackedException() {
		return numOfHostsForUnknownBlackedException;
	}

	public Map<String, Integer> getNumOfHostsForWrongBlackedException() {
		return numOfHostsForWrongBlackedException;
	}

	public Map<String, Integer> getNumOfHostsForRightBlackedException() {
		return numOfHostsForRightBlackedException;
	}

	public Map<String, Double> getConfidenceMapAfterBlackedHosts() {
		return confidenceMapAfterBlackedHosts;
	}
}
