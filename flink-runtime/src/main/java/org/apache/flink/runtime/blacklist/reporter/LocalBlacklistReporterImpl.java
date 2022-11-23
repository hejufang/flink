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

package org.apache.flink.runtime.blacklist.reporter;

import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.tracker.BlacklistTracker;
import org.apache.flink.runtime.clusterframework.types.ResourceID;

/**
 * Implement of Blacklist Reporter.
 */
public class LocalBlacklistReporterImpl implements BlacklistReporter {
	private final BlacklistTracker blacklistTracker;
	private final BlacklistUtil.FailureType defaultFailureType = BlacklistUtil.FailureType.TASK_MANAGER;

	public LocalBlacklistReporterImpl(BlacklistTracker blacklistTracker) {
		this.blacklistTracker = blacklistTracker;
	}

	@Override
	public void onFailure(String hostname, ResourceID resourceID, Throwable t, long timestamp) {
		onFailure(defaultFailureType, hostname, resourceID, t, timestamp);
	}

	@Override
	public void onFailure(BlacklistUtil.FailureType failureType, String hostname, ResourceID resourceID, Throwable t, long timestamp) {
		blacklistTracker.onFailure(failureType, hostname, resourceID, t, timestamp);
	}

	@Override
	public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) {
		blacklistTracker.addIgnoreExceptionClass(exceptionClass);
	}
}
