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

import org.apache.flink.runtime.blacklist.BlacklistActions;
import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Do nothing.
 */
public class NoOpBlacklistTrackerImpl implements BlacklistTracker {
	@Override
	public void start(ComponentMainThreadExecutor mainThreadExecutor, BlacklistActions blacklistActions) { }

	@Override
	public void clearAll() { }

	@Override
	public void onFailure(BlacklistUtil.FailureType failureType, String hostname, ResourceID resourceID, Throwable cause, long timestamp) { }

	@Override
	public Map<String, HostFailure> getBlackedHosts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, HostFailure> getBlackedCriticalErrorHosts() {
		return Collections.emptyMap();
	}

	@Override
	public Set<ResourceID> getBlackedResources(BlacklistUtil.FailureType failureType, String hostname) {
		return Collections.emptySet();
	}

	@Override
	public void addIgnoreExceptionClass(Class<? extends Throwable> exceptionClass) { }

	@Override
	public void close() throws Exception { }
}
