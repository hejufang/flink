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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blacklist.reporter.BlacklistReporter;
import org.apache.flink.runtime.blacklist.reporter.LocalBlacklistReporterImpl;
import org.apache.flink.runtime.blacklist.reporter.NoOpBlacklistReporterImpl;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporter;
import org.apache.flink.runtime.blacklist.reporter.RemoteBlacklistReporterImpl;
import org.apache.flink.runtime.blacklist.tracker.BlacklistTracker;
import org.apache.flink.runtime.blacklist.tracker.BlacklistTrackerImpl;
import org.apache.flink.runtime.blacklist.tracker.NoOpBlacklistTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util for {@link BlacklistTracker} and ${@link BlacklistReporter}.
 */
public class BlacklistUtil {
	private static final Logger LOG = LoggerFactory.getLogger(BlacklistUtil.class);

	public static BlacklistTracker createBlacklistTracker(Configuration configuration, ResourceManagerMetricGroup resourceManagerMetricGroup) {
		BlacklistConfiguration blacklistConfiguration = BlacklistConfiguration.fromConfiguration(configuration);
		if (blacklistConfiguration.isTaskBlacklistEnabled() || blacklistConfiguration.isTaskManagerBlacklistEnabled()) {
			LOG.info("create new Blacklist with config: {}", blacklistConfiguration);
			return new BlacklistTrackerImpl(blacklistConfiguration, resourceManagerMetricGroup);
		} else {
			LOG.info("Blacklist not enabled.");
			return new NoOpBlacklistTrackerImpl();
		}
	}

	public static BlacklistReporter createLocalBlacklistReporter(
			Configuration configuration,
			BlacklistTracker blacklistTracker) {
		BlacklistConfiguration blacklistConfiguration = BlacklistConfiguration.fromConfiguration(configuration);
		if (blacklistConfiguration.isTaskManagerBlacklistEnabled()) {
			return new LocalBlacklistReporterImpl(blacklistTracker);
		} else {
			return new NoOpBlacklistReporterImpl();
		}
	}

	public static RemoteBlacklistReporter createRemoteBlacklistReporter(
			Configuration configuration,
			JobID jobID,
			Time rpcTimeout) {
		BlacklistConfiguration blacklistConfiguration = BlacklistConfiguration.fromConfiguration(configuration);
		if (blacklistConfiguration.isTaskBlacklistEnabled()) {
			LOG.info("Create RemoteBlacklistReporter with jobId {}, rpcTimeout {}.",
					jobID, rpcTimeout);
			return new RemoteBlacklistReporterImpl(jobID, rpcTimeout);
		} else {
			return createNoOpRemoteBlacklistReporter();
		}
	}

	public static RemoteBlacklistReporter createNoOpRemoteBlacklistReporter() {
		LOG.debug("Create NoOpRemoteBlacklistReporter.");
		return new NoOpBlacklistReporterImpl();
	}

	/**
	 * Failure Type, now support container and task.
	 */
	public enum FailureType {
		TASK_MANAGER,
		TASK
	}
}
