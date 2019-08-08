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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static org.apache.flink.yarn.Utils.require;

/**
 * Utility class that provides helper methods to work with Zookeeper.
 */
public class ZkUtils {
	public static CuratorFramework newZkClient(Configuration config) {
		String zkQuorum = config.getString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);
		require(zkQuorum != null && !zkQuorum.isEmpty(), "Zookeeper quorum not set.");

		int sessionTimeoutMs = config.getInteger(HighAvailabilityOptions.JOB_UNIQUE_ZOOKEEPER_SESSION_TIMEOUT);
		int connectionTimeoutMs = config.getInteger(HighAvailabilityOptions.JOB_UNIQUE_ZOOKEEPER_CONNECTION_TIMEOUT);
		int baseSleepTimeMs = config.getInteger(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT);
		int retryTimes = config.getInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, retryTimes);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zkQuorum, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
		return client;
	}
}
